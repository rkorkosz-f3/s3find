package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	concurrency := flag.Int("c", 10, "concurrency")
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatal("should provide s3 uri and query")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	cli := s3.NewFromConfig(cfg)
	bucket, prefix, err := parseS3Uri(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	query, err := regexp.Compile(flag.Arg(1))
	if err != nil {
		log.Fatal(err)
	}
	wp := NewWorkerPool(*concurrency)
	wp.Run()
	var token *string
	out := make(chan record, 1)
	go func() {
		defer close(out)
		for {
			var wg sync.WaitGroup
			output, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            &bucket,
				Prefix:            &prefix,
				ContinuationToken: token,
			})
			if err != nil {
				log.Fatal(err)
			}
			wg.Add(len(output.Contents))
			for _, obj := range output.Contents {
				wp.AddJob(func() {
					defer wg.Done()
					record, err := findInFile(ctx, cli, bucket, *obj.Key, query)
					if err != nil {
						return
					}
					for r := range record {
						out <- r
					}
				})
			}
			wg.Wait()
			if !output.IsTruncated {
				break
			}
			token = output.NextContinuationToken
		}
	}()
	for r := range out {
		fmt.Println(r.line)
	}
}

func parseS3Uri(uri string) (bucket string, prefix string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	return parsed.Host, parsed.Path[1:], nil
}

type record struct {
	lineNumber int
	line       string
	key        string
}

func (r record) String() string {
	return fmt.Sprintf("%s\n%s", r.key, r.line)
}

func findInFile(ctx context.Context, cli *s3.Client, bucket, key string, query *regexp.Regexp) (chan record, error) {
	recordC := make(chan record, 1)
	out, err := cli.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(recordC)
		defer out.Body.Close()
		r, err := decode(out.Body, *out.ContentType)
		if err != nil {
			return
		}
		s := bufio.NewScanner(r)
		for i := 1; s.Scan(); i++ {
			matched := query.Match(s.Bytes())
			if matched {
				recordC <- record{lineNumber: i, line: s.Text(), key: key}
			}
		}
		if err := s.Err(); err != nil {
			log.Println(err)
		}
	}()
	return recordC, nil
}

func decode(obj io.Reader, contentType string) (io.Reader, error) {
	switch contentType {
	case "application/x-gzip":
		return gzip.NewReader(obj)
	}
	return obj, nil
}

type workerPool struct {
	maxWorker int
	jobC      chan func()
}

func NewWorkerPool(maxWorker int) *workerPool {
	return &workerPool{maxWorker: maxWorker, jobC: make(chan func())}
}

func (wp *workerPool) Run() {
	for i := 1; i <= wp.maxWorker; i++ {
		go func(workerID int) {
			for t := range wp.jobC {
				t()
			}
		}(i)
	}
}

func (wp *workerPool) AddJob(j func()) {
	wp.jobC <- j
}
