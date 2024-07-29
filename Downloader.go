package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type chunk struct {
	start, end int64
	data       []byte
}

func main() {
	startTime := time.Now()
	url := "https://link.testfile.org/PDF200MB"
	resp, err := http.Head(url)
	handleError(err)
	lens := resp.Header.Get("Content-Length")
	fmt.Println("Content-Length is ", lens)
	len, err := strconv.ParseInt(lens, 10, 64)
	handleError(err)
	sig := resp.Header.Get("ETag")
	fmt.Printf("Length of the file is %v and MD5 signature is %v\n", len, sig)
	if acceptRanges := resp.Header.Get("Accept-Ranges"); acceptRanges == "none" {
		log.Fatal("Server does not support range requests")
	}

	location := "200mb.pdf"
	outFile, err := os.Create(location)
	handleError(err)
	defer outFile.Close()

	noOfChunks := 50
	size := (len) / int64(noOfChunks)
	var wg sync.WaitGroup
	chunksChan := make(chan chunk, noOfChunks)

	for i := 0; i < noOfChunks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := int64(i) * size
			end := int64(i+1)*size - 1
			if i == noOfChunks-1 {
				end = len - 1
			}
			data, err := downloadChunk(url, start, end)
			if err != nil {
				log.Printf("Error downloading chunk %d: %v", i, err)
				return
			}
			chunksChan <- chunk{start: start, end: end, data: data}
			fmt.Printf("Downloaded bytes %v to %v\n", start, end)
		}(i)
	}

	go func() {
		wg.Wait()
		close(chunksChan)
	}()

	for c := range chunksChan {
		_, err := outFile.WriteAt(c.data, c.start)
		handleError(err)
	}

	fmt.Println("Download completed")
	totalTime := time.Since(startTime)
	fmt.Println("Time taken to download the file is ", totalTime)
}

func downloadChunk(url string, start, end int64) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", rangeHeader)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func handleError(err error) {
	if err != nil {
		log.Fatal("Error:", err)
	}
}
