package main

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
)

const (
	batchSize   = 1000
	workerCount = 100
)

func main() {
	fileOpen, err := os.Open("indonesian-news-title.csv")
	if err != nil {
		log.Fatal("Tidak dapat membuka file:", err)
	}
	defer fileOpen.Close()

	// Create a new csv file just title and content
	fileCreate, err := os.Create("detiknews-title-content.csv")
	if err != nil {
		log.Fatal("Tidak dapat membuat file:", err)
	}
	defer fileCreate.Close()

	csvProcessor := NewCsvProcessor()
	err = csvProcessor.ProcessFile(fileOpen, fileCreate)
	if err != nil {
		log.Fatal("Gagal memproses file:", err)
	}

	log.Println("File berhasil dibuat")
}

type WriterMutex struct {
	mu sync.Mutex
	w  *csv.Writer
}

func NewWriteMutex(writer *csv.Writer) *WriterMutex {
	return &WriterMutex{
		w: writer,
	}
}

type CsvProcessor struct {
	wg       sync.WaitGroup
	jobsChan chan map[string]string
}

func NewCsvProcessor() *CsvProcessor {
	return &CsvProcessor{
		jobsChan: make(chan map[string]string, batchSize),
	}
}

func (c *CsvProcessor) ProcessFile(readCSV *os.File, createdCSV *os.File) error {
	//url and title index
	urlIndex := -1
	titleIndex := -1

	//read readCSV
	reader := csv.NewReader(readCSV)
	//skip header and finf index of url and title
	titleUrlIndex, err := reader.Read()
	if err != nil {
		return err
	}
	for i, v := range titleUrlIndex {
		if v == "title" {
			titleIndex = i
		} else if v == "url" {
			urlIndex = i
		}
	}

	//if not found
	if titleIndex == -1 || urlIndex == -1 {
		return errors.New("title or url not found")
	}
	//write createdCSV
	writer := csv.NewWriter(createdCSV)
	defer writer.Flush()
	writer.Write([]string{"title", "content"})
	writerMutex := NewWriteMutex(writer)

	// Start workers
	for i := 0; i < workerCount; i++ {
		go c.worker(writerMutex, i)
	}

	for {
		//read csv
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			break
		}

		data := map[string]string{"title": "", "url": ""}
		for i, v := range record {
			if i == titleIndex {
				data["title"] = v
			}
			if i == urlIndex {
				data["url"] = v
			}
		}

		//check if title and url is empty
		if data["title"] == "" || data["url"] == "" {
			log.Println("Title or url is empty")
			continue
		}

		c.wg.Add(1)
		c.jobsChan <- data
	}

	close(c.jobsChan)
	c.wg.Wait()
	return nil
}

func (c *CsvProcessor) worker(writer *WriterMutex, id int) {
	couter := 0
	for data := range c.jobsChan {
		//scraping
		title := data["title"]
		content, err := c.scraping(data["url"])
		if err != nil {
			log.Println("Error scraping url:", data["url"])
			continue
		}

		quotedTitle := "\"" + title + "\""
		quotedContent := "\"" + content + "\""

		//write to csv
		writer.mu.Lock()
		writer.w.Write([]string{quotedTitle, quotedContent})
		writer.mu.Unlock()
		log.Printf("Title: %s,berhasil dibuat pada worker %d proses ke-%d", title, id, couter)
		couter++
		if couter == batchSize {
			couter = 0
			log.Println("Write to csv")
		}
	}
}

func (c *CsvProcessor) scraping(url string) (string, error) {
	// stringsbuilder
	var content strings.Builder

	// Request the HTML page.
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Printf("status code error: %d %s", res.StatusCode, res.Status)
		return "", errors.New("status code error")
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}

	// Find the content
	doc.Find("div.detail__body-text").Contents().Each(func(i int, s *goquery.Selection) {
		if goquery.NodeName(s) == "#text" {
			content.WriteString(s.Text())
		} else if goquery.NodeName(s) == "p" {
			content.WriteString(s.Text())
		}
	})

	return content.String(), nil
}
