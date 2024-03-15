package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

const (
	dbMaxIdleConns    = 4
	dbMaxConns        = 100
	totalWorkers      = 10
	channelBufferSize = 100
	csvFile           = "majestic_million.csv"
)

var (
	dataHeaders []string
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}
	var dbUsername = os.Getenv("DB_USERNAME")
	var dbName = os.Getenv("DB_NAME")
	var dbPass = os.Getenv("DB_PASSWORD")
	var dbConnString = fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", dbUsername, dbPass, dbName)

	f, err := os.Create("myprogram.prof")
	if err != nil {
		fmt.Println(err)
		return
	}
	pprof.StartCPUProfile(f)
	start := time.Now()

	db, err := OpenDBConnection(dbConnString)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	csvReader, csvFile, err := OpenCSVFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	row, err := csvReader.Read()
	if err == nil && len(dataHeaders) == 0 {
		dataHeaders = row
		log.Println("Fields found:", dataHeaders)
	}

	jobs := make(chan []string, channelBufferSize)
	quit := make(chan bool, totalWorkers)

	var wg sync.WaitGroup

	go DispatchWorkers(db, jobs, &wg, quit)
	ProcessCSVFileWithWorker(csvReader, jobs)
	quitWorkers(quit)
	wg.Wait()
	pprof.StopCPUProfile()

	duration := time.Since(start)
	log.Printf("Done in %d seconds", int(math.Ceil(duration.Seconds())))
}

func quitWorkers(quit chan bool) {
	log.Println("Quitting workers")
	for i := 0; i < totalWorkers; i++ {
		quit <- true
	}
}

func OpenDBConnection(dbConnString string) (*sql.DB, error) {

	log.Printf("Open DB connection using %s", dbConnString)

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

func OpenCSVFile() (*csv.Reader, *os.File, error) {
	log.Println("Open CSV file")

	file, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, nil
}

func toAnyList[T any](input []T) []any {
	list := make([]any, len(input))
	for i, v := range input {
		list[i] = v
	}
	return list
}

func worker(workerIndex int, db *sql.DB, jobs <-chan []string, query string, placeholders string, wg *sync.WaitGroup, quit <-chan bool) {
	maxBatchSize := 8
	defer wg.Add(-1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	defer conn.Close()

	for {
		counter := 0
		q := strings.Clone(query)
		values := make([]string, 0)
		timeout := false
		exit := false
		timer := time.After(1 * time.Second)
		for {
			select {
			case <-timer:
				timeout = true
			case job := <-jobs:
				if len(job) > 0 {
					values = append(values, job...)
					if counter > 0 {
						q = q + ", (" + placeholders + ")"
					}
					//log.Println("Got values ", workerIndex, counter, len(job))
					counter++
				}
			}
			if counter >= maxBatchSize || timeout {
				break
			}
		}
		if timeout {
			log.Printf("Worker %d timeout\n", workerIndex)
		}
		if len(values) > 0 {
			_, err = conn.ExecContext(context.Background(), q, toAnyList(values)...)
			log.Println("Worker data:", counter, query, values)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
		select {
		case <-quit: // check for quit w/o blocking
			log.Printf("Worker %d is exiting because of quit signal\n", workerIndex)
			exit = true
		default:
		}
		if exit {
			log.Printf("Worker %d exits\n", workerIndex)
			break
		}
	}
}

func DispatchWorkers(db *sql.DB, jobs <-chan []string, wg *sync.WaitGroup, quit <-chan bool) {
	var placeholders = strings.Join(generateQuestionsMark(len(dataHeaders)), ",")
	var query = fmt.Sprintf("INSERT INTO domain (%s) VALUES (%s)",
		strings.Join(dataHeaders, ","),
		placeholders,
	)
	for i := 0; i < totalWorkers; i++ {
		log.Printf("Starting Worker %d\n", i)
		wg.Add(1)
		go worker(i, db, jobs, query, placeholders, wg, quit)
	}
}

func ProcessCSVFileWithWorker(reader *csv.Reader, jobs chan<- []string) {
	rowcount := 0
	for ; rowcount < 11; rowcount++ {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		log.Println("read line with values:", row)
		jobs <- row
		if rowcount%1000 == 0 {
			log.Printf("Processed %d rows", rowcount)
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("Processed %d rows", rowcount)
	close(jobs)
}

func generateQuestionsMark(n int) []string {
	var r = make([]string, n)
	for i := range r {
		r[i] = "?"
	}
	return r
}
