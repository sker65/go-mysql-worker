package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

const (
	dbMaxIdleConns    = 4
	dbMaxConns        = 100
	totalWorkers      = 100
	channelBufferSize = 100
	sqlBatchSize      = 8
	CsvFile           = "majestic_million.csv"
)

var (
	dataHeaders []string
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	f, err := os.Create("myprogram.prof")
	if err != nil {
		fmt.Println(err)
		return
	}
	pprof.StartCPUProfile(f)
	start := time.Now()

	db, err := OpenDBConnection()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	csvReader, csvFile, err := OpenCSVFile(CsvFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer csvFile.Close()

	row, err := csvReader.Read()
	if err == nil {
		dataHeaders = row
		log.Println("Fields found:", dataHeaders)
	}

	jobs := make(chan []string, channelBufferSize)
	quit := make(chan bool, totalWorkers)

	var wg sync.WaitGroup

	go StartWorkers(db, jobs, &wg, quit)
	ProcessCSVFile(csvReader, jobs, 2000000)
	StopWorkers(quit)
	wg.Wait()
	pprof.StopCPUProfile()

	duration := time.Since(start)
	log.Printf("Done in %d seconds", int(math.Ceil(duration.Seconds())))
}

func OpenDBConnection() (*sql.DB, error) {

	dbUsername := os.Getenv("DB_USERNAME")
	dbName := os.Getenv("DB_NAME")
	dbPass := os.Getenv("DB_PASSWORD")
	dbConnString := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s", dbUsername, dbPass, dbName)
	dbConnStringPrintable := fmt.Sprintf("%s:***@tcp(localhost:3306)/%s", dbUsername, dbName)

	log.Printf("Open DB connection using %s", dbConnStringPrintable)

	db, err := sql.Open("mysql", dbConnString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(dbMaxConns)
	db.SetMaxIdleConns(dbMaxIdleConns)

	return db, nil
}

// OpenCSVFile opens a CSV file and returns a reader and a file handle
func OpenCSVFile(filename string) (*csv.Reader, *os.File, error) {
	log.Printf("Open CSV file '%s'\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Println("error opening csv file ", file, err.Error())
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return reader, file, nil
}

// toAnyList converts a slice of T to a slice of any
func toAnyList[T any](input []T) []any {
	list := make([]any, len(input))
	for i, v := range input {
		list[i] = v
	}
	return list
}

func worker(workerIndex int, db *sql.DB, jobs <-chan []string, query string, placeholders string, wg *sync.WaitGroup, quit <-chan bool) {
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
					log.Trace("Got values ", workerIndex, counter, len(job))
					counter++
				}
			}
			if counter >= sqlBatchSize || timeout {
				break
			}
		}
		if timeout {
			log.Printf("Worker %d timeout\n", workerIndex)
		}
		if len(values) > 0 {
			_, err = conn.ExecContext(context.Background(), q, toAnyList(values)...)
			log.Trace("Worker data:", counter, query, values)
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

// StartWorkers starts all workers providing them a job queue and a wait group, database connection and a query to execute
func StartWorkers(db *sql.DB, jobs <-chan []string, wg *sync.WaitGroup, quit <-chan bool) {
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

// StopWorkers stops all workers by sending them a quit signal
func StopWorkers(quit chan bool) {
	log.Println("Quitting workers")
	for i := 0; i < totalWorkers; i++ {
		quit <- true
	}
}

// ProcessCSVFile processes a CSV file and sends the rows to the jobs channel
// processing ends either when eof or maxLines is reached
func ProcessCSVFile(reader *csv.Reader, jobs chan<- []string, maxLines int) {
	rowcount := 0
	for ; rowcount < maxLines; rowcount++ {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		log.Traceln("read line with values:", row)
		jobs <- row
		if rowcount%1000 == 0 {
			log.Printf("Processed %d rows", rowcount)
		}
		// for testing only time.Sleep(2 * time.Second)
	}
	log.Printf("Processed %d rows", rowcount)
	close(jobs)
}

// generateQuestionsMark generates a slice of question marks of length n (used for building SQL statements)
func generateQuestionsMark(n int) []string {
	var r = make([]string, n)
	for i := range r {
		r[i] = "?"
	}
	return r
}
