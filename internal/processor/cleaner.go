package processor

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

// Database connection parameters
const (
	host     = "localhost" // Replace with your database host
	port     = 5432
	user     = "postgres" // Replace with your database user
	password = ""         // Replace with your database password
	dbname   = "imdb"     // Replace with your database name
)

// CleanTSV reads, processes, and inserts data into PostgreSQL
func CleanTSV(inputPath, outputPath string, workerCount int) error {
	// Open the TSV file
	inFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer inFile.Close()

	// Connect to PostgreSQL
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer db.Close()

	// Create a prepared statement for insertion

	stmt, err := db.Prepare(`
		INSERT INTO movie_person (tconst, titleid, nconst, category, job, charactername)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	// Scan through the TSV and insert rows into the database
	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		line := scanner.Text()
		cleaned := cleanLine(line)
		fields := strings.Split(cleaned, "\t")

		// Insert each row into the database
		_, err := stmt.Exec(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
		if err != nil {
			log.Printf("Error inserting row: %v", err)
			log.Printf("name is: %v", fields[2])
			continue // Skip rows that cause errors
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	log.Println("TSV data successfully imported into PostgreSQL.")
	return nil
}

// cleanLine removes extra spaces and returns the cleaned line
func cleanLine(line string) string {
	fields := strings.Split(line, "\t")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	return strings.Join(fields, "\t")
}
