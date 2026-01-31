package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/test-go/testify/assert"

	cfg "github.com/databendcloud/bend-archiver/config"
	"github.com/databendcloud/bend-archiver/ingester"
	"github.com/databendcloud/bend-archiver/source"
	"github.com/databendcloud/bend-archiver/worker"

	_ "github.com/datafuselabs/databend-go"
)

// TestCSVWorkflow tests the complete CSV to Databend workflow
func TestCSVWorkflow(t *testing.T) {
	// Skip if Databend is not available
	if !isDatabendAvailable() {
		t.Skip("Databend is not available, skipping CSV workflow test")
		return
	}

	fmt.Println("=== TEST CSV SOURCE ===")

	// Create test CSV file
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test_data.csv")
	err := createTestCSVFile(csvFile, 20)
	assert.NoError(t, err)

	// Prepare Databend table
	tableName := "test_csv_table"
	dsn := "http://databend:databend@localhost:8000"
	truncateCSVDatabendTable(tableName, dsn)
	prepareCSVDatabendTable(tableName, dsn)

	// Prepare test config
	testConfig := prepareCSVTestConfig(csvFile, tableName)
	startTime := time.Now()

	// Create source and worker
	src, err := source.NewSource(testConfig)
	assert.NoError(t, err)

	ig := ingester.NewDatabendIngester(testConfig)
	w := worker.NewWorker(testConfig, "csv.test_data", ig, src)

	// Run the worker
	w.Run(context.Background())

	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	// Verify the data was imported correctly
	err = checkCSVTargetTable(tableName, 20)
	assert.NoError(t, err)

	// Cleanup
	truncateCSVDatabendTable(tableName, dsn)
}

// TestCSVWorkflowWithMultipleFiles tests CSV workflow with multiple files
func TestCSVWorkflowWithMultipleFiles(t *testing.T) {
	// Skip if Databend is not available
	if !isDatabendAvailable() {
		t.Skip("Databend is not available, skipping CSV workflow test")
		return
	}

	fmt.Println("=== TEST CSV SOURCE WITH MULTIPLE FILES ===")

	// Create test CSV files
	tmpDir := t.TempDir()
	err := createTestCSVFile(filepath.Join(tmpDir, "data1.csv"), 10)
	assert.NoError(t, err)
	err = createTestCSVFile(filepath.Join(tmpDir, "data2.csv"), 15)
	assert.NoError(t, err)

	// Prepare Databend table
	tableName := "test_csv_multi"
	dsn := "http://databend:databend@localhost:8000"
	truncateCSVDatabendTable(tableName, dsn)
	prepareCSVDatabendTable(tableName, dsn)

	// Prepare test config with directory
	testConfig := prepareCSVTestConfig(tmpDir, tableName)
	startTime := time.Now()

	// Create source and worker
	src, err := source.NewSource(testConfig)
	assert.NoError(t, err)

	ig := ingester.NewDatabendIngester(testConfig)
	w := worker.NewWorker(testConfig, "csv.multi_data", ig, src)

	// Run the worker
	w.Run(context.Background())

	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	// Verify the data was imported correctly (10 + 15 = 25 rows)
	err = checkCSVTargetTable(tableName, 25)
	assert.NoError(t, err)

	// Cleanup
	truncateCSVDatabendTable(tableName, dsn)
}

// TestCSVWorkflowWithParallel tests CSV workflow with parallel processing
func TestCSVWorkflowWithParallel(t *testing.T) {
	// Skip if Databend is not available
	if !isDatabendAvailable() {
		t.Skip("Databend is not available, skipping CSV workflow test")
		return
	}

	fmt.Println("=== TEST CSV SOURCE WITH PARALLEL PROCESSING ===")

	// Create test CSV file with more rows
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "large_data.csv")
	err := createTestCSVFile(csvFile, 100)
	assert.NoError(t, err)

	// Prepare Databend table
	tableName := "test_csv_parallel"
	dsn := "http://databend:databend@localhost:8000"
	truncateCSVDatabendTable(tableName, dsn)
	prepareCSVDatabendTable(tableName, dsn)

	// Prepare test config with multiple threads
	testConfig := prepareCSVTestConfig(csvFile, tableName)
	testConfig.MaxThread = 4
	testConfig.BatchSize = 25
	startTime := time.Now()

	// Create source and worker
	src, err := source.NewSource(testConfig)
	assert.NoError(t, err)

	ig := ingester.NewDatabendIngester(testConfig)
	w := worker.NewWorker(testConfig, "csv.large_data", ig, src)

	// Run the worker
	w.Run(context.Background())

	endTime := fmt.Sprintf("end time: %s", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(endTime)
	fmt.Println(fmt.Sprintf("total time: %s", time.Since(startTime)))

	// Verify the data was imported correctly
	err = checkCSVTargetTable(tableName, 100)
	assert.NoError(t, err)

	// Cleanup
	truncateCSVDatabendTable(tableName, dsn)
}

// Helper functions

func isDatabendAvailable() bool {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		return false
	}
	defer db.Close()

	err = db.Ping()
	return err == nil
}

func createTestCSVFile(filename string, rows int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header
	_, err = file.WriteString("id,name,age,email,active\n")
	if err != nil {
		return err
	}

	// Write data rows
	for i := 1; i <= rows; i++ {
		active := "true"
		if i%2 == 0 {
			active = "false"
		}
		line := fmt.Sprintf("%d,User%d,%d,user%d@example.com,%s\n",
			i, i, 20+i%50, i, active)
		_, err = file.WriteString(line)
		if err != nil {
			return err
		}
	}

	return nil
}

func prepareCSVDatabendTable(tableName string, dsn string) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS default.%s (
			id BIGINT,
			name VARCHAR(255),
			age BIGINT,
			email VARCHAR(255),
			active BOOLEAN
		)
		`, tableName))
	if err != nil {
		log.Fatal(err)
	}
}

func truncateCSVDatabendTable(tableName string, dsn string) {
	db, err := sql.Open("databend", dsn)
	if err != nil {
		log.Printf("Warning: failed to open databend: %v", err)
		return
	}
	defer db.Close()

	// Drop table if exists
	_, err = db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS default.%s`, tableName))
	if err != nil {
		log.Printf("Warning: failed to drop table: %v", err)
	}
}

func prepareCSVTestConfig(csvPath string, tableName string) *cfg.Config {
	config := cfg.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvPath,
		DatabendDSN:   "http://databend:databend@localhost:8000",
		DatabendTable: fmt.Sprintf("default.%s", tableName),
		BatchSize:     10,
		MaxThread:     1,
		CopyPurge:     true,
		CopyForce:     false,
		UserStage:     "~",
	}
	return &config
}

func checkCSVTargetTable(tableName string, expectedCount int) error {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer db.Close()

	// Count rows
	var count int
	err = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM default.%s`, tableName)).Scan(&count)
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Printf("Expected %d rows, got %d rows\n", expectedCount, count)

	if count != expectedCount {
		return fmt.Errorf("expected %d rows, got %d rows", expectedCount, count)
	}

	// Verify some data
	rows, err := db.Query(fmt.Sprintf(`SELECT id, name, age, email, active FROM default.%s LIMIT 5`, tableName))
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer rows.Close()

	fmt.Println("Sample data from target table:")
	for rows.Next() {
		var id int64
		var name string
		var age int64
		var email string
		var active bool
		err = rows.Scan(&id, &name, &age, &email, &active)
		if err != nil {
			log.Fatal(err)
			return err
		}
		fmt.Printf("  id=%d, name=%s, age=%d, email=%s, active=%v\n", id, name, age, email, active)
	}

	return nil
}

