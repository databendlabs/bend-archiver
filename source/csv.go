package source

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-archiver/config"
)

type CSVSource struct {
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
	files         []string // List of CSV files to process
	currentFile   string   // Current file being processed
	columns       []string // Column names from CSV header
	totalRows     int      // Total rows across all files
}

func NewCSVSource(cfg *config.Config) (*CSVSource, error) {
	stats := NewDatabendIntesterStatsRecorder()

	// Discover CSV files
	files, err := discoverCSVFiles(cfg.SourceCSVPath)
	if err != nil {
		logrus.Errorf("failed to discover CSV files: %v", err)
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no CSV files found in path: %s", cfg.SourceCSVPath)
	}

	logrus.Infof("discovered %d CSV file(s): %v", len(files), files)

	return &CSVSource{
		cfg:           cfg,
		statsRecorder: stats,
		files:         files,
	}, nil
}

// discoverCSVFiles finds all CSV files in the given path
// If path is a file, return it; if it's a directory, return all .csv files
func discoverCSVFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat path %s: %w", path, err)
	}

	var files []string

	if info.IsDir() {
		// Read all files in directory
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read directory %s: %w", path, err)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if strings.HasSuffix(strings.ToLower(entry.Name()), ".csv") {
				files = append(files, filepath.Join(path, entry.Name()))
			}
		}
		sort.Strings(files) // Sort for consistent ordering
	} else {
		// Single file
		if !strings.HasSuffix(strings.ToLower(path), ".csv") {
			return nil, fmt.Errorf("file %s is not a CSV file", path)
		}
		files = append(files, path)
	}

	return files, nil
}

// AdjustBatchSizeAccordingToSourceDbTable returns the configured batch size
// For CSV files, we use the configured batch size directly
func (s *CSVSource) AdjustBatchSizeAccordingToSourceDbTable() uint64 {
	return uint64(s.cfg.BatchSize)
}

// GetSourceReadRowsCount returns the total number of rows in all CSV files
func (s *CSVSource) GetSourceReadRowsCount() (int, error) {
	if s.totalRows > 0 {
		return s.totalRows, nil
	}

	totalRows := 0
	for _, file := range s.files {
		count, err := countCSVRows(file)
		if err != nil {
			return 0, fmt.Errorf("failed to count rows in %s: %w", file, err)
		}
		totalRows += count
	}

	s.totalRows = totalRows
	return totalRows, nil
}

// countCSVRows counts the number of data rows in a CSV file (excluding header)
func countCSVRows(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	count := 0

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return 0, err
	}

	// Count data rows
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, nil
}

// GetMinMaxSplitKey returns the min and max row numbers
// For CSV, we use row numbers as the split key (1-based, excluding header)
func (s *CSVSource) GetMinMaxSplitKey() (uint64, uint64, error) {
	totalRows, err := s.GetSourceReadRowsCount()
	if err != nil {
		return 0, 0, err
	}

	if totalRows == 0 {
		return 0, 0, nil
	}

	return 1, uint64(totalRows), nil
}

// GetMinMaxTimeSplitKey is not supported for CSV files
func (s *CSVSource) GetMinMaxTimeSplitKey() (string, string, error) {
	return "", "", fmt.Errorf("time-based split is not supported for CSV files")
}

// DeleteAfterSync deletes the CSV files after successful sync
func (s *CSVSource) DeleteAfterSync() error {
	logrus.Infof("DeleteAfterSync: %v", s.cfg.DeleteAfterSync)
	if !s.cfg.DeleteAfterSync {
		return nil
	}

	for _, file := range s.files {
		logrus.Infof("deleting CSV file: %s", file)
		if err := os.Remove(file); err != nil {
			logrus.Errorf("failed to delete file %s: %v", file, err)
			return err
		}
	}

	return nil
}

// QueryTableData reads data from CSV files based on the condition
// conditionSql format: "(row_num >= 1 and row_num < 1001)"
func (s *CSVSource) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
	l := logrus.WithFields(logrus.Fields{
		"thread": threadNum,
		"source": "csv",
	})

	startTime := time.Now()

	// Parse the condition to get row range
	startRow, endRow, err := parseRowCondition(conditionSql)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse condition: %w", err)
	}

	l.Infof("reading rows %d to %d", startRow, endRow)

	var allData [][]interface{}
	var columns []string
	currentRow := uint64(1)

	for _, file := range s.files {
		data, cols, lastRow, err := s.readCSVFile(file, startRow, endRow, currentRow)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read file %s: %w", file, err)
		}

		if len(columns) == 0 {
			columns = cols
		}

		allData = append(allData, data...)
		currentRow = lastRow + 1

		// If we've read enough rows, stop
		if currentRow > endRow {
			break
		}
	}

	s.statsRecorder.RecordMetric(len(allData))
	stats := s.statsRecorder.Stats(time.Since(startTime))
	l.Infof("extract %d rows (%.2f rows/s)", len(allData), stats.RowsPerSecondd)

	return allData, columns, nil
}

// readCSVFile reads a specific range of rows from a CSV file
func (s *CSVSource) readCSVFile(filename string, startRow, endRow, currentRow uint64) ([][]interface{}, []string, uint64, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to read header: %w", err)
	}

	var data [][]interface{}
	rowNum := currentRow

	// Read all rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to read row: %w", err)
		}

		// Check if this row is in the desired range
		if rowNum >= startRow && rowNum < endRow {
			// Convert string values to interface{}
			row := make([]interface{}, len(record))
			for i, val := range record {
				row[i] = convertCSVValue(val)
			}
			data = append(data, row)
		}

		rowNum++

		// If we've passed the end row, stop reading this file
		if rowNum >= endRow {
			break
		}
	}

	return data, header, rowNum - 1, nil
}

// convertCSVValue attempts to convert CSV string values to appropriate types
func convertCSVValue(val string) interface{} {
	// Try to parse as integer
	if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(val, 64); err == nil {
		return floatVal
	}

	// Try to parse as boolean
	if val == "true" || val == "TRUE" || val == "True" {
		return true
	}
	if val == "false" || val == "FALSE" || val == "False" {
		return false
	}

	// Return as string
	return val
}

// parseRowCondition parses a condition like "(row_num >= 1 and row_num < 1001)"
// and returns the start and end row numbers
func parseRowCondition(condition string) (uint64, uint64, error) {
	// Remove parentheses and split by "and"
	condition = strings.Trim(condition, "()")
	parts := strings.Split(condition, " and ")

	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid condition format: %s", condition)
	}

	var startRow, endRow uint64
	var err error

	// Parse first part (e.g., "row_num >= 1")
	if strings.Contains(parts[0], ">=") {
		fields := strings.Split(parts[0], ">=")
		if len(fields) != 2 {
			return 0, 0, fmt.Errorf("invalid start condition: %s", parts[0])
		}
		startRow, err = strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse start row: %w", err)
		}
	}

	// Parse second part (e.g., "row_num < 1001")
	if strings.Contains(parts[1], "<=") {
		fields := strings.Split(parts[1], "<=")
		if len(fields) != 2 {
			return 0, 0, fmt.Errorf("invalid end condition: %s", parts[1])
		}
		endRow, err = strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse end row: %w", err)
		}
		endRow++ // Convert <= to <
	} else if strings.Contains(parts[1], "<") {
		fields := strings.Split(parts[1], "<")
		if len(fields) != 2 {
			return 0, 0, fmt.Errorf("invalid end condition: %s", parts[1])
		}
		endRow, err = strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to parse end row: %w", err)
		}
	}

	return startRow, endRow, nil
}

// GetDatabasesAccordingToSourceDbRegex is not applicable for CSV files
func (s *CSVSource) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
	return nil, fmt.Errorf("database regex is not supported for CSV files")
}

// GetTablesAccordingToSourceTableRegex is not applicable for CSV files
func (s *CSVSource) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	return nil, fmt.Errorf("table regex is not supported for CSV files")
}

// GetAllSourceReadRowsCount returns the total number of rows in all CSV files
func (s *CSVSource) GetAllSourceReadRowsCount() (int, error) {
	return s.GetSourceReadRowsCount()
}

// GetDbTablesAccordingToSourceDbTables returns a dummy table mapping for CSV
// This is used for compatibility with the worker interface
func (s *CSVSource) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
	// For CSV, we create a virtual "database.table" mapping
	// Use the first file name (without extension) as the table name
	tableName := "csv_data"
	if len(s.files) > 0 {
		baseName := filepath.Base(s.files[0])
		tableName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}

	return map[string][]string{
		"csv": {tableName},
	}, nil
}

