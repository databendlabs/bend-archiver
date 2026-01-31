package source

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/test-go/testify/assert"

	"github.com/databendcloud/bend-archiver/config"
)

// TestNewCSVSource tests creating a new CSV source
func TestNewCSVSource(t *testing.T) {
	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")
	content := "id,name,age\n1,Alice,25\n2,Bob,30\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, src)
	assert.Equal(t, 1, len(src.files))
	assert.Equal(t, csvFile, src.files[0])
}

// TestNewCSVSource_Directory tests creating a CSV source from a directory
func TestNewCSVSource_Directory(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple CSV files
	files := []string{"test1.csv", "test2.csv", "test3.csv"}
	for _, file := range files {
		content := "id,name\n1,Test\n"
		err := os.WriteFile(filepath.Join(tmpDir, file), []byte(content), 0644)
		assert.NoError(t, err)
	}

	// Create a non-CSV file (should be ignored)
	err := os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("test"), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: tmpDir,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, src)
	assert.Equal(t, 3, len(src.files))
}

// TestNewCSVSource_NoFiles tests error when no CSV files found
func TestNewCSVSource_NoFiles(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: tmpDir,
		BatchSize:     10,
	}

	_, err := NewCSVSource(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no CSV files found")
}

// TestCSVSource_GetSourceReadRowsCount tests counting rows in CSV files
func TestCSVSource_GetSourceReadRowsCount(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")
	content := "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	count, err := src.GetSourceReadRowsCount()
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

// TestCSVSource_GetMinMaxSplitKey tests getting min/max split keys
func TestCSVSource_GetMinMaxSplitKey(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")
	content := "id,name\n1,Alice\n2,Bob\n3,Charlie\n4,David\n5,Eve\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	min, max, err := src.GetMinMaxSplitKey()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), min)
	assert.Equal(t, uint64(5), max)
}

// TestCSVSource_QueryTableData tests querying data from CSV
func TestCSVSource_QueryTableData(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")
	content := "id,name,age,active\n1,Alice,25,true\n2,Bob,30,false\n3,Charlie,35,true\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	// Query rows 1-3
	data, columns, err := src.QueryTableData(1, "(row_num >= 1 and row_num < 3)")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(columns))
	assert.Equal(t, []string{"id", "name", "age", "active"}, columns)
	assert.Equal(t, 2, len(data))

	// Check first row
	assert.Equal(t, int64(1), data[0][0])
	assert.Equal(t, "Alice", data[0][1])
	assert.Equal(t, int64(25), data[0][2])
	assert.Equal(t, true, data[0][3])

	// Check second row
	assert.Equal(t, int64(2), data[1][0])
	assert.Equal(t, "Bob", data[1][1])
	assert.Equal(t, int64(30), data[1][2])
	assert.Equal(t, false, data[1][3])
}

// TestConvertCSVValue tests type conversion
func TestConvertCSVValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected interface{}
	}{
		{"integer", "123", int64(123)},
		{"negative integer", "-456", int64(-456)},
		{"float", "123.45", float64(123.45)},
		{"boolean true", "true", true},
		{"boolean TRUE", "TRUE", true},
		{"boolean false", "false", false},
		{"boolean FALSE", "FALSE", false},
		{"string", "hello", "hello"},
		{"email", "test@example.com", "test@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertCSVValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseRowCondition tests parsing row conditions
func TestParseRowCondition(t *testing.T) {
	tests := []struct {
		name          string
		condition     string
		expectedStart uint64
		expectedEnd   uint64
		expectError   bool
	}{
		{
			name:          "basic range",
			condition:     "(row_num >= 1 and row_num < 10)",
			expectedStart: 1,
			expectedEnd:   10,
			expectError:   false,
		},
		{
			name:          "with <= operator",
			condition:     "(row_num >= 5 and row_num <= 15)",
			expectedStart: 5,
			expectedEnd:   16, // <= converts to <
			expectError:   false,
		},
		{
			name:          "large numbers",
			condition:     "(row_num >= 1000 and row_num < 2000)",
			expectedStart: 1000,
			expectedEnd:   2000,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := parseRowCondition(tt.condition)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStart, start)
				assert.Equal(t, tt.expectedEnd, end)
			}
		})
	}
}

// TestCSVSource_MultipleFiles tests reading from multiple CSV files
func TestCSVSource_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first CSV file
	file1 := filepath.Join(tmpDir, "test1.csv")
	content1 := "id,name\n1,Alice\n2,Bob\n"
	err := os.WriteFile(file1, []byte(content1), 0644)
	assert.NoError(t, err)

	// Create second CSV file
	file2 := filepath.Join(tmpDir, "test2.csv")
	content2 := "id,name\n3,Charlie\n4,David\n"
	err = os.WriteFile(file2, []byte(content2), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: tmpDir,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	// Total rows should be 4
	count, err := src.GetSourceReadRowsCount()
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Query all rows
	data, columns, err := src.QueryTableData(1, "(row_num >= 1 and row_num < 5)")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columns))
	assert.Equal(t, 4, len(data))
}

// TestCSVSource_DeleteAfterSync tests deleting CSV files after sync
func TestCSVSource_DeleteAfterSync(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "test.csv")
	content := "id,name\n1,Alice\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:    "csv",
		SourceCSVPath:   csvFile,
		BatchSize:       10,
		DeleteAfterSync: true,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	// File should exist
	_, err = os.Stat(csvFile)
	assert.NoError(t, err)

	// Delete after sync
	err = src.DeleteAfterSync()
	assert.NoError(t, err)

	// File should not exist
	_, err = os.Stat(csvFile)
	assert.True(t, os.IsNotExist(err))
}

// TestCSVSource_GetDbTablesAccordingToSourceDbTables tests virtual table mapping
func TestCSVSource_GetDbTablesAccordingToSourceDbTables(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "my_data.csv")
	content := "id,name\n1,Alice\n"
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	dbTables, err := src.GetDbTablesAccordingToSourceDbTables()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(dbTables))
	assert.Contains(t, dbTables, "csv")
	assert.Equal(t, []string{"my_data"}, dbTables["csv"])
}

// TestCSVSource_EmptyFile tests handling empty CSV file
func TestCSVSource_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "empty.csv")
	content := "id,name\n" // Only header, no data
	err := os.WriteFile(csvFile, []byte(content), 0644)
	assert.NoError(t, err)

	cfg := &config.Config{
		DatabaseType:  "csv",
		SourceCSVPath: csvFile,
		BatchSize:     10,
	}

	src, err := NewCSVSource(cfg)
	assert.NoError(t, err)

	count, err := src.GetSourceReadRowsCount()
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	min, max, err := src.GetMinMaxSplitKey()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), min)
	assert.Equal(t, uint64(0), max)
}


