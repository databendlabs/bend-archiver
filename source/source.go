package source

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-archiver/config"
)

type Sourcer interface {
	AdjustBatchSizeAccordingToSourceDbTable() uint64
	GetSourceReadRowsCount() (int, error)
	GetMinMaxSplitKey() (uint64, uint64, error)
	GetMinMaxTimeSplitKey() (string, string, error)
	DeleteAfterSync() error
	QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error)
	GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error)
	GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error)
	GetAllSourceReadRowsCount() (int, error)
	GetDbTablesAccordingToSourceDbTables() (map[string][]string, error)
}

func NewSource(cfg *config.Config) (Sourcer, error) {
	switch cfg.DatabaseType {
	case "mysql":
		return NewMysqlSource(cfg)
	case "tidb":
		return NewMysqlSource(cfg)
	case "pg":
		return NewPostgresSource(cfg)
	case "oracle":
		return NewOracleSource(cfg)
	case "mssql":
		return NewSqlServerSource(cfg)
	case "csv":
		return NewCSVSource(cfg)
	default:
		return NewMysqlSource(cfg)
	}
}

func SlimCondition(maxThread int, minSplitKey, maxSplitKey uint64) [][]uint64 {
	var conditions [][]uint64
	if minSplitKey > maxSplitKey {
		return conditions
	}
	rangeSize := (maxSplitKey - minSplitKey) / uint64(maxThread)
	for i := 0; i < maxThread; i++ {
		lowerBound := minSplitKey + rangeSize*uint64(i)
		upperBound := lowerBound + rangeSize
		if i == maxThread-1 {
			// Ensure the last condition includes maxSplitKey
			upperBound = maxSplitKey
		}
		conditions = append(conditions, []uint64{lowerBound, upperBound})
	}
	return conditions
}

func SplitCondition(sourceSplitKey string, batchSize, minSplitKey, maxSplitKey uint64) []string {
	var conditions []string
	for {
		if minSplitKey >= maxSplitKey {
			conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s <= %d)", sourceSplitKey, minSplitKey, sourceSplitKey, maxSplitKey))
			break
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= %d and %s < %d)", sourceSplitKey, minSplitKey, sourceSplitKey, minSplitKey+batchSize))
		minSplitKey += batchSize
	}
	return conditions
}

func SplitConditionAccordingMaxGoRoutine(sourceSplitKey string, batchSize, minSplitKey, maxSplitKey, allMax uint64) <-chan string {
	conditions := make(chan string, 100) // make a buffered channel

	go func() {
		defer close(conditions) // make sure close channel

		if minSplitKey > maxSplitKey {
			return
		}

		for {
			if (minSplitKey + batchSize - 1) >= maxSplitKey {
				if minSplitKey > allMax {
					return
				}
				if maxSplitKey == allMax {
					conditions <- fmt.Sprintf("(%s >= %d and %s <= %d)", sourceSplitKey, minSplitKey, sourceSplitKey, maxSplitKey)
				} else {
					conditions <- fmt.Sprintf("(%s >= %d and %s < %d)", sourceSplitKey, minSplitKey, sourceSplitKey, maxSplitKey)
				}
				break
			}
			if (minSplitKey + batchSize - 1) >= allMax {
				conditions <- fmt.Sprintf("(%s >= %d and %s <= %d)", sourceSplitKey, minSplitKey, sourceSplitKey, allMax)
				return
			}
			conditions <- fmt.Sprintf("(%s >= %d and %s < %d)", sourceSplitKey, minSplitKey, sourceSplitKey, minSplitKey+batchSize-1)
			minSplitKey += batchSize - 1
		}
	}()

	return conditions
}

func SplitTimeConditionsByMaxThread(conditions []string, maxThread int) [][]string {
	// If maxThread is greater than the length of conditions, return conditions as a single group
	if maxThread >= len(conditions) {
		return [][]string{conditions}
	}
	var splitConditions [][]string
	chunkSize := (len(conditions) + maxThread - 1) / maxThread
	for i := 0; i < len(conditions); i += chunkSize {
		end := i + chunkSize
		if end > len(conditions) {
			end = len(conditions)
		}
		splitConditions = append(splitConditions, conditions[i:end])
	}
	return splitConditions
}

func SplitConditionAccordingToTimeSplitKey(cfg *config.Config, minTimeSplitKey, maxTimeSplitKey string) ([]string, error) {
	var conditions []string

	// Parse the time strings
	minTime, err := parseTimeDynamic(minTimeSplitKey)
	if err != nil {
		return nil, err
	}

	maxTime, err := parseTimeDynamic(maxTimeSplitKey)
	if err != nil {
		return nil, err
	}
	if minTime.After(maxTime) {
		return conditions, nil
	}

	// Iterate over the time range
	for {
		if minTime.After(maxTime) {
			conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s <= '%s')", cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), cfg.SourceSplitTimeKey, maxTime.Format("2006-01-02 15:04:05")))
			break
		}
		if minTime.Equal(maxTime) {
			conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s <= '%s')", cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), cfg.SourceSplitTimeKey, maxTime.Format("2006-01-02 15:04:05")))
			break
		}
		conditions = append(conditions, fmt.Sprintf("(%s >= '%s' and %s < '%s')", cfg.SourceSplitTimeKey, minTime.Format("2006-01-02 15:04:05"), cfg.SourceSplitTimeKey, minTime.Add(cfg.GetTimeRangeBySplitUnit()).Format("2006-01-02 15:04:05")))
		minTime = minTime.Add(cfg.GetTimeRangeBySplitUnit())
	}

	return conditions, nil
}

func GenerateJSONFile(columns []string, data [][]interface{}) (string, int, error) {
	l := logrus.WithFields(logrus.Fields{"tardatabend": "IngestData"})
	var batchJsonData []string

	for _, row := range data {
		if len(row) == 0 {
			continue
		}
		rowMap := make(map[string]interface{})
		for i, column := range columns {
			rowMap[column] = row[i]
		}
		jsonData, err := json.Marshal(rowMap)
		if err != nil {
			return "", 0, err
		}
		batchJsonData = append(batchJsonData, string(jsonData))
	}

	fileName, bytesSize, err := generateNDJsonFile(batchJsonData)
	if err != nil {
		l.Errorf("generate NDJson file failed: %v\n", err)
		return "", 0, err
	}
	return fileName, bytesSize, nil
}

func generateNDJsonFile(batchJsonData []string) (string, int, error) {
	fileName := fmt.Sprintf("databend-ingest-%d-", time.Now().UnixNano())
	outputFile, err := os.CreateTemp("/tmp", fileName+"*.ndjson")
	if err != nil {
		return "", 0, err
	}
	defer outputFile.Close()

	// Create a buffered writer for the Ndjson file
	writer := bufio.NewWriter(outputFile)
	bytesSum := 0

	for _, data := range batchJsonData {
		n, err := writer.WriteString(data + "\n")
		if err != nil {
			return "", 0, err
		}
		bytesSum += n
	}
	// Flush any remaining data to the NDJson file
	err = writer.Flush()
	if err != nil {
		return "", 0, err
	}
	return outputFile.Name(), bytesSum, err
}

func parseTimeDynamic(timeStr string) (time.Time, error) {
	var layouts = []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000Z07:00",
	}

	var err error
	var parsedTime time.Time
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, timeStr)
		if err == nil {
			return parsedTime, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse time: %v", err)
}

func toUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case uint64:
		return v, nil
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative value: %d", v)
		}
		return uint64(v), nil
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case string:
		return strconv.ParseUint(v, 10, 64)
	case float64:
		if v < 0 {
			return 0, fmt.Errorf("negative float value: %f", v)
		}
		return uint64(v), nil
	default:
		return 0, fmt.Errorf("unexpected type: %T", val)
	}
}
