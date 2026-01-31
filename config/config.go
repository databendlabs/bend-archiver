package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/pkg/errors"
)

type TimeSplitUnit int

const (
	Minute  TimeSplitUnit = iota
	Quarter               // 15 minutes
	Hour
	Day
)

var TimeSplitUnitToString = map[TimeSplitUnit]string{
	Minute:  "minute",
	Quarter: "quarter",
	Hour:    "hour",
	Day:     "day",
}

var StringToTimeSplitUnit = map[string]TimeSplitUnit{
	"minute":  Minute,
	"quarter": Quarter,
	"hour":    Hour,
	"day":     Day,
}

type Config struct {
	// Source configuration
	DatabaseType         string   `json:"databaseType" default:"mysql"`
	SourceHost           string   `json:"sourceHost"`
	SourcePort           int      `json:"sourcePort"`
	SourceUser           string   `json:"sourceUser"`
	SourcePass           string   `json:"sourcePass"`
	SourceDB             string   `json:"sourceDB"`
	SSLMode              string   `json:"sslMode"`
	SourceTable          string   `json:"sourceTable"`
	SourceDbTables       []string `json:"sourceDbTables"`       // source db tables format: [db1.table1,db2.table2] or [db.*@table.*,mydb.*.table.*]
	SourceQuery          string   `json:"sourceQuery"`          // select * from table where condition
	SourceWhereCondition string   `json:"sourceWhereCondition"` //example: where id > 100 and id < 200 and time > '2023-01-01'
	SourceSplitKey       string   `json:"sourceSplitKey"`       // primary split key for split table, only for int type
	// the format of time field must be: 2006-01-02 15:04:05
	SourceSplitTimeKey string `json:"SourceSplitTimeKey"`           // time field for split table
	TimeSplitUnit      string `json:"TimeSplitUnit" default:"hour"` // time split unit, default is hour, option is: minute, hour, day

	// CSV specific configuration
	SourceCSVPath string `json:"sourceCSVPath"` // path to CSV file or directory containing CSV files

	// Databend configuration
	DatabendDSN      string `json:"databendDSN" default:"localhost:8000"`
	DatabendTable    string `json:"databendTable"`
	BatchSize        int64  `json:"batchSize" default:"1000"`
	BatchMaxInterval int    `json:"batchMaxInterval" default:"3"` // for rate limit control

	// related docs: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table
	CopyPurge           bool   `json:"copyPurge" default:"true"`
	CopyForce           bool   `json:"copyForce" default:"false"`
	DisableVariantCheck bool   `json:"disableVariantCheck" default:"true"`
	UserStage           string `json:"userStage" default:"~"`
	DeleteAfterSync     bool   `json:"deleteAfterSync" default:"false"`
	MaxThread           int    `json:"maxThread" default:"1"` // only supported with SourceSplitKey (auto increment)
	// Oracle
	OracleSID string `json:"oracleSID"`
}

func LoadConfig(configFile string) (*Config, error) {
	conf := Config{}

	f, err := os.Open(configFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&conf)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return &conf, err
	}
	preCheckConfig(&conf)

	return &conf, nil
}

func preCheckConfig(cfg *Config) {
	if cfg.UserStage == "" {
		cfg.UserStage = "~"
	}
	if cfg.MaxThread == 0 {
		cfg.MaxThread = 1
	}

	// For CSV files, skip database-specific validations
	if cfg.DatabaseType == "csv" {
		if cfg.SourceCSVPath == "" {
			panic("sourceCSVPath must be set for CSV database type")
		}
		// For CSV, we use row numbers as split key, so we set a default
		if cfg.SourceSplitKey == "" {
			cfg.SourceSplitKey = "row_num"
		}
		// Set a default where condition for CSV
		if cfg.SourceWhereCondition == "" {
			cfg.SourceWhereCondition = "row_num >= 1"
		}
		return
	}

	// Database-specific validations
	if cfg.SourceSplitKey != "" && cfg.SourceSplitTimeKey != "" {
		panic("cannot set both sourceSplitKey and sourceSplitTimeKey")
	}
	if cfg.SourceSplitKey == "" && cfg.SourceSplitTimeKey == "" {
		panic("must set one of sourceSplitKey and sourceSplitTimeKey")
	}
	if cfg.SourceSplitTimeKey != "" || cfg.SourceSplitKey != "" {
		if cfg.SourceWhereCondition == "" {
			panic("must set sourceWhereCondition when sourceSplitTimeKey is set")
		}
	}
	if cfg.SourceSplitTimeKey != "" {
		if cfg.MaxThread > 1 {
			panic("SourceSplitTimeKey does not support MaxThread > 1; use SourceSplitKey for parallelism")
		}
		// time warehouse condition must be  x < time and y > time
		err := validateSourceSplitTimeKey(cfg.SourceWhereCondition)
		if err != nil {
			panic(err)
		}
	}
	if cfg.SourceSplitTimeKey != "" {
		err := checkTimeSplitUnit(cfg.TimeSplitUnit)
		if err != nil {
			panic(err)
		}
	}
}

func validateSourceSplitTimeKey(value string) error {
	// 正则表达式匹配 field>'x' and field <'y' 或者 field >= 'x' and field <='y', 或者 field >='x' and field <'y', 或者 field>'x' and field <='y' 的格式
	pattern := `^\w+\s*(>|>=)\s*'[^']*'\s+and\s+\w+\s*(<|<=)\s*'[^']*'$`
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		return err
	}
	if !matched {
		return errors.New("SourceSplitTimeKey does not match the required format")
	}
	return nil
}

func checkTimeSplitUnit(unit string) error {
	_, ok := StringToTimeSplitUnit[unit]
	if !ok {
		return fmt.Errorf("invalid TimeSplitUnit: %s, it should be 'minute', 'quarter', 'hour', or 'day'", unit)
	}
	return nil
}

func (c *Config) GetTimeRangeBySplitUnit() time.Duration {
	switch StringToTimeSplitUnit[c.TimeSplitUnit] {
	case Minute:
		return 10 * time.Minute
	case Quarter:
		return 15 * time.Minute
	case Hour:
		return 2 * time.Hour
	case Day:
		return 24 * time.Hour
	default:
		return 0
	}
}
