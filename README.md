# bend-archiver
Archive data from common databases into Databend with parallel sync (by key or time range).

## Supported sources
| Data source | Supported |
|:-----------|:---------:|
| MySQL      |    Yes    |
| PostgreSQL |    Yes    |
| TiDB       |    Yes    |
| SQL Server |    Yes    |
| CSV        |    Yes    |
| Oracle     | Coming soon |
| NDJSON     | Coming soon |

## Install
Download the binary from the [release page](https://github.com/databendcloud/bend-archiver/releases).

## Configure
Create `config/conf.json`.

Parameters (defaults are from code):
| Key | Required | Default | Notes |
|:----|:--------:|:--------|:------|
| `databaseType` | No | `mysql` | `mysql`, `tidb`, `pg`, `mssql`, `oracle`, `csv` |
| `sourceHost` | For DB sources | - | Source host |
| `sourcePort` | For DB sources | - | Source port |
| `sourceUser` | For DB sources | - | Source user |
| `sourcePass` | For DB sources | - | Source password |
| `sourceDB` | If no `sourceDbTables` | - | Source database |
| `sourceTable` | If no `sourceDbTables` | - | Source table |
| `sourceCSVPath` | For CSV only | - | Path to CSV file or directory |
| `sourceDbTables` | No | `[]` | Multi-table: `["dbRegex@tableRegex"]` |
| `sourceQuery` | No | - | Currently ignored |
| `sourceWhereCondition` | For DB sources | - | WHERE clause without `WHERE` |
| `sourceSplitKey` | If key split | - | Integer primary key (auto-set for CSV) |
| `sourceSplitTimeKey` | If time split | - | Time column (not supported for CSV) |
| `timeSplitUnit` | If time split | `hour` | `minute`, `quarter`, `hour`, `day` |
| `sslMode` | No | `disable` | Postgres only |
| `databendDSN` | Yes | `localhost:8000` | Databend DSN |
| `databendTable` | Yes | - | Target table |
| `batchSize` | Yes | `1000` | Rows per batch |
| `batchMaxInterval` | No | `3` | Seconds between batches |
| `copyPurge` | No | `true` | Databend COPY option |
| `copyForce` | No | `false` | Databend COPY option |
| `disableVariantCheck` | No | `true` | Databend COPY option |
| `userStage` | No | `~` | Databend stage |
| `deleteAfterSync` | No | `false` | Deletes source rows/files |
| `maxThread` | No | `1` | Max concurrency |
| `oracleSID` | No | - | Oracle SID |

Rules:
- `sourceWhereCondition` is always required for database sources; for time split use `t >= '...' and t < '...'` with `YYYY-MM-DD HH:MM:SS`.
- `sourceSplitKey` and `sourceSplitTimeKey` are mutually exclusive.
- For time split, `timeSplitUnit` is required.
- For CSV sources, only `sourceCSVPath` is required (other source parameters are ignored).

Example (key split):
```json
{
  "databaseType": "mysql",
  "sourceHost": "127.0.0.1",
  "sourcePort": 3306,
  "sourceUser": "root",
  "sourcePass": "123456",
  "sourceDB": "mydb",
  "sourceTable": "test_table",
  "sourceWhereCondition": "id > 0",
  "sourceSplitKey": "id",
  "databendDSN": "databend://username:password@host:port?sslmode=disable",
  "databendTable": "mydb.test_table",
  "batchSize": 40000,
  "maxThread": 5
}
```

Example (time split keys):
```json
{
  "sourceWhereCondition": "t1 >= '2024-06-01 00:00:00' and t1 < '2024-07-01 00:00:00'",
  "sourceSplitTimeKey": "t1",
  "timeSplitUnit": "hour"
}
```

Example (CSV file or directory):
```json
{
  "databaseType": "csv",
  "sourceCSVPath": "/path/to/data.csv",
  "databendDSN": "databend://username:password@localhost:8000?sslmode=disable",
  "databendTable": "default.my_table",
  "batchSize": 10000,
  "maxThread": 4,
  "deleteAfterSync": false
}
```

## Run
```bash
./bend-archiver -f config/conf.json
```
If `-f` is omitted, it loads `config/conf.json`.

## Development
### Build
```bash
go build -o bend-archiver ./cmd
```

### Tests
```bash
# Run all tests
go test ./...

# Run only unit tests (no external services needed)
go test ./source ./config

# Run CSV tests specifically
go test -v ./source -run CSV
go test -v ./config -run TestPreCheckConfig_CSV
go test -v ./cmd -run TestCSV
```

### Run from source
```bash
go run ./cmd -f config/conf.json
```

## Notes
- Multi-table sync uses regex in `sourceDbTables` (example: `["^mydb$@^test_table_.*$"]`).
- The MySQL driver reports BOOL as `TINYINT(1)`, so use `TINYINT` in Databend for boolean columns.
- COPY options reference: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table#copy-options
- CSV files:
  - First row must be column headers
  - Supports single file or directory (all `.csv` files)
  - Automatically detects data types (integers, floats, booleans, strings)
  - Uses row numbers for parallel processing
  - Set `deleteAfterSync: true` to remove CSV files after successful import
