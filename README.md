# DuckDB MCP Server

A Model Context Protocol (MCP) server that provides secure, read-only access to DuckDB databases and data files.

## Features

- **Read-only Safety**: All operations run in DuckDB's native read-only mode
- **Multiple Data Sources**: Support for DuckDB database files and directories with CSV, Parquet, JSON files
- **HIVE Partitioning**: Automatic detection of HIVE-style partitioned data structures
- **Rich Metadata**: Table schemas, statistics, and database information
- **Powerful Querying**: Execute any read-only SQL query with formatted results
- **Statistical Summaries**: Built-in table summarization using DuckDB's SUMMARIZE function

## Installation

```bash
npm install
npm run build
```

## Usage

### Basic Usage

```bash
# Connect to a DuckDB database file
npx duckmcp /path/to/database.duckdb

# Connect to a directory containing data files
npx duckmcp /path/to/data/directory

# Connect to HIVE partitioned data
npx duckmcp /path/to/partitioned/data
```

### Directory Structure Support

The server automatically detects and loads:
- CSV files (`*.csv`)
- Parquet files (`*.parquet`)
- JSON files (`*.json`, `*.jsonl`)

For directories, it uses glob patterns like `directory/**/*.csv` to support HIVE partitioning:

```
data/
├── year=2023/month=01/sales.parquet
├── year=2023/month=02/sales.parquet
└── year=2024/month=01/sales.parquet
```

## Available Tools

### `get_tables`
List all available tables and views in the database.

```json
{
  "name": "get_tables"
}
```

### `get_schema`
Get the schema (columns and types) for a specific table.

```json
{
  "name": "get_schema",
  "arguments": {
    "table_name": "my_table"
  }
}
```

### `describe_table`
Get detailed information about a table including schema and row count.

```json
{
  "name": "describe_table",
  "arguments": {
    "table_name": "my_table"
  }
}
```

### `execute_query`
Execute a SQL query against the database (read-only mode enforced by DuckDB).

```json
{
  "name": "execute_query",
  "arguments": {
    "sql": "SELECT * FROM my_table LIMIT 10",
    "limit": 100
  }
}
```

### `summarize_table`
Generate statistical summary of all columns using DuckDB's SUMMARIZE function.

```json
{
  "name": "summarize_table",
  "arguments": {
    "table_name": "my_table"
  }
}
```

### `get_database_info`
Get general information about the database and connection.

```json
{
  "name": "get_database_info"
}
```

## Examples

### Example Data Structure

Create some example data:

```bash
mkdir -p examples/data/year=2023/month=01
mkdir -p examples/data/year=2023/month=02

# Create sample CSV files
echo "id,name,value\n1,Alice,100\n2,Bob,200" > examples/data/year=2023/month=01/data.csv
echo "id,name,value\n3,Charlie,150\n4,David,250" > examples/data/year=2023/month=02/data.csv
```

### Running with Sample Data

```bash
npx duckmcp examples/data
```

This will automatically:
1. Detect CSV files in the directory
2. Create a table called `data_csv`
3. Include HIVE partition columns (`year`, `month`)

### Example Queries

Once connected, you can use these queries:

```sql
-- List all data with partition columns
SELECT * FROM data_csv;

-- Aggregate by partition
SELECT year, month, COUNT(*), AVG(value) 
FROM data_csv 
GROUP BY year, month;

-- Get table summary statistics
SUMMARIZE data_csv;
```

## Safety Features

- **Read-Only Mode**: DuckDB connection is configured with `access_mode: 'read_only'`
- **No Query Filtering**: DuckDB natively rejects any write operations (INSERT, UPDATE, DELETE, CREATE, etc.)
- **Error Handling**: Clear error messages for rejected operations
- **Safe Defaults**: All operations default to read-only behavior

## Error Handling

The server handles errors gracefully:
- Invalid SQL queries return DuckDB's native error messages
- Missing tables/columns return descriptive errors
- Connection issues are logged and reported
- Write operations are automatically rejected by DuckDB

## Development

```bash
# Install dependencies
npm install

# Run in development mode
npm run dev examples/data

# Build for production
npm run build

# Run tests
npm test

# Type checking
npm run typecheck

# Linting
npm run lint
```

## License

MIT