# qparquet

Library to read and write data from parquet files.

Apache Parquet is a columnar storage format available to any project in the Hadoop ecosystem

## Requirements

* kdb+ ≥3.5 64-bit
  * [embedPy](https://code.kx.com/v2/ml/embedpy/)
* Python ≥3.5
  * [pandas](https://pandas.pydata.org/)
  * [pyarrow](https://arrow.apache.org/docs/python/)

## Run

Start q with qparquet

```bash
q qparquet.q
```

Or from q, load `qparquet.q`.

```q
q)\l qparquet.q
```

## Functions

### .qparquet.getTable

### .qparquet.setTable

### .qparquet.getColumnNames

### .qparquet.getColumns

### .qparquet.getColumn

### .qparquet.getColumnCustom

### .qparquet.getColumnsCustom

### .qparquet.getTableCustom

## Reference links

* [https://arrow.apache.org/docs/python/parquet.html](https://arrow.apache.org/docs/python/parquet.html)
* [https://code.kx.com/v2/ml/embedpy/](https://code.kx.com/v2/ml/embedpy/)