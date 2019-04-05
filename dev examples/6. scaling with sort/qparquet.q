//parquet library prints many warnings - ignore for this example
p)import warnings
p)warnings.filterwarnings("ignore")

//Import pandas, numpy, and pyarrow
p)import pandas as pd
p)import numpy as np
p)import pyarrow as pa
p)import pyarrow.parquet as pq

p)def getColumnNames(file):    return (pq.read_schema(file)).names

getColumnNames:.p.get`getColumnNames

p)def getColumns(file, cols): table=pq.read_table(file, columns=cols); return (table.to_pandas()).to_dict('list')

getColumns:.p.get`getColumns

getColumn:{[file;column] first value getColumns[file;enlist column]`}

.p.e "def getColumnWithConversion(file, col, conversion): ",
		"table=pq.read_table(file, columns=[col]);",
		"table=table.to_pandas();",
		"exec(conversion);",
		"return table.to_dict('list')";

getColumnWithConversionPY:.p.get`getColumnWithConversion

getColumnWithConversion:{[f;c;conv] first value getColumnWithConversionPY[f;c;conv]`}