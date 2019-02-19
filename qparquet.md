

```q
//parquet library prints many warnings - ignore for this example
p)import warnings
p)warnings.filterwarnings("ignore")
```


```q
//Import pandas, numpy, and pyarrow
p)import pandas as pd
p)import numpy as np
p)import pyarrow as pa
```


```q
//Create a sample table
qtab:([] one:-1 0 2.5;two:("foo";"bar";"baz");three:101b)
qtab
```




    one two   three
    ---------------
    -1  "foo" 1    
    0   "bar" 0    
    2.5 "baz" 1    





```q
//Create a dataframe from the q table
.p.set[`df;] .p.import[`pandas;`:DataFrame]qtab
```


```q
p)from IPython.display import display, HTML
p)display(HTML(df.to_html()))
```


<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>one</th>
      <th>two</th>
      <th>three</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>-1.0</td>
      <td>foo</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.0</td>
      <td>bar</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2.5</td>
      <td>baz</td>
      <td>True</td>
    </tr>
  </tbody>
</table>



```q
//Write the dataframe to a parquet file
p)import pyarrow.parquet as pq
p)table = pa.Table.from_pandas(df)
p)pq.write_table(table, 'example.parquet')
```


```q
//Read the parquet file back to a dataframe
p)table2 = pq.read_table('example.parquet')
p)df2 = table2.to_pandas()
p)display(HTML(df2.to_html()))
```


<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>one</th>
      <th>two</th>
      <th>three</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>-1.0</td>
      <td>foo</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.0</td>
      <td>bar</td>
      <td>False</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2.5</td>
      <td>baz</td>
      <td>True</td>
    </tr>
  </tbody>
</table>



```q
//Bring the dataframe in to q as a wrapped foreign 
df2:.p.wrap .p.pyget`df2
print df2
```

       one  two  three
    0 -1.0  foo   True
    1  0.0  bar  False
    2  2.5  baz   True



```q
//Extract the table data as a dictionary
qdict:df2[`:to_dict;`list]`
qdict
```




    one  | -1    0     2.5  
    two  | "foo" "bar" "baz"
    three| 1     0     1    





```q
//Flip the dictionary to give a table
flip qdict
```




    one two   three
    ---------------
    -1  "foo" 1    
    0   "bar" 0    
    2.5 "baz" 1    



