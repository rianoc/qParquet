//Load needed functions
\l ../../qparquet.q

//Open handles to worker processes
.z.pd:`u#asc hopen each"J"$(.Q.opt .z.X)`slaves

file:`:example.parquet;

columns:.qparquet.getColumnNames[file];

destination:`:splayed

pyConversions:enlist[`time]!enlist "table[\"time\"]=pandas.to_numeric(table[\"time\"])";
qConversions:`time`sym!({`timestamp$x-`long$2000.01.01D-1970.01.01D};`$);

start:.z.p;

sortTab:.qparquet.getColumnsCustom[file;2#columns;pyConversions;qConversions];
sortTab:update ind:i from sortTab;
sortTab:update `p#sym from `sym`time xasc sortTab;
.Q.dd[destination;`] set .Q.en[`.] delete ind from sortTab;

-25!(.z.pd;({sortInd::x};sortTab`ind));
{neg[x][]} each .z.pd;

//Distribute tasks to workers
//Each worker reads a column at a time
{[f;d;p;q;c]
  show string[.z.p],"  ",string c;
  .Q.dd[d;c] set @[;sortInd] .qparquet.getColumnsCustom[f;enlist c;p;q]c
    }[file;destination;pyConversions;qConversions] peach columns except `time`sym

//Add a .d file to the destination to inform q of the order of columns
.Q.dd[destination;`.d] set columns

end:.z.p;

show "Took ",string end-start;

//Load the converted table
\l splayed

//Query the q table
show select from splayed

show meta splayed