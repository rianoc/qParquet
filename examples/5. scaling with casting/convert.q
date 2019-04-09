//Load needed functions
\l ../../qparquet.q

//Open handles to worker processes
.z.pd:`u#asc hopen each"J"$(.Q.opt .z.X)`slaves

file:`:example.parquet;

columns:.qparquet.getColumnNames[file];

destination:`:splayed

pyConversions:enlist[`time]!enlist "table[\"time\"]=pandas.to_numeric(table[\"time\"])";
qConversions:enlist[`time]!enlist {`timestamp$x-`long$2000.01.01D-1970.01.01D};

start:.z.p;
//Distribute tasks to workers
//Each worker reads a column at a time
{[f;d;p;q;c]
  show string[.z.p],"  ",string c;
  .Q.dd[d;c] set .qparquet.getColumnsCustom[f;enlist c;p;q]c
    }[file;destination;pyConversions;qConversions] peach columns

//Add a .d file to the destination to inform q of the order of columns
.Q.dd[destination;`.d] set columns

end:.z.p;

show "Took ",string end-start;

//Load the converted table
\l splayed

//Query the q table
show select from splayed
