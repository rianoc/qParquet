//Load needed functions
\l ../../qparquet.q

//Open handles to worker processes
.z.pd:`u#asc hopen each"J"$(.Q.opt .z.X)`slaves

file:`:example.parquet;

columns:.qparquet.getColumnNames[file];

destination:`:splayed

//Distribute tasks to workers
//Each worker reads a column at a time
{[f;d;c] .Q.dd[d;c] set .qparquet.getColumn[f;c]}[file;destination] peach columns

//Add a .d file to the destination to inform q of the order of columns
.Q.dd[destination;`.d] set columns

//Load the converted table
\l splayed

//Query the q table
show select from splayed 