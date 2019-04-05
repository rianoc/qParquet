//Load needed functions
\l qparquet.q

//Open handles to worker processes
.z.pd:`u#asc hopen each"J"$(.Q.opt .z.X)`slaves

file:"example.parquet";

columns:-1_getColumnNames[file]`

destination:`:splayed

conversionsPY:enlist[`time]!enlist "table[\"time\"]=pd.to_numeric(table[\"time\"])";
conversionsQ:enlist[`time]!enlist {`timestamp$x-`long$2000.01.01D-1970.01.01D};

start:.z.p;
//Distribute tasks to workers
//Each worker reads a column at a time
{[f;d;convPY;convQ;c]
  show string[.z.p],"  ",c;
  .Q.dd[d;`$c] set 
    $[(`$c) in key convQ;
      convQ[`$c];
	  (::)]
	0N!$[(`$c) in key convPY;
	     getColumnWithConversion[f;c;convPY`$c];
		 getColumn[f;c]]
    }[file;destination;conversionsPY;conversionsQ] peach columns

//Add a .d file to the destination to inform q of the order of columns
.Q.dd[destination;`.d] set `$columns

end:.z.p;

show "Took ",string end-start;

//Load the converted table
\l splayed

//Query the q table
show select from splayed
