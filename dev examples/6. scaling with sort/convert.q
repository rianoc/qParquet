//Load needed functions
\l qparquet.q

//Open handles to worker processes
.z.pd:`u#asc hopen each"J"$(.Q.opt .z.X)`slaves

file:"example.parquet";

columns:-1_getColumnNames[file]`

destination:`:splayed

conversionsPY:enlist[`time]!enlist "table[\"time\"]=pd.to_numeric(table[\"time\"])";
conversionsQ:`time`sym!({`timestamp$x-`long$2000.01.01D-1970.01.01D};`$);

start:.z.p;

sortTab:flip `time`sym!{[c] $[(`$c) in key conversionsQ;conversionsQ[`$c];(::)]
                         getColumnWithConversion[file;c;conversionsPY`$c]} each 2#columns;
sortTab:update ind:i from sortTab;
sortTab:update `p#sym from `sym`time xasc sortTab;
.Q.dd[destination;`] set .Q.en[`.] delete ind from sortTab;

-25!(.z.pd;({sortInd::x};sortTab`ind));

//Distribute tasks to workers
//Each worker reads a column at a time
{[f;d;convPY;convQ;c]
  show string[.z.p],"  ",c;
  .Q.dd[d;`$c] set {x[sortInd]}
    $[(`$c) in key convQ;
      convQ[`$c];
	  (::)]
	$[(`$c) in key convPY;
	     getColumnWithConversion[f;c;convPY`$c];
		 getColumn[f;c]]
    }[file;destination;conversionsPY;conversionsQ] peach 2_columns;

//Add a .d file to the destination to inform q of the order of columns
.Q.dd[destination;`.d] set `$columns

end:.z.p;

show "Took ",string end-start;

//Load the converted table
\l splayed

//Query the q table
show select from splayed

show meta splayed