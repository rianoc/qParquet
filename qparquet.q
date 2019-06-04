.qparquet.init:{[]
  .qparquet.filePath:{x -3+count x} value .z.s;
  slash:$[.z.o like "w*";"\\";"/"];
  .qparquet.basePath:slash sv -1_slash vs .qparquet.filePath;
  if[not `p in key `;system"l ",getenv[`QHOME],slash,"p.q"];
  .p.e {"exec(open(r'",x,"').read());"}.qparquet.basePath,slash,"qparquet.py";
  };

.qparquet.init[];

.qparquet.py.getTable:.p.get`getTable;

.qparquet.getTable:{[file]
  tab:.qparquet.py.getTable[1_ string file]`;
  flip .p.wrap[tab][`:to_dict;`list]`
  };

.qparquet.py.setTable:.p.get`setTable;

.qparquet.setTable:{[file;table]
  `$ .qparquet.py.setTable[1_ string file;table]`
  };

.qparquet.py.getColumnNames:.p.get`getColumnNames;

.qparquet.getColumnNames:{[file]
  {x where not x like "_*"}`$ .qparquet.py.getColumnNames[1_ string file]`
  };

.qparquet.py.getColumns:.p.get`getColumns;

.qparquet.getColumns:{[file;columns]
  flip .qparquet.py.getColumns[1_ string file;string columns]`
  };

.qparquet.getColumn:{[file;column]
  .qparquet.getColumns[file;enlist column]column
  };

.qparquet.py.getColumnCustom:.p.get`getColumnCustom;

.qparquet.getColumnCustom:{[file;column;conversion]
  .qparquet.py.getColumnCustom[1_ string file;string column;conversion][`] column
  };
  
.qparquet.getColumnsCustom:{[file;columns;pyConversions;qConversions]
  columnData:{[f;p;q;c] 
    colData:value $[c in key p;.qparquet.getColumnCustom[;;p c]; .qparquet.getColumn],(f;c);
    $[c in key q;q[c] colData;colData]
    }[file;pyConversions;qConversions] each columns;
  flip columns!columnData
 };
 
 .qparquet.getTableCustom:{[file;pyConversions;qConversions]
  columns:.qparquet.getColumnNames[file];
  .qparquet.getColumnsCustom[file;columns;pyConversions;qConversions]
 };