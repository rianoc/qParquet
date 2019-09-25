.qparquet.init:{[]
  .qparquet.filePath:{x -3+count x} value .z.s;
  slash:$[.z.o like "w*";"\\";"/"];
  .qparquet.basePath:slash sv -1_slash vs .qparquet.filePath;
  if[not `p in key `;system"l ",getenv[`QHOME],slash,"p.q"];
  .p.e"import sys";
  .p.e "sys.path.append(\"",ssr[;"\\";"\\\\"] .qparquet.basePath,"\")";
  .qparquet.py.lib:.p.import`qparquet;
  };

.qparquet.init[];

.qparquet.getTable:{[file]
  tab:.qparquet.py.lib[`:getTable][1_ string file]`;
  flip .p.wrap[tab][`:to_dict;`list]`
  };

.qparquet.setTable:{[file;table]
  `$ .qparquet.py.lib[`:setTable][1_ string file;table]`
  };

.qparquet.getColumnNames:{[file]
  {x where not x like "_*"}`$ .qparquet.py.lib[`:getColumnNames][1_ string file]`
  };

.qparquet.getColumns:{[file;columns]
  flip .qparquet.py.lib[`:getColumns][1_ string file;string columns]`
  };

.qparquet.getColumn:{[file;column]
  .qparquet.getColumns[file;enlist column]column
  };

.qparquet.getColumnCustom:{[file;column;conversion]
  .qparquet.py.lib[`:getColumnCustom][1_ string file;string column;conversion][`] column
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