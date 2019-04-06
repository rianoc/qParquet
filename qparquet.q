.qparquet.init:{[]
  .qparquet.filePath:{x -3+count x} value .z.s;
  .qparquet.basePath:"\\" sv -1_"\\" vs .qparquet.filePath;
  if[not `p in key `;system"l ",getenv[`QHOME],"\\p.q"];
  .p.e {"exec(open(r'",x,"').read());"}.qparquet.basePath,$[.z.o like "w*";"\\";"/"],"qparquet.py";
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
  `$ .qparquet.py.getColumnNames[1_ string file]`
  };

.qparquet.py.getColumns:.p.get`getColumns;

.qparquet.getColumns:{[file;columns]
  flip .qparquet.py.getColumns[1_ string file;string columns]`
  };

.qparquet.getColumn:{[file;column]
  .qparquet.getColumns[file;enlist column]column
  };

.qparquet.py.getColumnCutsom:.p.get`getColumnCustom;

.qparquet.getColumnCustom:{[file;column;conversion]
   .qparquet.py.getColumnCustom[file;column;conversion]column
  };