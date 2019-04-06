.qparquet.init:{[]
  .qparquet.filePath:{x -3+count x} value .z.s;
  .qparquet.basePath:"\\" sv -1_"\\" vs .qparquet.filePath;
  system"l ",getenv[`QHOME],"\\p.q";
  .p.e {"exec(open(r'",x,"').read());"}.qparquet.basePath,$[.z.o like "w*";"\\";"/"],"qparquet.py";
  };

.qparquet.init[];

.qparquet.py.getColumnNames:.p.get`getColumnNames;

.qparquet.getColumnNames:{[file]
  .qparquet.py.getColumnNames[1_ string file]`
  };

.qparquet.py.getColumns:.p.get`getColumns;

.qparquet.getColumns:{[file;columns]
  .qparquet.py.getColumns[1_ string file;string columns]`
  };

.qparquet.getColumn:{[file;column]
  first value .qparquet.getColumns[file;enlist column]
  };

.qparquet.py.getColumnCutsom:.p.get`getColumnCustom;

.qparquet.getColumnCustom:{[file;column;conversion]
   first value .qparquet.py.getColumnCustom[file;column;conversion]
  };