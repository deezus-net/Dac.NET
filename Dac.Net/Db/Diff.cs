using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class Diff
    {
        public Dictionary<string, Table> AddedTables { get; set; } = new Dictionary<string, Table>();

        public string[] DeletedTableNames { get; set; } = { };
        public Dictionary<string, ModifiedTable> ModifiedTables { get; set; } = new Dictionary<string, ModifiedTable>();
        public DataBase CurrentDb { get; set; }
        public DataBase NewDb { get; set; }
    }

    public class ModifiedTable
    {
        public Column[] AddedColumns { get; set; } = { };
        public Dictionary<string, Column> ModifiedColumns { get; set; } = new Dictionary<string, Column>();
        public string[] DeletedColumnName { get; set; } = { };
        public Index[] AddedIndices { get; set; } = { };
        public Dictionary<string, Index> ModifiedIndices { get; set; } = new Dictionary<string, Index>();
        public string[] DeletedIndexNames { get; set; } = { };
    }
}