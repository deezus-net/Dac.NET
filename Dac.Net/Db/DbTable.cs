using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class DbTable
    {
        public Dictionary<string, DbColumn> DbColumns { get; set; } = new Dictionary<string, DbColumn>();
        public Dictionary<string, DbIndex> DbIndices { get; set; } = new Dictionary<string, DbIndex>();

    }

    public class DbColumn
    {
        public string DisplayName { get; set; }
        public string Type { get; set; }
        public int Length { get; set; }
        public bool Pk { get; set; }
        public bool Id { get; set; }
        public bool NotNull { get; set; }
        public string Check { get; set; }
        public string CheckName { get; set; }
        public Dictionary<string, DbForeignKey> Fk { get; set; } = new Dictionary<string, DbForeignKey>();
        public string Default { get; set; }
        public string DefaultName { get; set; }
        public string Comment { get; set; }
    }

    public class DbIndex
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public Dictionary<string, string> Columns { get; set; } = new Dictionary<string, string>();
        public bool Unique { get; set; }
    }

    public class DbForeignKey
    {
        public string Table { get; set; }
        public string Column { get; set; }
        public string Update { get; set; }
        public string Delete { get; set; }
    }

    public class DbDiff
    {
        public Dictionary<string, DbTable> AddedTables { get; set; }
        public string[] DeletedTableNames { get; set; }
        public Dictionary<string, ModifiedTable> ModifiedTables { get; set; }
        public Dictionary<string, DbTable> CurrentTables { get; set; }
        public Dictionary<string, DbTable> NewTables { get; set; }
    }

    public class ModifiedTable
    {
        public DbColumn[] AddedColumns { get; set; }
        public Dictionary<string, DbColumn[]> ModifiedColumns { get; set; }
        public string[] DeletedColumnName { get; set; }
        public DbIndex[] AddedIndices { get; set; }
        public Dictionary<string, DbIndex[]> ModifiedIndices { get; set; }
        public string[] DeletedIndexNames { get; set; }
    }
}