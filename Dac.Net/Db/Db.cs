using System.Collections.Generic;
using System.Linq;

namespace Dac.Net.Db
{
    public class Db
    {
        public Dictionary<string, DbTable> Tables { get; set; } = new Dictionary<string, DbTable>();
    }


    public class DbTable
    {
        public Dictionary<string, DbColumn> Columns { get; set; } = new Dictionary<string, DbColumn>();
        public Dictionary<string, DbIndex> Indices { get; set; } = new Dictionary<string, DbIndex>();

    }

    public class DbColumn
    {
        public string DisplayName { get; set; }
        public string Type { get; set; }
        public int? Length { get; set; }
        public bool? Pk { get; set; }
        public bool? Id { get; set; }
        public bool? NotNull { get; set; }
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
        public bool? Unique { get; set; }
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
        public bool HasDiff => AddedTables.Any() || DeletedTableNames.Any() || ModifiedTables.Any(); 
        
        public Dictionary<string, DbTable> AddedTables { get; set; } = new Dictionary<string, DbTable>();
        public List<string> DeletedTableNames { get; set; } = new List<string>();
        public Dictionary<string, ModifiedTable> ModifiedTables { get; set; } = new Dictionary<string, ModifiedTable>();
        public Dictionary<string, DbTable> CurrentTables { get; set; } = new Dictionary<string, DbTable>();
        public Dictionary<string, DbTable> NewTables { get; set; } = new Dictionary<string, DbTable>();
    }

    public class ModifiedTable
    {
        public Dictionary<string, DbColumn> AddedColumns { get; set; } = new Dictionary<string, DbColumn>(); 
        public Dictionary<string, DbColumn[]> ModifiedColumns { get; set; } = new Dictionary<string, DbColumn[]>();
        public List<string> DeletedColumnName { get; set; } = new List<string>();
        public Dictionary<string, DbIndex> AddedIndices { get; set; } = new Dictionary<string, DbIndex>();
        public Dictionary<string, DbIndex[]> ModifiedIndices { get; set; } = new Dictionary<string, DbIndex[]>();
        public List<string> DeletedIndexNames { get; set; } = new List<string>();
    }
}