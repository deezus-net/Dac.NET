using System.Collections.Generic;

namespace Molder.Db
{
    public class Table
    {
        public Dictionary<string, Column> Columns { get; set; } = new Dictionary<string, Column>();
        public Dictionary<string, Index> Indexes { get; set; } = new Dictionary<string, Index>();
    }
}