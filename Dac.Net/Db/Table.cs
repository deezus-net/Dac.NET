using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class Table
    {
        public Dictionary<string, Column> Columns { get; set; } = new Dictionary<string, Column>();
        public Dictionary<string, Index> Indices { get; set; } = new Dictionary<string, Index>();
    }
}