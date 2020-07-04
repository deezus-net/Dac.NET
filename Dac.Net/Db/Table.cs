using System.Collections.Generic;
using YamlDotNet.Serialization;

namespace Dac.Net.Db
{
    public class Table
    {
        [YamlIgnore]
        public string Name { get; set; }
        public Dictionary<string, Column> Columns { get; set; } = new Dictionary<string, Column>();
        public Dictionary<string, Index> Indexes { get; set; } = new Dictionary<string, Index>();
        
        public string TableId { get; set; }

    }
}