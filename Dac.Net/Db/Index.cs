using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class Index
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public Dictionary<string, string> Columns = new Dictionary<string, string>();
        public bool? Unique { get; set; }
    }
}