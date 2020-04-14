using System.Collections.Generic;
using System.Linq;

namespace Dac.Net.Db
{
    public class Index
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public Dictionary<string, string> Columns = new Dictionary<string, string>();
        public bool? Unique { get; set; }

        public bool Equals(Index target)
        {
            var col1 = string.Join("__", Columns.Select(x => $"{x.Key},{x.Value}"));
            var col2 = string.Join("__", target.Columns.Select(x => $"{x.Key},{x.Value}"));
            return Unique == target.Unique && Type == target.Type && col1 == col2;
        }

    }
}