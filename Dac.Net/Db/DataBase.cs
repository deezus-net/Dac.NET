using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class DataBase
    {
        public Dictionary<string, Table> Tables { get; set; } = new Dictionary<string, Table>();
        public Dictionary<string, Synonym> Synonyms { get; set; } = new Dictionary<string, Synonym>();
        
        public Dictionary<string, string> Views { get; set; } = new Dictionary<string, string>();
    }
}