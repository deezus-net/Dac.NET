using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class Procedure
    {
        public Dictionary<string, string> Inputs { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Output { get; set; } = new Dictionary<string, string>();
        public string Content { get; set; }
    }
}