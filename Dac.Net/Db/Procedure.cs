using System.Collections.Generic;
using System.Linq;
using Dac.Net.Core;

namespace Dac.Net.Db
{
    public class Procedure
    {
        public Dictionary<string, string> Inputs { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Output { get; set; } = new Dictionary<string, string>();
        public string Content { get; set; }

        public bool Equals(Procedure target)
        {
            return Inputs.SequenceEqual(target.Inputs) && Output.SequenceEqual(target.Output) &&
                   Content == target.Content;
        }
    }
}