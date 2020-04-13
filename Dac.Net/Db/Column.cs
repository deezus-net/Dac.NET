using System.Collections.Generic;
using YamlDotNet.Serialization;

namespace Dac.Net.Db
{
    public class Column
    {
        public string DisplayName { get; set; }
        public string Type { get; set; }
        public string Length { get; set; }
        public bool? Pk { get; set; }
        public bool? Id { get; set; }
        public bool? NotNull { get; set; }
        public string Check { get; set; }
        [YamlIgnore]
        public string CheckName { get; set; }
        public Dictionary<string, ForeignKey> ForeignKeys { get; set; } = new Dictionary<string, ForeignKey>();
        public string Default { get; set; }
        [YamlIgnore]
        public string DefaultName { get; set; }
        public string Comment { get; set; }

        [YamlIgnore]
        public int LengthInt
        {
            get
            {
                int.TryParse(Length, out var length);
                return length;
            }
        }
    }
}