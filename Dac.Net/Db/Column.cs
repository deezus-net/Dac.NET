using System.Collections.Generic;
using System.Linq;
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

        public bool Equals(Column target)
        {
            // foreign key check
            var fkName1 = ForeignKeys.Keys;
            var fkName2 = target.ForeignKeys.Keys;

            var fkDiff = false;
            foreach (var fkName in fkName1.Concat(fkName2).Distinct())
            {
                if (!fkName1.Contains(fkName) || !fkName2.Contains(fkName))
                {
                    fkDiff = true;
                    break;
                }

                if ((ForeignKeys[fkName].Update != target.ForeignKeys[fkName].Update) ||
                    (ForeignKeys[fkName].Delete != target.ForeignKeys[fkName].Delete) ||
                    (ForeignKeys[fkName].Table != target.ForeignKeys[fkName].Table) ||
                    (ForeignKeys[fkName].Column != target.ForeignKeys[fkName].Column))
                {
                    fkDiff = true;
                    break;
                }
            }

            return Type == target.Type &&
                   Length == target.Length &&
                   NotNull == target.NotNull &&
                   Id == target.Id &&
                   Default == target.Default &&
                   string.Join(",", fkName1) == string.Join(",", fkName2) &&
                   !fkDiff;
        }

    }
}