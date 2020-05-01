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
        public int LengthInt => int.TryParse(Length, out var length) ? length : int.MaxValue;

        public bool Equals(Column target)
        {
            // foreign key check
            var fk1 = (ForeignKeys ?? new Dictionary<string, ForeignKey>());
            var fk2 = (target.ForeignKeys ?? new Dictionary<string, ForeignKey>());

            var fkDiff = false;
            foreach (var fkName in fk1.Keys.Concat(fk2.Keys).Distinct())
            {
                if (!fk1.ContainsKey(fkName) || !fk2.ContainsKey(fkName))
                {
                    fkDiff = true;
                    break;
                }

                if ((fk1[fkName].Update != fk2[fkName].Update) ||
                    (fk1[fkName].Delete != fk2[fkName].Delete) ||
                    (fk1[fkName].Table != fk2[fkName].Table) ||
                    (fk1[fkName].Column != fk2[fkName].Column))
                {
                    fkDiff = true;
                    break;
                }
            }

            return Type?.ToLower() == target.Type?.ToLower() &&
                   Length?.ToLower() == target.Length?.ToLower() &&
                   NotNull == target.NotNull &&
                   Id == target.Id &&
                   Default?.ToLower() == target.Default?.ToLower() &&
                   Check == target.Check &&
                   string.Join(",", fk1.Keys) == string.Join(",", fk2.Keys) &&
                   !fkDiff;



        }

    }
}