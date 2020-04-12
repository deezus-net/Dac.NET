using System.Collections.Generic;

namespace Dac.Net.Db
{
    public class DataBase
    {
        public Dictionary<string, Table> Tables { get; set; } = new Dictionary<string, Table>();
    }
}