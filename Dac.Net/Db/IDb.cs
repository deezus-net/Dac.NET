using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dac.Net.Db
{
    public interface IDb
    {
        Task<string> Drop(Dictionary<string, DbTable> tables, bool queryOnly);
        Task<Dictionary<string, DbTable>> Extract();
        string Query(Dictionary<string, DbTable> tables);
        Task<string> Create(Dictionary<string, DbTable> tables, bool queryOnly);
        Task<string> ReCreate(Dictionary<string, DbTable> tables, bool queryOnly);
        Task<string> Update(Dictionary<string, DbTable> tables, bool queryOnly, bool dropTable);
        Task<DbDiff> Diff(Dictionary<string, DbTable> tables);
        
    }
}