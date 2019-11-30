using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dac.Net.Db
{
    public interface IDb
    {
        Task<string> Drop(Db db, bool queryOnly);
        Task<Db> Extract();
        string Query(Db db);
        Task<string> Create(Db db, bool queryOnly);
        Task<string> ReCreate(Db db, bool queryOnly);
        Task<string> Update(Db db, bool queryOnly, bool dropTable);
        Task<DbDiff> Diff(Db db);
        
    }
}