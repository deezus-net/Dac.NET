using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Dac.Net.Db
{
    public class MsSql : IDb
    {
        private SqlConnection _connection;
        
        public string UserId { get; set; }
        public string Password { get; set; }
        public string InitialCatalog { get; set; }
        public string DataSource { get; set; }
        
        public async Task Connect()
        {
            var sb = new SqlConnectionStringBuilder
            {
                UserID = UserId,
                Password = Password,
                InitialCatalog = InitialCatalog,
                DataSource = DataSource
            };
            _connection = new SqlConnection(sb.ToString());
            await _connection.OpenAsync();
        }


        public Task<string> Drop(Db db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<Db> Extract()
        {
            throw new System.NotImplementedException();
        }

        public string Query(Db db)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> Create(Db db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> ReCreate(Db db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> Update(Db db, bool queryOnly, bool dropTable)
        {
            throw new System.NotImplementedException();
        }

        public Task<DbDiff> Diff(Db db)
        {
            throw new System.NotImplementedException();
        }
    }
}