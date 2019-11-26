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

        public Task<string> Drop(Dictionary<string, DbTable> tables, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<Dictionary<string, DbTable>> Extract()
        {
            throw new System.NotImplementedException();
        }

        public string Query(Dictionary<string, DbTable> tables)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> Create(Dictionary<string, DbTable> tables, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> ReCreate(Dictionary<string, DbTable> tables, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public Task<string> Update(Dictionary<string, DbTable> tables, bool queryOnly, bool dropTable)
        {
            throw new System.NotImplementedException();
        }

        public Task<DbDiff> Diff(Dictionary<string, DbTable> tables)
        {
            throw new System.NotImplementedException();
        }
    }
}