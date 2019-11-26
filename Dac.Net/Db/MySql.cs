using System.Collections.Generic;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Dac.Net.Db
{
    public class MySql : IDb
    {
        private MySqlConnection _connection;
        
        public string Server { get; set; }
        public uint Port { get; set; } = 3306;
        public string Database { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
        
        public async Task Connect()
        {
            var sb = new MySqlConnectionStringBuilder
            {
                Server = Server,
                Port = Port,
                UserID = UserId,
                Password = Password,
                Database = Database
            };

            _connection = new MySqlConnection(sb.ToString());
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