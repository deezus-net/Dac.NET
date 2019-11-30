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