using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Dac.Net.Db
{
    public class PgSql : IDb
    {
        private NpgsqlConnection _connection;
        public string Host { get; set; }
        public int Port { get; set; } = 5432;
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }

        public async Task Connect()
        {
            var sb = new NpgsqlConnectionStringBuilder
            {
                Host = Host,
                Port = Port,
                Username = Username,
                Password = Password,
                Database = Database
            };

            _connection = new NpgsqlConnection(sb.ToString());
            await _connection.OpenAsync();
        }
    }
}