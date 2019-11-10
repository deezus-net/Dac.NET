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
    }
}