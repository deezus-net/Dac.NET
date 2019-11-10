using Xunit;

namespace Dac.Net.Test.Db
{
    public class PsSqlTest
    {
        [Fact]
        public async void ConnectTest()
        {
            var pg = new Net.Db.PgSql()
            {
                Host = "localhost",
                Username = "dac",
                Password = "dac",
                Database = "dac"
            };
            await pg.Connect();
        }
    }
}