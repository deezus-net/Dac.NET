using Xunit;

namespace Dac.Net.Test.Db
{
    public class MySqlTest
    {
        [Fact]
        public async void ConnectTest()
        {
            var my = new Net.Db.MySql()
            {
                Server = "localhost",
                UserId = "dac",
                Password = "dac",
                Database = "dac"
            };
            await my.Connect();
        }
    }
}