using Xunit;

namespace Dac.Net.Test.Db
{
    public class MsSqlTest
    {
        [Fact]
        public async void ConnectTest()
        {
            var ms = new Net.Db.MsSql()
            {
                DataSource = "localhost",
                UserId = "sa",
                Password = "!Passw0rd",
                InitialCatalog = "master"
            };
            await ms.Connect();
        }
    }
}