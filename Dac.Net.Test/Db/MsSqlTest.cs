
using Dac.Net.Core;
using Dac.Net.Db;
using Dac.Net.Test.Core;
using Xunit;
using Xunit.Abstractions;

namespace Dac.Net.Test.Db
{
    public class MsSqlTest
    {
        private readonly ITestOutputHelper _output;
        public MsSqlTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void ConnectTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var sql = new MsSql(server);
            var res = sql.Connect();
            Assert.True(res);
        }
        
        [Fact]
        public void CreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");

            var sql = new MsSql(server);
            var res = sql.Connect();
            var query = sql.Create(db, false);
            _output.WriteLine(query);
            Assert.False(string.IsNullOrWhiteSpace(query));
        }
        
        [Fact]
        public void ReCreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");

            var sql = new MsSql(server);
            var res = sql.Connect();
            var query = sql.ReCreate(db, false);
            _output.WriteLine(query);
            Assert.False(string.IsNullOrWhiteSpace(query));
        }
    }
}