
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
            var sql = new MsSql(server, false);
            var res = sql.Connect();
            Assert.True(res);
        }
        
        [Fact]
        public void DropTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");
            Utility.TrimDataBaseProperties(db);
            
            var sql = new MsSql(server, false);
            sql.Connect();
            var result = sql.Drop(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        [Fact]
        public void CreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");

            var sql = new MsSql(server, false);
            var res = sql.Connect();
            sql.Drop(db, false);
            var result = sql.Create(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        
       [Fact]
       public void ExtractTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");
            
            var sql = new MsSql(server, false);
            var res = sql.Connect();
            sql.ReCreate(db, false);
            Utility.TrimDataBaseProperties(db);
            
            var extract = sql.Extract();
            var yaml = Utility.DataBaseToYaml(extract);
            _output.WriteLine(yaml);
            Assert.NotEmpty(extract.Tables);
        }

        [Fact]
        public void ReCreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");

            var sql = new MsSql(server, false);
            sql.Connect();
            var result = sql.ReCreate(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        [Fact]
        public void DiffTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");
  
            var sql = new MsSql(server, false);
            var res = sql.Connect();
         //   sql.ReCreate(db, false);
            Utility.TrimDataBaseProperties(db);
            var diff = sql.Diff(db);
            Assert.False(diff.HasDiff);
        }
        
        [Fact]
        public void UpdateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");
            Utility.TrimDataBaseProperties(db);
            
            var sql = new MsSql(server, false);
            sql.Connect();
            var result = sql.Update(db, true, true);
            _output.WriteLine(result.Query ?? "");
            Assert.True(result.Success);
        }
        
       
        
        [Fact]
        public void QueryTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mssql"];
            var db = Utility.LoadDataBase("TestData/mssql.yml");
            
            var sql = new MsSql(server, false);
            var res = sql.Connect();
            var query = sql.Query(db);
            _output.WriteLine(query);
            Assert.False(string.IsNullOrWhiteSpace(query));
        }
    }
}