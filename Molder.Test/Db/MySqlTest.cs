
using Molder.Core;
using Molder.Test.Core;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Db
{
    public class MySqlTest
    {
        private readonly ITestOutputHelper _output;
        public MySqlTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void ConnectTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var sql = new Molder.Db.MySql(server, false);
            var res = sql.Connect();
            Assert.True(res);
        }
        
        [Fact]
        public void DropTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");
            Utility.TrimDataBaseProperties(db);
            
            var sql = new Molder.Db.MySql(server, false);
            sql.Connect();
            var result = sql.Drop(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        [Fact]
        public void CreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");

            var sql = new Molder.Db.MySql(server, false);
            
            sql.Connect();
            sql.Drop(db, false);
            var result = sql.Create(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        [Fact]
        public void ExtractTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");
            var sql = new Molder.Db.MySql(server, false);
            var res = sql.Connect();
            sql.ReCreate(db, false);
            var extract = sql.Extract();
            var yaml = Utility.DataBaseToYaml(extract);
            _output.WriteLine(yaml);
            Assert.NotEmpty(db.Tables);
        }

        [Fact]
        public void ReCreateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");

            var sql = new Molder.Db.MySql(server, false);
            sql.Connect();
            var result = sql.ReCreate(db, false);
            _output.WriteLine(result.Query);
            Assert.True(result.Success);
        }
        
        [Fact]
        public void DiffTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");
            
                
            var sql = new Molder.Db.MySql(server, false);
            var res = sql.Connect();
            sql.ReCreate(db, false);
            Utility.TrimDataBaseProperties(db);
            var diff = sql.Diff(db);
            Assert.False(diff.HasDiff);
        }
        
        [Fact]
        public void UpdateTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");
            var sql = new Molder.Db.MySql(server, false);
            sql.Connect();
            sql.ReCreate(db, false);
            
            Utility.TrimDataBaseProperties(db);
            
            
            var result = sql.Update(db, false, false);
            _output.WriteLine(result.Query ?? "");
            Assert.True(result.Success);
        }
        
     
        
        [Fact]
        public void QueryTest()
        {
            var server = Utility.LoadServers("TestData/servers.yml")["mysql"];
            var db = Utility.LoadDataBase("TestData/mysql.yml");
            
            var sql = new Molder.Db.MySql(server, false);
            var res = sql.Connect();
            var query = sql.Query(db);
            _output.WriteLine(query);
            Assert.False(string.IsNullOrWhiteSpace(query));
        }
    }
}