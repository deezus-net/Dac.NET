using Molder.Core;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Core
{
    public class UtilityTest
    {
        private readonly ITestOutputHelper _output;
        public UtilityTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void LoadServersTest()
        {
            var servers = Utility.LoadServers("TestData/servers.yml");
            Assert.NotEmpty(servers);
        }
        
        [Fact]
        public void LoadDataBaseTest()
        {
            var db = Utility.LoadDataBase("TestData/mssql.yml");
            Assert.NotNull(db);
        }
        
        [Fact]
        public void TrimQueryTest()
        {
            var result = Utility.TrimQuery("t   e  st");
            Assert.Equal( "t e st", result);
        }
    }
}