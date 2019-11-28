using Dac.Net.Class;
using Xunit;

namespace Dac.Net.Test
{
    public class CoreTest
    {
        private const string TestDataDir = "/Users/you/git/Dac.Net/Dac.Net.Test/.TestData/";
        
        [Fact]
        public void ExtractTest()
        {
            var args = new AppArg
            {
                Input = $"{TestDataDir}postgres/hosts.yml",
                Command = "extract"
            };
            var core = new Core(args);
            core.Execute();
        }
        
        [Fact]
        public void CreateTest()
        {
            var args = new AppArg
            {
                Hosts = $"{TestDataDir}postgres/hosts.yml",
                Input = $"{TestDataDir}postgres/db.yml",
                Command = "create"
            };
            var core = new Core(args);
            core.Execute();
        }
        
        [Fact]
        public void ReCreateTest()
        {
            var args = new AppArg
            {
                Hosts = $"{TestDataDir}postgres/hosts.yml",
                Input = $"{TestDataDir}postgres/db.yml",
                Command = "recreate"
            };
            var core = new Core(args);
            core.Execute();
        }
    }
}