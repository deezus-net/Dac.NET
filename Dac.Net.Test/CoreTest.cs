using Dac.Net.Class;
using Xunit;
using Xunit.Abstractions;

namespace Dac.Net.Test
{
    public class CoreTest
    {
        private const string TestDataDir = "/Users/you/git/Dac.Net/Dac.Net.Test/.TestData/";

        public CoreTest(ITestOutputHelper output)
        {
            Core.Output = output.WriteLine;
        }
        
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
        
        [Fact]
        public void UpdateTest()
        {
            var args = new AppArg
            {
                Hosts = $"{TestDataDir}postgres/hosts.yml",
                Input = $"{TestDataDir}postgres/db.yml",
                Command = "update",
                Query = true
            };
            var core = new Core(args);
            core.Execute();
        }
        
        [Fact]
        public void DiffTest()
        {
            var args = new AppArg
            {
                Hosts = $"{TestDataDir}postgres/hosts.yml",
                Input = $"{TestDataDir}postgres/db.yml",
                Command = "diff",
                Query = true
            };
            var core = new Core(args);
            core.Execute();
        }
    }
}