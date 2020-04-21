using System.IO;
using Dac.Net.Core;
using Xunit;
using Xunit.Abstractions;

namespace Dac.Net.Test.Core
{
    public class MainTest
    {
        private readonly ITestOutputHelper _output;
        public MainTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void ExtractTest()
        {
            var main = new Main("extract", "-f", "TestData/servers.yml", "-o", "TestData/extract")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void CreateTest()
        {
            var main = new Main("create", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-q")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void ReCreateTest()
        {
            var main = new Main("recreate", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-q")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void UpdateTest()
        {
            var main = new Main("update", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-H", "mssql", "-q")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void DiffTest()
        {
            var main = new Main("diff", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-H", "mssql")
                {OutPut = _output.WriteLine};
            main.Run();
        }
    }
}