using System;
using System.IO;
using Molder.Core;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Core
{
    public class MainTest
    {
        private readonly ITestOutputHelper _output;
        private readonly ResultOutput _resultOutput;
        public MainTest(ITestOutputHelper output)
        {
            _output = output;
            _resultOutput = new ResultOutput(new Output(_output));
        }
/*
        [Fact]
        public void ArgTest()
        {
            try
            {
                var main = new Main("create", "-i", "TestData/servers.yml")
                    {OutPut = _output.WriteLine};
                main.Run();
            }
            catch (Exception e)
            {
                _output.WriteLine(e.Message);
            }
        }

        [Fact]
        public void ExtractTest()
        {
            var main = new Main("extract", "-f", "TestData/servers.yml", "-o", "TestData")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void CreateTest()
        {
            var main = new Main("create", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-h", "mssql", "--dry-run")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void ReCreateTest()
        {
            var main = new Main("recreate", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-h", "mssql", "--dry-run")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void UpdateTest()
        {
            var main = new Main("update", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-h", "mssql", "-q")
                {OutPut = _output.WriteLine};
            main.Run();
        }
        
        [Fact]
        public void DiffTest()
        {
            var main = new Main("diff", "-f", "TestData/servers.yml", "-i", "TestData/mssql.yml", "-h", "mssql")
                {OutPut = _output.WriteLine};
            main.Run();
        }*/

        [Fact]
        public void HelpTest()
        {
            var main = new Main("help")
                {OutPut = _resultOutput };
            main.Run();
        }
    }
}