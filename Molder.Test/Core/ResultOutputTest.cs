using System;
using System.Collections.Generic;
using Molder.Core;
using Molder.Db;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Core
{
    public class ResultOutputTest
    {
        private readonly ITestOutputHelper _output;
        private readonly ResultOutput _resultOutput;
        private readonly ResultOutput _resultOutput2;

        public ResultOutputTest(ITestOutputHelper output)
        {
            _output = output;
            _resultOutput = new ResultOutput(new Output(_output));
            _resultOutput2 = new ResultOutput(new ConsoleOutput());
        }

        [Fact]
        public void WriteLineTest()
        {
            _resultOutput.WriteLine("tests");
            _resultOutput2.WriteLine("tests");
        }

        [Fact]
        public void ErrorTest()
        {
            _resultOutput.Error("error");
            _resultOutput2.Error("error");
        }

        [Fact]
        public void CreateTest()
        {
            var result = new QueryResult()
            {
                Success = true
            };
            var commandLine = new CommandLine(new[]{ "" });
            
            _resultOutput.Create(result, commandLine, "test");
            _resultOutput2.Create(result, commandLine, "test");
        }
        
        [Fact]
        public void DiffTest()
        {
            var diff = new Diff()
            {
                ModifiedTables = new Dictionary<string, ModifiedTable>()
                {
                    {"test", new ModifiedTable()
                    {
                        AddedColumns = new Dictionary<string, Column>()
                        {
                            {"name", new Column()
                            {
                                Type = "varchar",
                                Length = "100"
                            }}
                        }
                    }}
                },
                DeletedTableNames = new List<string>()
                {
                    "deleted_table"
                }
            };
            
            _resultOutput.Diff(diff, "test");
            _resultOutput2.Diff(diff, "test");
        }
    }

    public class Output : IOutput
    {
        private readonly ITestOutputHelper _output;

        public Output(ITestOutputHelper output)
        {
            _output = output;
        }

        public void WriteLine(string message)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(message);
            _output.WriteLine(message);
        }

        public void Write(string message)
        {
            _output.WriteLine(message);
        }

        public void SetColor(ConsoleColor color)
        {
            
        }

        public void ClearColor()
        {
            
        }
    }
}