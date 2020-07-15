using System;
using Molder.Core;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Core
{
    public class ResultOutputTest
    {
        private readonly ITestOutputHelper _output;
        private readonly ResultOutput _resultOutput;

        public ResultOutputTest(ITestOutputHelper output)
        {
            _output = output;
            _resultOutput = new ResultOutput(new Output(_output));
        }

        [Fact]
        public void WriteLineTest()
        {
            _resultOutput.WriteLine("tests");
        }

        [Fact]
        public void ErrorTest()
        {
            _resultOutput.Error("error");
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

        public void SetColor(string color)
        {
            throw new System.NotImplementedException();
        }

        public void ClearColor()
        {
            throw new System.NotImplementedException();
        }
    }
}