using System;
using System.Threading.Tasks;
using CommandLine;
using Dac.Net.Class;

namespace Dac.Net
{
    class Program
    {
        static void Main(string[] args)
        {
            Parser.Default.ParseArguments<AppArg>(args).WithParsed(result =>
            {
                var core = new Core(result);
                core.Execute();
            }).WithNotParsed(result =>
            {
                var a = "";
            });
        }
    }
}
