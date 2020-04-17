using System;
using Dac.Net.Core;
using Dac.Net.Db;

namespace Dac.Net
{
    class Program
    {
        static void Main(string[] args)
        {
            var core = new Main(args) {OutPut = Console.WriteLine};
            core.Run();
        }
    }
}