using System;
using Molder.Db;
using Molder.Core;

namespace Molder
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var core = new Main(args) {OutPut = new ResultOutput(new ConsoleOutput())};
                core.Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}