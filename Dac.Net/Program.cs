using System;
using Dac.Net.Core;
using Dac.Net.Db;

namespace Dac.Net
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var core = new Main(args) {OutPut = Console.WriteLine};
                core.Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}