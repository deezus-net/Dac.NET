using Dac.Net.Class;
using Xunit;

namespace Dac.Net.Test
{
    public class CoreTest
    {
        private const string TestDataDir = ".TestData/";
        
        [Fact]
        public void Test()
        {
            var args = new AppArg { Input = $"{TestDataDir}mysql/hosts.yml"};
            var core = new Core(args);
            core.Execute();
        }
    }
}