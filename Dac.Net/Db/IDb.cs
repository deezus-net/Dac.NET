using System.Threading.Tasks;

namespace Dac.Net.Db
{
    public interface IDb
    {
        Task Connect();
    }
}