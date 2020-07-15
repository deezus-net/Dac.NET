using Molder.Db;

namespace Molder.Core
{
    public interface IOutput
    {
        void WriteLine(string message);
        void Write(string message);
        void SetColor(string color);
        void ClearColor();
    }
}