using System;

namespace Molder.Core
{
    public class ConsoleOutput : IOutput
    {
        public void Write(string message)
        {
            Console.Write(message);
        }
        
        public void WriteLine(string message)
        {
            Console.WriteLine(message);
        }
        public void SetColor(string color)
        {
            throw new NotImplementedException();
        }

        public void ClearColor()
        {
            throw new NotImplementedException();
        }

       
    }
}