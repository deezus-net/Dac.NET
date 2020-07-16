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

        public void SetColor(ConsoleColor color)
        {
            Console.ForegroundColor = color;
        }

        public void ClearColor()
        {
            Console.ResetColor();
        }

    }
}