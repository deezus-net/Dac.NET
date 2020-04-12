namespace Dac.Net.Db
{
    public class Server
    {
        public string Type { get; set; }
        public string Host { get; set; }
        public int? Port { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }
        public string Name { get; set; }
    }
}