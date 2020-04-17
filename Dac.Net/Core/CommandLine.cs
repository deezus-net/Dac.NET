using System.Collections.Generic;
using System.Linq;
using Dac.Net.Db;

namespace Dac.Net.Core
{
    public class CommandLine
    {
        public string Command { get; private set; }
        public string HostsFile { get; private set; }
        public string InputFile { get; private set; }
        public bool Query { get; private set; }
        public bool Drop { get; private set; }
        public string OutputFile { get; private set; }
        public List<Server> Servers { get; private set; } = new List<Server>();
        
        public DataBase DataBase { get; private set; }

        public CommandLine(string[] args)
        {
            Parse(args);
        }

        public void Parse(string[] args)
        {
            if (args.Length == 0)
            {

            }
            else
            {
                Command = args[0];
            }

            var server = new Server();
            var servers = new Dictionary<string, Server>();
            
            for (var i = 1; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-f":
                    case "--hosts":
                        HostsFile = args[i + 1];
                        servers = Utility.LoadServers(HostsFile);
                        i++;
                        break;
                    case "-H":
                    case "--host":
                        server.Host = args[i + 1];
                        i++;
                        break;
                    case "-t":
                    case "--type":
                        server.Type = args[i + 1];
                        i++;
                        break;
                    case "-u":
                    case "--user":
                        server.User = args[i + 1];
                        i++;
                        break;
                    case "-p":
                    case "--password":
                        server.Password = args[i + 1];
                        i++;
                        break;
                    case "-P":
                    case "--port":
                        if (int.TryParse(args[i + 1], out var port))
                        {
                            server.Port = port;
                        }

                        i++;
                        break;
                    case "-d":
                    case "--database":
                        server.Database = args[i + 1];
                        i++;
                        break;
                    case "-i":
                    case "--input":
                        InputFile = args[i + 1];
                        DataBase = Utility.LoadDataBase(InputFile);
                        Utility.TrimDataBaseProperties(DataBase);
                        i++;
                        break;
                    case "-q":
                    case "--query":
                        Query = true;
                        break;
                    case "-D":
                    case "--drop":
                        Drop = true;
                        break;
                    case "-o":
                    case "--output":
                        OutputFile = args[i + 1];
                        i++;
                        break;
                }

            }



            if (servers.Any())
            {
                if (!string.IsNullOrWhiteSpace(server.Host))
                {
                    if (servers.ContainsKey(server.Host))
                    {
                        Servers.Add(servers[server.Host]);
                    }
                }
                else
                {
                    Servers = servers.Select(x =>
                    {
                        x.Value.Name = x.Key;
                        return x.Value;
                    }).ToList();
                }
            }
            else
            {
                if (server.IsValid)
                {
                    Servers.Add(server);
                }
            }

        }
    }
}