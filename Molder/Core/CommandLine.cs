using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Molder.Db;

namespace Molder.Core
{
    public class CommandLine
    {
        public string Command { get; private set; }
        public string HostsFile { get; private set; }
        
        public string Host { get; private set; }
        
        public string User { get; private set; }
        
        public string Password { get; private set; }
        
        public string Database { get; private set; }
        
        public string Port { get; private set; }
        
        public string Type { get; private set; }
        public string InputFile { get; private set; }
        public bool Query { get; private set; }
        public bool Drop { get; private set; }
        
        public bool DryRun { get; set; }
        public string OutputFile { get; private set; }

        public List<Server> Servers { get; private set; } = new List<Server>();
        
        public DataBase DataBase { get; private set; }
        
        public string ErrorMessage { get; private set; }

        public CommandLine(string[] args)
        {
            Parse(args);
        }

        public void Parse(string[] args)
        {
            ErrorMessage = "";
            if (args.Length == 0)
            {
                ErrorMessage = Help();
                return;
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
                    case "--hosts":
                        HostsFile = args[i + 1];
                        servers = Utility.LoadServers(HostsFile);
                        foreach (var (name, s) in servers)
                        {
                            s.Name = name;
                        }
                        i++;
                        break;
                    case "--host":
                        Host = args[i + 1];
                        server.Host = Host;
                        server.Name = Host;
                        i++;
                        break;
                    case "--type":
                        Type = args[i + 1];
                        server.Type = Type;
                        i++;
                        break;
                    case "--user":
                        User = args[i + 1];
                        server.User = User;
                        i++;
                        break;
                    case "--password":
                        Password = args[i + 1];
                        server.Password = Password;
                        i++;
                        break;
                    case "--port":
                        Port = args[i + 1];
                        if (int.TryParse(args[i + 1], out var port))
                        {
                            server.Port = port;
                        }

                        i++;
                        break;
                    case "--database":
                        Database = args[i + 1];
                        server.Database = Database;
                        i++;
                        break;
                    case "--input":
                        InputFile = args[i + 1];
                        DataBase = Utility.LoadDataBase(InputFile);
                        Utility.TrimDataBaseProperties(DataBase);
                        i++;
                        break;
                    case "--query":
                        Query = true;
                        break;
                    case "--drop":
                        Drop = true;
                        break;
                    case "--output":
                        OutputFile = args[i + 1];
                        i++;
                        break;
                    case "--dry-run":
                        DryRun = true;
                        break;
                    case "--help" :
                        ErrorMessage = Help();
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

        public bool Check()
        {
            if (!string.IsNullOrWhiteSpace(ErrorMessage))
            {
                return false;
            }
            
            var commands = new[]
            {
                Define.Command.Create,
                Define.Command.Drop,
                Define.Command.Query,
                Define.Command.Extract,
                Define.Command.Create,
                Define.Command.ReCreate,
                Define.Command.Update,
                Define.Command.Diff,
            };
            if (!commands.Contains(Command))
            {
                ErrorMessage = Help();
                return false;
            }

            if (!string.IsNullOrWhiteSpace(HostsFile))
            {
                if (!File.Exists(HostsFile))
                {
                    ErrorMessage = $"{HostsFile} not found";
                    return false;
                }

            }
            else
            {
                if (string.IsNullOrEmpty(Type) || string.IsNullOrWhiteSpace(Host) || string.IsNullOrWhiteSpace(User) ||
                    string.IsNullOrWhiteSpace(Password) || string.IsNullOrWhiteSpace(Database))
                {
                    ErrorMessage = $"type, host, user, password, database are required";
                    return false;
                }
            }

            if (Command == Define.Command.Extract && string.IsNullOrWhiteSpace(OutputFile))
            {
                ErrorMessage = "output is required";
                return false;
            }

            if (new[]
            {
                Define.Command.Create, Define.Command.ReCreate, Define.Command.Update, Define.Command.Diff,
                Define.Command.Drop
            }.Contains(Command) && string.IsNullOrWhiteSpace(InputFile))
            {
                ErrorMessage = "input is required";
                return false;
            }

            return true;
        }

        private static string Help()
        {
            var assembly = Assembly.GetExecutingAssembly();
            
            var help = new StringBuilder();
            help.AppendLine($"{assembly.GetName().Name} ver.{assembly.GetName().Version}");
            help.AppendLine("usage [command] [options]");
            help.AppendLine("commands");
            help.AppendLine($"  {Define.Command.Create}");
            help.AppendLine($"  {Define.Command.Diff}");
            help.AppendLine($"  {Define.Command.Extract}");
            help.AppendLine($"  {Define.Command.Query}");
            help.AppendLine($"  {Define.Command.Update}");
            help.AppendLine($"  {Define.Command.ReCreate}");
            help.AppendLine("");
            help.AppendLine("options");
            help.AppendLine("  --hosts <filepath>       hosts file path.");
            help.AppendLine("  --host <host>             database server / DataBase name when use hosts file. (required if not use hosts)");
            help.AppendLine("  --type <type>             database type. (required if not use hosts)");
            help.AppendLine("  --user <user>             database user. (required if not use hosts)");
            help.AppendLine("  --password <password>     database password. (required if not use hosts)");
            help.AppendLine("  --port <port number>      database port.");
            help.AppendLine("  --database <database>     database name. (required if not use hosts)");
            help.AppendLine("  --input <input filepath>  yaml path.");
            help.AppendLine("  --query                   create Query.");
            help.AppendLine("  --drop                    dropping tables not include in yaml.");
            help.AppendLine("  --dry-run                 execute query, but not commit.");
            help.AppendLine("  --output <output dirpath> output directory when extracting, querying.");
            help.AppendLine("");
            help.AppendLine("  --help                    show help.");
            help.AppendLine("ex1");
            help.AppendLine("  molder extract --host localhost --type mysql --user root --password password --database sample --output db.yml");
            help.AppendLine("ex2");
            help.AppendLine("  molder extract --hosts servers.yml --output .");
            help.AppendLine("");
            help.AppendLine("https://github.com/deezus-net/Molder");
            return help.ToString();
        }
    }
}