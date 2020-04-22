using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Dac.Net.Db;

namespace Dac.Net.Core
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
                    case "-f":
                    case "--hosts":
                        HostsFile = args[i + 1];
                        servers = Utility.LoadServers(HostsFile);
                        i++;
                        break;
                    case "-h":
                    case "--host":
                        Host = args[i + 1];
                        server.Host = Host;
                        i++;
                        break;
                    case "-t":
                    case "--type":
                        Type = args[i + 1];
                        server.Type = Type;
                        i++;
                        break;
                    case "-u":
                    case "--user":
                        User = args[i + 1];
                        server.User = User;
                        i++;
                        break;
                    case "-p":
                    case "--password":
                        Password = args[i + 1];
                        server.Password = Password;
                        i++;
                        break;
                    case "-P":
                    case "--port":
                        Port = args[i + 1];
                        if (int.TryParse(args[i + 1], out var port))
                        {
                            server.Port = port;
                        }

                        i++;
                        break;
                    case "-d":
                    case "--database":
                        Database = args[i + 1];
                        server.Database = Database;
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
                    case "--dry-run":
                        DryRun = true;
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
                Define.Command.Trim
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

            if (new[] {Define.Command.Trim}.Contains(Command) &&
                (string.IsNullOrWhiteSpace(InputFile) || string.IsNullOrWhiteSpace(OutputFile)))
            {
                ErrorMessage = "input, output are required";
                return false;
            }

            return true;
        }

        private static string Help()
        {
            var ver = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            
            var help = new StringBuilder();
            help.AppendLine($"{ver.ProductName} ver.{ver.FileVersion}");
            help.AppendLine("usage [command] [options]");
            help.AppendLine("commands");
            help.AppendLine($"  {Define.Command.Create}");
            help.AppendLine($"  {Define.Command.Diff}");
            help.AppendLine($"  {Define.Command.Extract}");
            help.AppendLine($"  {Define.Command.Query}");
            help.AppendLine($"  {Define.Command.Trim}");
            help.AppendLine($"  {Define.Command.Update}");
            help.AppendLine($"  {Define.Command.ReCreate}");
            help.AppendLine("");
            help.AppendLine("options");
            help.AppendLine("  -f, --hosts <filepath>       Hosts file path.");
            help.AppendLine("  -H, --host <host>            Database server / DataBase name when use hosts file. (required if not use hosts)");
            help.AppendLine("  -t, --type <type>            database type. (required if not use hosts)");
            help.AppendLine("  -u, --user <user>            Database user. (required if not use hosts)");
            help.AppendLine("  -p, --password <password>    Database password. (required if not use hosts)");
            help.AppendLine("  -P, --port <port number>     Database port.");
            help.AppendLine("  -d, --database <database>    Database name. (required if not use hosts)");
            help.AppendLine("  -i, --input <input-filepath> Yaml path.");
            help.AppendLine("  -q, --query                  Create Query.");
            help.AppendLine("  -D, --drop                   Dropping tables not include in yaml.");
            help.AppendLine("  -o, --output <output>        Output filename when trim / Output directory when extracting, querying.");
            help.AppendLine("");
            help.AppendLine("ex1");
            help.AppendLine("  dac extract -H localhost -t mysql -u root -P password -d sample -o db.yml");
            help.AppendLine("ex2");
            help.AppendLine("  dac extract -f servers.yml -o .");
            help.AppendLine("");
            help.AppendLine("https://github.com/deezus-net/Dac.Net");
            return help.ToString();
        }
    }
}