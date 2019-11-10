using System;
using CommandLine;

namespace Dac.Net.Class
{
    public class AppArg
    {
        [Option('c', "command", HelpText = "command", Required = false)]
        public string Command { get; set; }
        
        [Option('H', "hosts", HelpText = "Hosts file path.", Required = false)]
        public string Hosts { get; set; }
                
        [Option('h', "host", HelpText = "Database server / DataBase name when use hosts file. (required if not use hosts)", Required = false)]
        public string Host { get; set; }
        
        [Option('t', "type", HelpText = "database type. (required if not use hosts)", Required = false)]
        public string Type { get; set; }
        
        [Option('u', "user", HelpText = "Database user. (required if not use hosts)", Required = false)]
        public string User { get; set; }
        
        [Option('P', "password", HelpText = "Database password. (required if not use hosts)", Required = false)]
        public string Password { get; set; }
        
        [Option('p', "port", HelpText = "Database port. (required if not use hosts)", Required = false)]
        public int Port { get; set; }
        
        [Option('d', "database", HelpText = "Database name. (required if not use hosts)", Required = false)]
        public string Database { get; set; }
        
        [Option('i', "input", HelpText = "Yaml path.", Required = false)]
        public string Input { get; set; }

        [Option('q', "query", HelpText = "Create Query.", Required = false)]
        public bool Query { get; set; }
        
        [Option('D', "drop", HelpText = "Dropping tables not include in yaml.", Required = false)]
        public bool DropTable { get; set; }
        
        [Option('o', "output", HelpText = "Output filename when trim / Output directory when extracting, querying.", Required = false)]
        public string Output { get; set; }
    }
}