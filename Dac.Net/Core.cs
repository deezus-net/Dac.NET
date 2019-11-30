using System;
using System.Collections.Generic;
using System.IO;
using Dac.Net.Class;
using Dac.Net.Db;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Dac.Net
{
    public class Core
    {
        private IDb _db;
        private readonly AppArg _args;
        public static Action<string> Output { get; set; }
        
        public Core(AppArg args)
        {
            _args = args;
        }

        
        
        public void Execute()
        {
            var hosts = new Dictionary<string, DbHost>();
            if (!string.IsNullOrWhiteSpace(_args.Input))
            {
                hosts = ParseHostsYaml();
            }
            else
            {
                hosts.Add(_args.Type, new DbHost()
                {
                    Host = _args.Host,
                    Type = _args.Type,
                    User = _args.User,
                    Password = _args.Password,
                    Port = _args.Port,
                    Database = _args.Database
                });
            }

            foreach (var (name, dbHost) in hosts)
            {
                IDb db = null;
                switch (dbHost.Type)
                {
                    case Define.DbType.MySql:
                        db = new Db.MySql();
                        break;
                    case Define.DbType.PgSql:
                        db = new PgSql()
                        {
                            Host = dbHost.Host,
                            Database = dbHost.Database,
                            Username = dbHost.User,
                            Password = dbHost.Password,
                            Port = dbHost.Port ?? 0
                        };
   
                        
                        break;
                    case Define.DbType.MsSql:
                        db = new MsSql();
                        break;
                }

                if (db == null)
                {
                    continue;
                }

                switch (_args.Command)
                {
                    case Define.Command.Extract:
                        Extract(db);
                        break;
                    case Define.Command.Create:
                        Create(db);
                        break;
                    case Define.Command.ReCreate:
                        ReCreate(db);
                        break;
                    case Define.Command.Update:
                        Update(db);
                        break;
                    case Define.Command.Diff:
                        Diff(db);
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, DbHost> ParseHostsYaml()
        {
            var yaml = File.ReadAllText(_args.Hosts);
            var deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
            return deserializer.Deserialize<Dictionary<string, DbHost>>(yaml);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private Db.Db ParseInputYaml()
        {
            var yaml = File.ReadAllText(_args.Input);
            var deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
            return deserializer.Deserialize<Db.Db>(yaml);
        }


        private async void Extract(IDb db)
        {
            var tables = await db.Extract();
        }

        private async void Create(IDb db)
        {
            try
            {
                await db.Create(ParseInputYaml(), _args.Query);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private async void ReCreate(IDb db)
        {
            var query = await db.ReCreate(ParseInputYaml(), _args.Query);
            Console.WriteLine(query);
            Output?.Invoke(query);
        }

        private async void Update(IDb db)
        {
            var query = await db.Update(ParseInputYaml(), _args.Query, _args.DropTable);
            Console.WriteLine(query);
            Output?.Invoke(query);
        }

        private async void Diff(IDb db)
        {
            var diff = await db.Diff(ParseInputYaml());
        }

        private void Delete()
        {
            
        }

    }
}