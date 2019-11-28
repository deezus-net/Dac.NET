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
                        db = new Db.PgSql()
                        {
                            Host = dbHost.Host,
                            Database = dbHost.Database,
                            Username = dbHost.User,
                            Password = dbHost.Password,
                            Port = dbHost.Port ?? 0
                        };
   
                        
                        break;
                    case Define.DbType.MsSql:
                        db = new Db.MsSql();
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
        private Dictionary<string, DbTable> ParseInputYaml()
        {
            var yaml = File.ReadAllText(_args.Input);
            var deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
            return deserializer.Deserialize<Dictionary<string, DbTable>>(yaml);
        }


        private async void Extract(IDb db)
        {
            var tables = await db.Extract();
        }

        private async void Create(IDb db)
        {
            var tables = ParseInputYaml();
            await db.Create(tables, _args.Query);
        }

        /// <summary>
        /// 
        /// </summary>
        private void ReCreate(IDb db)
        {
            var tables = ParseInputYaml();
            db.ReCreate(tables, _args.Query);
        }

        private void Update()
        {
            
        }

        private void Diff()
        {
            
        }

        private void Delete()
        {
            
        }

    }
}