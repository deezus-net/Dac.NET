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
        private AppArg _args;
        private Dictionary<string, DbHost> DbHosts;
        
        public Core(AppArg args)
        {
            _args = args;
        }

        
        
        public void Execute()
        {
            if (!string.IsNullOrWhiteSpace(_args.Input))
            {
                ParseInputYaml();
            }
            
            switch (_args.Type)
            {
                case Define.DbType.MySql:
                    _db = new Db.MySql();
                    break;
                case Define.DbType.PgSql:
                    _db = new Db.PgSql();
                    break;
                case Define.DbType.MsSql:
                    _db = new Db.MsSql();
                    break;
            }

            if (_db == null)
            {
                return;
            }

            
        }

        private void ParseInputYaml()
        {
            var yaml = File.ReadAllText(_args.Input);
            var deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
            DbHosts = deserializer.Deserialize<Dictionary<string, DbHost>>(yaml);
        }

        private void Extract()
        {
            
        }

        private void Create()
        {
            
        }

        private void ReCreate()
        {
            
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