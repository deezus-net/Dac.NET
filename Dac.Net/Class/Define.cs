using System.Collections.Generic;

namespace Dac.Net.Class
{
    public class Define
    {
        public static class Command
        {
            public static readonly string[] Diff = {"diff"};
            public static readonly string[] Create = {"create"};
            public static readonly string[] ReCreate = {"recreate"};
            public static readonly string[] Update = {"update"};
            public static readonly string[] Delete = {"delete"};
        }

        public static class DbType
        {
            public const string MySql = "mysql";
            public const string PgSql = "pgsql";
            public const string MsSql = "mssql";
        }

        public static class ColumnType
        {
            public static readonly Dictionary<string, string> PgSql = new Dictionary<string, string>()
            {
                {"integer", "int"},
                {"character varying", "varchar"},
                {"serial", "int"},
                {"timestamp without time zone", "timestamp"},
            };

            public static readonly Dictionary<string, string> Mysql = new Dictionary<string, string>();
            public static readonly Dictionary<string, string> MsSql = new Dictionary<string, string>();
        }
    }
}