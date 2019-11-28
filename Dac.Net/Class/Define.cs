using System.Collections.Generic;

namespace Dac.Net.Class
{
    public class Define
    {
        public static class Command
        {
            public const string Diff = "diff";
            public const string Extract = "extract";
            public const string Create = "create";
            public const string ReCreate = "recreate";
            public const string Update = "update";
            public const string Delete = "delete";
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