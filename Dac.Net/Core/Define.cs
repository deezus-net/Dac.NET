using System.Collections.Generic;

namespace Dac.Net.Core
{
    public class Define
    {
        public class Command
        {
            public const string Drop = "drop";
            public const string Query = "query";
            public const string Extract = "extract";
            public const string Create = "create";
            public const string ReCreate = "recreate";
            public const string Update = "update";
            public const string Diff = "diff";
        }

        public class DatabaseType
        {
            public const string Mysql = "mysql";
            public const string Postgres = "pgsql";
            public const string MsSql = "mssql";
        }

        public class ColumnType
        {
            public static Dictionary<string, string> PgSql = new Dictionary<string, string>()
            {

                {"integer", "int"},
                {"character varying", "varchar"},
                {"serial", "int"},
                {"timestamp without time zone", "timestamp"}
            };

            public static Dictionary<string, string> MySql = new Dictionary<string, string>();

            public static Dictionary<string, string> MsSql = new Dictionary<string, string>();
        }
    }
}