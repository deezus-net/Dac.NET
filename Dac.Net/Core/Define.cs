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
            public const string Trim = "trim";
        }

        public class DatabaseType
        {
            public const string Mysql = "mysql";
            public const string Postgres = "postgres";
            public const string MsSql = "mssql";
        }
    }
}