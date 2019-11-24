using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Dac.Net.Db
{
    public class PgSql : IDb
    {
        public string Host { get; set; }
        public int Port { get; set; } = 5432;
        public string Username { get; set; }
        public string Password { get; set; }
        public string Database { get; set; }

        public async Task<NpgsqlConnection> Connect()
        {
            var sb = new NpgsqlConnectionStringBuilder
            {
                Host = Host,
                Port = Port,
                Username = Username,
                Password = Password,
                Database = Database
            };

            var connection = new NpgsqlConnection(sb.ToString());
            await connection.OpenAsync();

            return connection;
        }

        public async Task<string> Create(Dictionary<string, DbTable> tables, bool queryOnly = false)
        {
            var query = CreateQuery(tables);
            if (queryOnly)
            {
                return query;
            }

            using (var con = await Connect())
            {
                using (var trn = con.BeginTransaction())
                {
                    using (var cmd = new NpgsqlCommand(query, trn.Connection, trn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    trn.Commit();
                }
            }

            return query;

        }

        private string CreateQuery(Dictionary<string, DbTable> tables)
        {

            var query = new List<string>();
            foreach (var (tableName, dbTable) in tables)
            {

                query.Add($"CREATE TABLE {tableName}(");

                var columnQuery = new List<string>();
                var pk = new List<string>();
                foreach (var (columnName, dbColumn) in dbTable.DbColumns)
                {
                    if (dbColumn.Id)
                    {
                        dbColumn.NotNull = true;
                        dbColumn.Type = "serial";
                    }

                    var notNull = dbColumn.NotNull ? " NOT NULL " : "";
                    var check = !string.IsNullOrWhiteSpace(dbColumn.Check) ? $" CHECK({dbColumn.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(dbColumn.Default) ? $" DEFAULT {dbColumn.Default} " : "";
                    var type = dbColumn.Type + (dbColumn.Length > 0 ? $"({dbColumn.Length})" : "");

                    columnQuery.Add($"    {columnName} {type}{notNull}{check}{def}");
                    if (dbColumn.Pk || dbColumn.Id)
                    {
                        pk.Add(columnName);
                    }
                }

                query.Add(string.Join(",\n", columnQuery) + (pk.Count > 0 ? "," : ""));

                if (pk.Count > 0)
                {
                    query.Add($"    CONSTRAINT PK_{tableName} PRIMARY KEY ");
                    query.Add("    (");
                    var pkQuery = new List<string>();
                    pk.ForEach(p => { pkQuery.Add($"        {p}"); });
                    query.Add(string.Join(",\n", pkQuery));
                    query.Add("    )");
                }

                query.Add(");");

                foreach (var (indexName, dbIndex) in dbTable.DbIndices)
                {
                    var indexColumns = new List<string>();
                    foreach (var (columnName, order) in dbIndex.Columns)
                    {
                        indexColumns.Add(columnName);
                    }

                    query.Add($"CREATE {(dbIndex.Unique ? "UNIQUE " : "")}INDEX {indexName} ON {tableName}(");
                    query.Add($"    {string.Join(",", indexColumns)}");
                    query.Add(");");
                }

            }

            // foregin key
            foreach (var (tableName, dbTable) in tables)
            {
                foreach (var (columnName, dbColumn) in dbTable.DbColumns.Where(x => x.Value.Fk != null))
                {
                    foreach (var (fkName, fkForeignKey) in dbColumn.Fk)
                    {
                        query.Add(CreateAlterForeignKey(fkName, tableName, columnName, fkForeignKey.Table,
                            fkForeignKey.Column, fkForeignKey.Update, fkForeignKey.Delete));
                    }
                }
            }

            return string.Join("\n", query);




        }


        private static string CreateAlterForeignKey(string name, string table, string column, string targetTable,
            string targetColumn, string onUpdate, string onDelete)
        {

            if (!string.IsNullOrWhiteSpace(onUpdate))
            {
                onUpdate = $" ON UPDATE {onUpdate} ";
            }

            if (!string.IsNullOrWhiteSpace(onDelete))
            {
                onDelete = $" ON DELETE {onDelete} ";
            }

            return $@"
            ALTER TABLE 
            ""{table}"" 
            ADD CONSTRAINT ""{name}"" FOREIGN KEY (""{column}"") REFERENCES ""{targetTable}""(""{targetColumn}""){onUpdate ?? ""}{onDelete ?? ""};";
        }
    }
}