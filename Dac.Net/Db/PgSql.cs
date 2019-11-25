using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Dac.Net.Class;
using Microsoft.VisualBasic;
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public async Task<string> Create(Dictionary<string, DbTable> tables, bool queryOnly = false)
        {
            var query = CreateQuery(tables);
            if (queryOnly)
            {
                return query;
            }

            await ExecQuery(query);
            return query;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public async Task<string> Drop(Dictionary<string, DbTable> tables, bool queryOnly = false)
        {
            var queries = new List<string>();
            foreach (var (tableName, dbTable) in tables)
            {
                queries.Add($"DROP TABLE IF EXISTS \"{tableName}\" CASCADE;");
            }

            var query = string.Join("\n", queries);

            if (queryOnly)
            {
                return query;
            }

            await ExecQuery(query);
            return query;
        }

        /*    public async diff(db: Db) {
                const orgDb = await this.extract();
                return checkDbDiff(orgDb, db);
            } */

        public async Task<Dictionary<string, DbTable>> Extract()
        {
            var tables = new Dictionary<string, DbTable>();
            using (var con = await Connect())
            {
                foreach (var row in GetResult(con,
                    "SELECT relname FROM \"pg_stat_user_tables\" WHERE schemaname=\'public\'"))
                {
                    tables.Add(Convert.ToString(row["relname"]), new DbTable());
                }

                // get sequence list
                var sequences = new List<string>();
                foreach (var row in GetResult(con, "SELECT sequence_name FROM information_schema.sequences"))
                {
                    sequences.Add(Convert.ToString(row["sequence_name"]));
                }





                foreach (var (tableName, dbTable) in tables)
                {

                    // get column list
                    var query = @"
                    SELECT 
                        column_name, 
                        data_type, 
                        is_nullable, 
                        character_maximum_length, 
                        is_identity, 
                        column_default 
                    FROM 
                        information_schema.columns 
                    WHERE 
                        table_name = @table_name";
                    foreach (var row in GetResult(con, query,
                        new Dictionary<string, object>() {{"table_name", tableName}}))
                    {
                        var columnDefault = Convert.ToString(row["column_default"]);
                        var id = sequences.Any(seq => columnDefault.Contains(seq));
                        var type = id ? "serial" : Convert.ToString(row["data_type"]);
                        int.TryParse(Convert.ToString(row["character_maximum_length"]), out var length);

                        type = Define.ColumnType.PgSql.ContainsKey(type) ? Define.ColumnType.PgSql[type] : type;

                        var column = new DbColumn()
                        {
                            Type = type,
                            Id = id,
                            Length = length,
                            NotNull = Convert.ToString(row["is_nullable"]) == "NO"
                        };
                        if (!string.IsNullOrWhiteSpace(columnDefault) && !id)
                        {
                            column.Default = columnDefault;
                        }

                        tables[tableName].DbColumns.Add(Convert.ToString(row["column_name"]), column);
                    }


                    // get primary key list
                    query = @"
                SELECT
                    ccu.column_name 
                FROM
                    information_schema.table_constraints tc
                INNER JOIN
                    information_schema.constraint_column_usage ccu
                ON
                    tc.table_catalog = ccu.table_catalog
                AND
                    tc.table_schema = ccu.table_schema
                AND
                    tc.table_name = ccu.table_name
                AND
                    tc.constraint_name = ccu.constraint_name
                WHERE
                    tc.table_catalog = @table_catalog
                AND
                    tc.table_name = @table_name
                AND
                    tc.constraint_type = 'PRIMARY KEY'";
                    foreach (var row in GetResult(con, query,
                        new Dictionary<string, object>() {{"table_catalog", Database}, {"table_name", tableName}}))

                    {
                        var columnName = Convert.ToString(row["column_name"]);
                        if (tables[tableName].DbColumns.ContainsKey(columnName))
                        {
                            tables[tableName].DbColumns[columnName].Pk = true;
                        }
                    }

                    // get index list
                    query = @"
                SELECT 
                    indexname, 
                    indexdef 
                FROM 
                    pg_indexes 
                WHERE tablename = @table_name";
                    foreach (var row in GetResult(con, query,
                        new Dictionary<string, object>() {{"table_name", tableName}}))
                    {
                        var indexdef = Convert.ToString(row["indexdef"]);
                        var indexName = Convert.ToString(row["indexname"]);
                        if (!tables[tableName].DbIndices.ContainsKey(indexName))
                        {
                            tables[tableName].DbIndices.Add(indexName, new DbIndex()
                            {
                                Unique = indexdef.Contains("UNIQUE INDEX"),
                                Columns = { }
                            });
                        }

                        var m = Regex.Match(indexdef, "\\(.*\\)");
                        if (!m.Success)
                        {
                            continue;
                        }

                        foreach (var col in m.Value.Replace("(", "").Replace(")", "").Split(','))
                        {
                            var tmp = col.Trim().Split(' ');
                            if (tables[tableName].DbColumns.ContainsKey(tmp[0]))
                            {
                                tables[tableName].DbIndices[indexName].Columns[tmp[0]] =
                                    tmp.Length > 1 ? tmp[1] : "ASC";
                            }
                        }
                    }



                    // remove primarykey index
                    var pkColumns = new List<string>();
                    foreach (var (columnName, dbColumn) in dbTable.DbColumns)
                    {
                        if (dbTable.DbColumns[columnName].Pk)
                        {
                            pkColumns.Add(columnName);
                        }
                    }

                    foreach (var (indexName, dbIndex) in dbTable.DbIndices)
                    {
                        var columns = new List<string>();
                        foreach (var (columnName, order) in dbTable.DbIndices[indexName].Columns)
                        {
                            columns.Add(columnName);
                        }

                        if (string.Join(",", columns.OrderBy(x => x)) == string.Join(",", pkColumns.OrderBy(x => x)))
                        {
                            dbTable.DbIndices.Remove(indexName);
                        }
                    }

                    // get check list
                    query = @"
                SELECT
                    co.consrc,
                    co.conname
                FROM
                    pg_constraint AS co 
                INNER JOIN
                    pg_class AS cl
                ON
                    co.conrelid = cl.oid
                WHERE
                    co.contype = 'c'
                AND
                    cl.relname = @relname";
                    foreach (var row in GetResult(con, query,
                        new Dictionary<string, object>() {{"relname", tableName}}))
                    {
                        var m = Regex.Match(Convert.ToString(row["consrc"]), @"\((.*)\)");
                        var consrc = m.Success ? m.Groups[1].Value : Convert.ToString(row["consrc"]);
                        foreach (var (colName, dbColumn ) in dbTable.DbColumns)
                        {
                            if (consrc.Contains(colName))
                            {
                                dbTable.DbColumns[colName].Check = consrc;
                                dbTable.DbColumns[colName].CheckName = Convert.ToString(row["conname"]);
                            }
                        }
                    }


                    // get foreign key list
                    query = @"
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    pc.confupdtype,
                    pc.confdeltype
                FROM
                    information_schema.table_constraints AS tc
                INNER JOIN
                    information_schema.key_column_usage AS kcu
                ON
                    tc.constraint_name = kcu.constraint_name
                INNER JOIN
                    information_schema.constraint_column_usage AS ccu
                ON
                    ccu.constraint_name = tc.constraint_name
                INNER JOIN
                    pg_constraint AS pc
                ON
                    tc.constraint_name = pc.conname
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
                AND
                    tc.table_name = @table_name";
                    var conf = new Dictionary<string, string>()
                    {
                        {"a", ""},
                        {"r", "RESTRICT"},
                        {"c", "CASCADE"},
                        {"n", "SET NULL"},
                        {"d", "SET DEFAULT"}
                    };
                    foreach (var row in GetResult(con, query,
                        new Dictionary<string, object>() {{"table_name", tableName}}))
                    {

                        var columnName = Convert.ToString(row["column_name"]);
                        if (!tables[tableName].DbColumns.ContainsKey(columnName))
                        {
                            continue;
                        }

                        var update = conf.ContainsKey(Convert.ToString(row["confupdtype"]))
                            ? conf[Convert.ToString(row["confupdtype"])]
                            : "";
                        var del = conf.ContainsKey(Convert.ToString(row["confdeltype"]))
                            ? conf[Convert.ToString(row["confdeltype"])]
                            : "";


                        var key = $"{row["foreign_table_name"]}.{row["foreign_column_name"]}";
                        tables[tableName].DbColumns[columnName].Fk.Add(Convert.ToString(row["constraint_name"]),
                            new DbForeignKey()
                            {
                                Table = Convert.ToString(row["foreign_table_name"]),
                                Column = Convert.ToString(row["foreign_column_name"]),
                                Update = update,
                                Delete = del
                            });
                    }


                    //      trimDbProperties(db);

                }

                return tables;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tables"></param>
        /// <returns></returns>
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





        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <param name="targetTable"></param>
        /// <param name="targetColumn"></param>
        /// <param name="onUpdate"></param>
        /// <param name="onDelete"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task ExecQuery(string query)
        {
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
        }

        private List<Dictionary<string, object>> GetResult(NpgsqlConnection con, string query,
            Dictionary<string, object> parameters = null)
        {
            var res = new List<Dictionary<string, object>>();
            using (var cmd = new NpgsqlCommand(query, con))
            {
                if (parameters?.Any() ?? false)
                {
                    foreach (var (name, value) in parameters)
                    {
                        cmd.Parameters.Add(new NpgsqlParameter(name, value));
                    }
                }

                using (var reader = cmd.ExecuteReader())
                {
                    var columns = reader.GetColumnSchema();
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        foreach (var column in columns)
                        {
                            row.Add(column.ColumnName, reader[column.ColumnName]);
                        }

                        res.Add(row);
                    }

                }
            }

            return res;
        }

    }
}