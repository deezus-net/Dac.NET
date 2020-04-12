using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;

namespace Dac.Net.Db
{
    public class MsSql : IDb
    {
        private Server _server;
        private SqlConnection _sqlConnection;

        public MsSql(Server server)
        {
            _server = server;
        }

        public string Drop(DataBase db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public DataBase Extract()
        {
            throw new System.NotImplementedException();
        }

        public string Query(DataBase db)
        {
            throw new System.NotImplementedException();
        }

        public string Create(DataBase db, bool queryOnly)
        {
            var queries = new StringBuilder();
            queries.AppendLine($"USE [{_server.Database}];");
            queries.AppendLine(CreateQuery(db));
            var query = queries.ToString();
            if (queryOnly)
            {
                return query;
            }

            using (var cmd = _sqlConnection.CreateCommand())
            {
                cmd.CommandText = query;
                cmd.ExecuteNonQuery();
            }

            return query;
        }

        public string ReCreate(DataBase db, bool queryOnly)
        {
            var queries = new StringBuilder();
            queries.AppendLine($"USE [{_server.Database}];");

            // get table and foreign key list
            var tables = new List<string>();
            var foreignKeys = new Dictionary<string, string>();

            var query = @"
                        SELECT
                            t.name AS table_name,
                            fk.name AS fk_name
                        FROM
                            sys.tables AS t
                        LEFT OUTER JOIN
                            sys.foreign_keys AS fk
                        ON
                            fk.parent_object_id = t.object_id
            ";

            foreach (DataRow row in GetResult(query).Rows)
            {

                var tableName = row.Field<string>("table_name");
                var fkName = row.Field<string>("fk_name");
                if (!tables.Contains(tableName))
                {
                    tables.Add(tableName);
                }

                if (!string.IsNullOrWhiteSpace(fkName))
                {
                    foreignKeys[fkName] = tableName;
                }
            }

            // drop exist foreign keys
            foreach (var (fkName, tableName) in foreignKeys)
            {
                queries.AppendLine("ALTER TABLE");
                queries.AppendLine($"[{tableName}]");
                queries.AppendLine("DROP CONSTRAINT");
                queries.AppendLine($"[{fkName}];");
            }

            // drop exist tables
            foreach (var tableName in tables)
            {
                queries.AppendLine($"DROP TABLE [{tableName}];");
            }

            queries.AppendLine(CreateQuery(db));

            var result = queries.ToString();
            if (queryOnly)
            {
                return result;
            }

            using (var trn = _sqlConnection.BeginTransaction())
            {
                using (var cmd = new SqlCommand("", trn.Connection, trn))
                {
                    cmd.CommandText = result;
                    cmd.ExecuteNonQuery();
                }

                trn.Commit();
            }

            return result;
        }

        public string Update(DataBase db, bool queryOnly, bool dropTable)
        {
            throw new System.NotImplementedException();
        }

        public Diff Diff(DataBase db)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Connect()
        {
            try
            {
                var port = _server.Port != null ? $",{_server.Port}" : "";
                var connectionString = new SqlConnectionStringBuilder()
                {
                    DataSource = $"{_server.Host}{port}",
                    UserID = _server.User,
                    Password = _server.Password,
                    InitialCatalog = _server.Database
                }.ToString();
                _sqlConnection = new SqlConnection(connectionString);
                _sqlConnection.Open();

                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Close()
        {
            try
            {
                _sqlConnection.Close();
            }
            catch (Exception e)
            {

            }

            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private string CreateQuery(DataBase db)
        {
            var query = new StringBuilder();
            var fkQuery = new StringBuilder();

            foreach (var (tableName, table) in db.Tables)
            {
                query.AppendLine($"CREATE TABLE [dbo].[{tableName}](");
                var columnQuery = new List<string>();
                var pk = new List<string>();
                foreach (var (columnName, column) in table.Columns)
                {

                    if (column.Id ?? false)
                    {
                        column.NotNull = true;
                        column.Type = "int";
                    }

                    var identity = (column.Id ?? false) ? " IDENTITY " : "";
                    var notNull = (column.NotNull ?? false) ? " NOT NULL " : "";
                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $" CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";
                    var type = column.Type + (column.LengthInt > 0 ? $"({column.LengthInt})" : "");

                    columnQuery.Add($"    [{columnName}] {type}{identity}{notNull}{def}{check}");
                    if ((column.Pk ?? false) || (column.Id ?? false))
                    {
                        pk.Add(columnName);
                    }

                }

                query.AppendLine(string.Join(",\n", columnQuery) + (pk.Any() ? "," : ""));

                if (pk.Any())
                {
                    query.AppendLine($"    CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED");
                    query.AppendLine("    (");
                    var pkQuery = new List<string>();
                    pk.ForEach(p => { pkQuery.Add($"        [{p}]"); });
                    query.AppendLine(string.Join("\n,", pkQuery));
                    query.AppendLine("    )");
                }

                // foreign key
                foreach (var (columnName, column) in table.Columns)
                {

                    if (column.ForeignKeys.Any())
                    {
                        foreach (var (fkName, foreignKey) in column.ForeignKeys)
                        {
                            fkQuery.AppendLine(CreateAlterForeignKey(fkName, tableName, columnName, foreignKey.Table,
                                foreignKey.Column, foreignKey.Update, foreignKey.Delete));
                        }
                    }
                }

                query.AppendLine(");");

                var num = 1;
                foreach (var (indexName, index) in table.Indices)
                {
                    var name = !string.IsNullOrWhiteSpace(indexName) ? indexName : $"INDEX_{tableName}_${num++}";
                    query.AppendLine($"CREATE {(index.Unique ? "UNIQUE " : "")}INDEX [{name}] ON [dbo].[{tableName}](");
                    query.AppendLine(
                        $"    {string.Join(",", index.Columns.Keys.Select(c => $"[{c}] {index.Columns[c]}"))}");
                    query.AppendLine(");");
                }

            }

            return $"{query}\n{fkQuery}";
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
            else
            {
                onUpdate = "";
            }

            if (!string.IsNullOrWhiteSpace(onDelete))
            {
                onDelete = $" ON DELETE {onDelete} ";
            }
            else
            {
                onDelete = "";
            }

            var query = new StringBuilder();

            query.AppendLine("ALTER TABLE");
            query.AppendLine($"    [dbo].[{table}]");
            query.AppendLine("ADD CONSTRAINT");
            query.AppendLine($"    [{name}]");
            query.AppendLine("FOREIGN KEY");
            query.AppendLine("(");
            query.AppendLine($"    [{column}]");
            query.AppendLine(")");
            query.AppendLine($"REFERENCES [dbo].[{targetTable}]([{targetColumn}]){onUpdate}{onDelete};");

            return query.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="trn"></param>
        /// <returns></returns>
        private DataTable GetResult(string query, SqlTransaction trn = null)
        {
            var res = new DataTable();
            using (var cmd = trn == null ? _sqlConnection.CreateCommand() : new SqlCommand("", trn.Connection, trn))
            {
                cmd.CommandText = query;
                using (var da = new SqlDataAdapter(cmd))
                {
                    da.Fill(res);
                }
            }

            return res;
        }

    }
}