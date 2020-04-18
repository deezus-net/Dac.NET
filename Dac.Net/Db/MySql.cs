using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Dac.Net.Core;
using MySql.Data.MySqlClient;
using Org.BouncyCastle.Math.Field;

namespace Dac.Net.Db
{
    public class MySql : IDb
    {
        private readonly Server _server;
        private MySqlConnection _mySqlConnection;

        public MySql(Server server)
        {
            _server = server;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public string Drop(DataBase db, bool queryOnly)
        {
            var queries = new StringBuilder();
            foreach (var (tableName, table) in db.Tables)
            {
                queries.AppendLine($"DROP TABLE IF EXISTS `{tableName}`;");
            }

            var result = queries.ToString();

            if (queryOnly)
            {
                return result;
            }

            using (var trn = _mySqlConnection.BeginTransaction())
            {
                var query = @$"SET FOREIGN_KEY_CHECKS = 0;
                                          {result}
                                          SET FOREIGN_KEY_CHECKS = 1;";
                using (var cmd = new MySqlCommand(query, trn.Connection, trn))
                {
                    cmd.ExecuteNonQuery();
                }

                trn.Commit();
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataBase Extract()
        {
            var tables = new Dictionary<string, Table>();

            foreach (DataRow row in GetResult("show tables").Rows)
            {
                tables.Add(row.Field<string>(0), new Table());
            }

            foreach (var (tableName, table) in tables)
            {
                // get column list
                foreach (DataRow row in GetResult($"DESCRIBE {tableName}").Rows)
                {
                    var type = row.Field<string>("Type");
                    var length = 0;
                    // let length = parseInt((type.match(/\(([0-9]+)\)/) || [])[1] || 0, 10);
                    // type = type.replace(/\([0-9]+\)/, '');

                    if (type == "int")
                    {
                        length = 0;
                    }

                    var column = new Column()
                    {
                        Type = type,
                        Length = Convert.ToString(length),
                        Pk = row.Field<string>("Key") == "PRI",
                        NotNull = row.Field<string>("Null") == "NO",
                        Id = row.Field<string>("Extra") == "auto_increment"
                    };
                    if (!string.IsNullOrWhiteSpace(row.Field<string>("Default")))
                    {
                        column.Default = row.Field<string>("Default");
                    }

                    tables[tableName].Columns.Add(row.Field<string>("Field"), column);
                }

                // get foreign key
                var fkNames = new List<string>();
                var query = @"
                    SELECT
                        col.TABLE_NAME AS table_name,
                        col.COLUMN_NAME AS column_name,
                        t.CONSTRAINT_NAME AS constraint_name,
                        col.REFERENCED_TABLE_NAME AS foreign_table_name,
                        col.REFERENCED_COLUMN_NAME AS foreign_column_name,
                        rc.UPDATE_RULE,
                        rc.DELETE_RULE 
                    FROM
                        information_schema.KEY_COLUMN_USAGE col 
                    INNER JOIN
                        information_schema.TABLE_CONSTRAINTS t 
                    ON
                        col.TABLE_SCHEMA = t.TABLE_SCHEMA 
                    AND
                        col.CONSTRAINT_NAME = t.CONSTRAINT_NAME 
                    INNER JOIN 
                        information_schema.REFERENTIAL_CONSTRAINTS AS rc 
                    ON
                        col.CONSTRAINT_name = rc.CONSTRAINT_NAME 
                    AND
                        col.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA 
                    WHERE
                        t.TABLE_SCHEMA = @database 
                    AND 
                        t.TABLE_NAME = @table 
                    AND
                        t.CONSTRAINT_TYPE = 'FOREIGN KEY'";
                foreach (DataRow row in GetResult(query, null, new MySqlParameter("database", _server.Database),
                    new MySqlParameter("table", tableName)).Rows)
                {

                    var columnName = row.Field<string>("column_name");
                    if (!table.Columns.ContainsKey(columnName))
                    {
                        continue;
                    }

                    var updateRule = row.Field<string>("UPDATE_RULE");
                    var deleteRule = row.Field<string>("DELETE_RULE");

                    if (updateRule == "NO ACTION" || updateRule == "RESTRICT")
                    {
                        updateRule = "";
                    }

                    if (deleteRule == "NO ACTION" || deleteRule == "RESTRICT")
                    {
                        deleteRule = "";
                    }

                    fkNames.Add(row.Field<string>("constraint_name"));
                    table.Columns[columnName].ForeignKeys.Add(row.Field<string>("constraint_name"), new ForeignKey()
                    {
                        Table = row.Field<string>("foreign_table_name"),
                        Column = row.Field<string>("foreign_column_name"),
                        Update = updateRule,
                        Delete = deleteRule
                    });
                }

                // get index list
                foreach (DataRow row in GetResult($"SHOW INDEX FROM {tableName} WHERE Key_name != 'PRIMARY'").Rows)
                {

                    var indexName = row.Field<string>("Key_name");
                    /*     if (fkNames.Contains(indexName) && checkDiff) {
                             // ignore when same name foreign key exists
                             continue;
                         }
                     */
                    var nonUnique = row.Field<string>("Non_unique");
                    var collation = row.Field<string>("Collation");
                    if (!table.Indices.ContainsKey(indexName))
                    {
                        tables[tableName].Indices.Add(indexName, new Index()
                        {
                            Unique = nonUnique == "0"
                        });
                    }

                    if (row.Field<string>("Index_type") == "FULLTEXT")
                    {
                        table.Indices[indexName].Type = "fulltext";
                        table.Indices[indexName].Columns.Add(row.Field<string>("Column_name"), "ASC");
                    }
                    else
                    {
                        table.Indices[indexName].Columns.Add(row.Field<string>("Column_name"),
                            collation == "A" ? "ASC" : "DESC");
                    }
                }
            }

            var db = new DataBase() {Tables = tables};
            Utility.TrimDataBaseProperties(db);
            return db;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        public string Query(DataBase db)
        {
            return CreateQuery(db);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public string Create(DataBase db, bool queryOnly)
        {
            var query = CreateQuery(db);
            if (!queryOnly)
            {
                using (var trn = _mySqlConnection.BeginTransaction())
                {
                    using (var cmd = new MySqlCommand(query, trn.Connection, trn))
                    {
                        cmd.ExecuteNonQuery();
                    }

                    trn.Commit();

                }
            }

            return query;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public string ReCreate(DataBase db, bool queryOnly)
        {
            var tables = new List<string>();
            foreach (DataRow row in GetResult("show tables").Rows)
            {
                tables.Add(row.Field<string>(0));
            }

            var query = new StringBuilder();
            if (tables.Any())
            {
                query.AppendLine("SET FOREIGN_KEY_CHECKS = 0;");
                query.AppendLine($"DROP TABLE {string.Join(",", tables.Select(x => $"`{x}`"))};");
                query.AppendLine("SET FOREIGN_KEY_CHECKS = 1;");
            }

            query.AppendLine(CreateQuery(db));

            var result = query.ToString();
            if (queryOnly)
            {
                return result;
            }


            using (var trn = _mySqlConnection.BeginTransaction())
            {
                using (var cmd = new MySqlCommand(result, trn.Connection, trn))
                {
                    cmd.ExecuteNonQuery();
                }

                trn.Commit();

            }

            return result;
        }

        public string Update(DataBase db, bool queryOnly, bool dropTable)
        {
            var diff = Diff(db);
            var orgDb = diff.CurrentDb;
            var query = new StringBuilder();
            var createFkQuery = new List<string>();
            var dropFkQuery = new List<string>();
            // fk


            // add tables
            if (diff.AddedTables.Any())
            {
                query.AppendLine(CreateQuery(new DataBase() {Tables = diff.AddedTables}));
            }

            foreach (var (tableName, table) in diff.ModifiedTables)
            {

                // add columns
                foreach (var (columnName, column) in table.AddedColumns)
                {
                    var notNull = (column.NotNull ?? false) ? " NOT NULL " : " NULL ";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";
                    var type = ((column.Id ?? false) ? "int" : column.Type) +
                               (column.LengthInt > 0 ? $"({column.Length})" : "");
                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $") CHECK({column.Check}) " : "";

                    query.AppendLine(
                        $"ALTER TABLE `{tableName}` ADD COLUMN `{columnName}` {type}{((column.Id ?? true) ? " AUTO_INCREMENT" : "")}{notNull}{def}{check};");

                    foreach (var (fkName, fk) in column.ForeignKeys)
                    {
                        createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                            fk.Update, fk.Delete));
                    }
                }

                // modify columns
                foreach (var (columnName, columns) in table.ModifiedColumns)
                {
                    var orgColumn = columns[0];
                    var newColumn = columns[1];

                    var notNull = (newColumn.NotNull ?? false) ? " NOT NULL " : " NULL ";
                    var def = !string.IsNullOrWhiteSpace(newColumn.Default) ? $" DEFAULT {newColumn.Default} " : "";
                    var type = ((newColumn.Id ?? false) ? "int" : newColumn.Type) +
                               (newColumn.LengthInt > 0 ? $"({newColumn.Length})" : "");
                    var check = !string.IsNullOrWhiteSpace(newColumn.Check) ? $" CHECK({newColumn.Check}) " : "";

                    query.AppendLine(
                        $"ALTER TABLE `{tableName}` MODIFY `{columnName}` {type}{((newColumn.Id ?? false) ? " AUTO_INCREMENT" : "")}{notNull}{def};");

                    // foreign key
                    var orgFk = orgColumn.ForeignKeys ?? new Dictionary<string, ForeignKey>();
                    var newFk = newColumn.ForeignKeys ?? new Dictionary<string, ForeignKey>();

                    foreach (var fkName in orgFk.Keys.Concat(newFk.Keys).Distinct())
                    {
                        if (!orgFk.ContainsKey(fkName))
                        {
                            var fk = newColumn.ForeignKeys[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                            continue;
                        }

                        if (!newFk.ContainsKey(fkName))
                        {

                            dropFkQuery.Add($"ALTER TABLE `{tableName}` DROP FOREIGN KEY `{fkName}`;");
                            // drop foreign key index
                            dropFkQuery.Add($"ALTER TABLE `{tableName}` DROP INDEX `{fkName}`;");
                            continue;
                        }

                        if ((orgColumn.ForeignKeys[fkName].Update != newColumn.ForeignKeys[fkName].Update) ||
                            (orgColumn.ForeignKeys[fkName].Delete != newColumn.ForeignKeys[fkName].Delete) ||
                            (orgColumn.ForeignKeys[fkName].Table != newColumn.ForeignKeys[fkName].Table) ||
                            (orgColumn.ForeignKeys[fkName].Column != newColumn.ForeignKeys[fkName].Column))
                        {

                            dropFkQuery.Add($"ALTER TABLE `{tableName}` DROP FOREIGN KEY `{fkName}`;");

                            // drop foreign key index
                            dropFkQuery.Add($"ALTER TABLE `{tableName}` DROP INDEX `{fkName}`;");

                            var fk = newColumn.ForeignKeys[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                        }
                    }

                }

                // drop columns
                foreach (var columnName in table.DeletedColumnName)
                {
                    query.AppendLine($"ALTER TABLE `{tableName}` DROP COLUMN `{columnName}`;");
                }

                // create index
                foreach (var (indexName, index) in table.AddedIndices)
                {
                    query.AppendLine(
                        $"ALTER TABLE `{tableName}` ADD {((index.Unique ?? false) ? "UNIQUE " : "")}{((index.Type ?? "").ToLower() == "fulltext" ? "FULLTEXT " : "")}INDEX `{indexName}` ({string.Join(",", index.Columns.Select(x => $"`{x.Key}` {x.Value}"))}));");
                }

                // modify index
                foreach (var (indexName, columns) in table.ModifiedIndices)
                {
                    var index = columns[1];
                    query.AppendLine(
                        $"ALTER TABLE `{tableName}` DROP INDEX `{indexName}`, ADD {((index.Unique ?? false) ? "UNIQUE " : "")}{((index.Type ?? "").ToLower() == "fulltext" ? "FULLTEXT " : "")}INDEX `{indexName}` ({string.Join(",", index.Columns.Select(x => $"`{x.Key}` {x.Value}"))}));");
                }

                // drop index
                foreach (var indexName in table.DeletedIndexNames)
                {
                    query.AppendLine($"ALTER TABLE `{tableName}` DROP INDEX `{indexName}`;");
                }
            }

            // drop tables
            if (dropTable && diff.DeletedTableNames.Any())
            {
                query.AppendLine("SET FOREIGN_KEY_CHECKS = 0;");
                foreach (var tableName in diff.DeletedTableNames)
                {
                    query.AppendLine($"DROP TABLE `{tableName}`;");
                }

                query.AppendLine("SET FOREIGN_KEY_CHECKS = 1;");
            }

            var result = string.Join("\n", dropFkQuery) + "\n" + query.ToString() + "\n" +
                         string.Join("\n", createFkQuery);
            if (!string.IsNullOrWhiteSpace(result))
            {
                if (!queryOnly)
                {
                    using (var trn = _mySqlConnection.BeginTransaction())
                    {
                        using (var cmd = new MySqlCommand(result, trn.Connection, trn))
                        {
                            cmd.ExecuteNonQuery();
                        }

                        trn.Commit();
                    }

                }

                return result;

            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        public Diff Diff(DataBase db)
        {
            return new Diff(Extract(),   db);
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
                var connectionString = new MySqlConnectionStringBuilder()
                {
                    Server = _server.Host,
                    UserID = _server.User,
                    Password = _server.Password,
                    Database = _server.Database
                };
                if (_server.Port != null)
                {
                    connectionString.Port = (uint) _server.Port;
                }

                _mySqlConnection = new MySqlConnection(connectionString.ToString());
                _mySqlConnection.Open();

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
                _mySqlConnection.Close();
            }
            catch (Exception e)
            {

            }

            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <param name="trn"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        private DataTable GetResult(string query, MySqlTransaction trn = null, params MySqlParameter[] parameters)
        {
            var res = new DataTable();
            using (var cmd =
                trn == null ? _mySqlConnection.CreateCommand() : new MySqlCommand("", trn.Connection, trn))
            {
                cmd.CommandText = query;
                if (parameters.Any())
                {
                    cmd.Parameters.AddRange(parameters);
                }

                using (var da = new MySqlDataAdapter(cmd))
                {
                    da.Fill(res);
                }
            }

            return res;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private string CreateQuery(DataBase db)
        {
            var query = new StringBuilder();

            foreach (var (tableName, table) in db.Tables)
            {

                query.AppendLine($"CREATE TABLE `{tableName}` (");
                var columnQuery = new StringBuilder();
                var pk = new List<string>();

                foreach (var (columnName, column) in table.Columns)
                {

                    if (column.Id ?? false)
                    {
                        column.NotNull = true;
                        column.Type = "int";
                    }

                    var notNull = (column.NotNull ?? false) ? " NOT NULL " : "";
                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $") CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $") DEFAULT {column.Default} " : "";
                    var type = column.Type + (column.LengthInt > 0 ? $"({column.Length})" : "");

                    columnQuery.AppendLine(
                        $"    `{columnName}` {type}{((column.Id ?? false) ? " AUTO_INCREMENT " : "")}{notNull}{def}{check}");
                    if ((column.Pk ?? false) || (column.Id ?? false))
                    {
                        pk.Add(columnName);
                    }
                }

                query.AppendLine(columnQuery + (pk.Any() ? "," : ""));

                if (pk.Any())
                {
                    query.AppendLine("    PRIMARY KEY");
                    query.AppendLine("    (");
                    query.AppendLine(string.Join(",\n", pk.Select(x => $"        `{x}`")));
                    query.AppendLine($"    ){(table.Indices.Any() ? "," : "")}");
                }

                var indexQuery = new StringBuilder();
                foreach (var (indexName, index) in table.Indices)
                {
                    indexQuery.AppendLine(
                        $"    {((index.Unique ?? false) ? "UNIQUE " : "")}{((index.Type ?? "").ToLower() == "fulltext" ? "FULLTEXT " : "")}INDEX `{indexName}`)({string.Join(",", index.Columns.Select(x => $"`{x.Key}` {x.Value}"))})");
                }

                query.AppendLine(indexQuery.ToString());
                query.AppendLine(");");

            }

            // foreign key
            foreach (var (tableName, table) in db.Tables)
            {
                foreach (var (columnName, column) in table.Columns.Where(x => x.Value.ForeignKeys.Any()))
                {

                    foreach (var (fkName, fk) in column.ForeignKeys)
                    {
                        query.AppendLine(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                            fk.Update, fk.Delete));
                    }
                }
            }

            return query.ToString();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <param name="targetTable"></param>
        /// <param name="targetColumn"></param>
        /// <param name="onupdate"></param>
        /// <param name="ondelete"></param>
        /// <returns></returns>
        private static string CreateAlterForeignKey(string name, string table, string column, string targetTable,
            string targetColumn, string onupdate, string ondelete)
        {
            if (!string.IsNullOrWhiteSpace(onupdate))
            {
                onupdate = $" ON UPDATE {onupdate} ";
            }

            if (!string.IsNullOrWhiteSpace(ondelete))
            {
                ondelete = $" ON DELETE {ondelete} ";
            }


            /*  // check index
              let hasIndex = false;
              //const hasIndex = tables[foreignTable].indexes.Any(i => i.Value.Columns.All(c => c.Key == foreignColumn));
              if (!hasIndex) {
                  query += `ALTER TABLE \`${foreignTable}\` ADD INDEX \`fk_${foreignTable}_${foreignColumn}_index\` (\`${foreignColumn}\` ASC);\n`;
              }
              //hasIndex = tables[table].Indexes.Any(i => i.Value.Columns.All(c => c.Key == column));
              if (!hasIndex) {
                  query += `ALTER TABLE \`${table}\` ADD INDEX \`fk_${table}_${column}_index\` (\`${column}\` ASC);\n`;
              }
        */

            return
                $"ALTER TABLE `{table}` ADD CONSTRAINT `{name}` FOREIGN KEY (`${column}`) REFERENCES `{targetTable}`(`${targetColumn}`){onupdate}{ondelete};";
        }
    }


}