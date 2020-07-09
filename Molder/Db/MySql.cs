using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Molder.Core;
using MySql.Data.MySqlClient;
using Org.BouncyCastle.Math.Field;

namespace Molder.Db
{
    public class MySql : IDb
    {
        private readonly Server _server;
        private MySqlConnection _mySqlConnection;
        private bool _dryRun = false;

        public MySql(Server server, bool dryRun)
        {
            _server = server;
            _dryRun = dryRun;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetName()
        {
            return _server.Name;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public QueryResult Drop(DataBase db, bool queryOnly)
        {
            var queryResult = new QueryResult();
            var queries = new StringBuilder();

            queries.AppendLine(Utility.CreateQueryHeader(_server));
            queries.AppendLine("SET FOREIGN_KEY_CHECKS = 0;");
            foreach (var (tableName, table) in db.Tables)
            {
                queries.AppendLine($"DROP TABLE IF EXISTS `{tableName}`;");
            }
            foreach (var (viewName, definition) in db.Views ?? new Dictionary<string, string>())
            {
                queries.AppendLine($"DROP VIEW IF EXISTS `{viewName}`;");
            }
            queries.AppendLine("SET FOREIGN_KEY_CHECKS = 1;");

            queryResult.Query = queries.ToString();

            if (queryOnly)
            {
                return queryResult;
            }

            Transaction(queryResult);
            return queryResult;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataBase Extract()
        {
            var tables = new Dictionary<string, Table>();
            var query = @"
                        SELECT 
                            TABLE_NAME AS name, 
                            TABLE_TYPE AS type 
                        FROM 
                            information_schema.TABLES 
                        WHERE 
                            TABLE_SCHEMA = @database
                        AND 
                            TABLE_TYPE = 'BASE TABLE'";

            foreach (DataRow row in GetResult(query, null, new MySqlParameter("database", _server.Database)).Rows)
            {
                tables.Add(row.Field<string>(0), new Table());
            }

            foreach (var (tableName, table) in tables)
            {
                // get column list
                foreach (DataRow row in GetResult($"DESCRIBE {tableName}").Rows)
                {
                    var type = row.Field<string>("Type");
                    var length = "0";

                    var m = Regex.Match(type, @"\(([0-9,]+)\)");
                    if (m.Success)
                    {
                        length = m.Groups[1].Value;
                        type = Regex.Replace(type, @"\(([0-9,]+)\)", "");
                    }
                    
                    if (type == "int")
                    {
                        length = "0";
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
                query = @"
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
                    if (fkNames.Contains(indexName))
                    {
                        // ignore when same name foreign key exists
                        continue;
                    }

                    var nonUnique = row.Field<long>("Non_unique");
                    var collation = row.Field<string>("Collation");
                    if (!table.Indexes.ContainsKey(indexName))
                    {
                        tables[tableName].Indexes.Add(indexName, new Index()
                        {
                            Unique = nonUnique == 0
                        });
                    }

                    if (row.Field<string>("Index_type") == "FULLTEXT")
                    {
                        table.Indexes[indexName].Type = "fulltext";
                        table.Indexes[indexName].Columns.Add(row.Field<string>("Column_name"), "ASC");
                    }
                    else
                    {
                        table.Indexes[indexName].Columns.Add(row.Field<string>("Column_name"),
                            collation == "A" ? "ASC" : "DESC");
                    }
                }
            }
            
            // get views
            var views = new Dictionary<string, string>();
            query = @"
                SELECT 
                    TABLE_NAME AS name,
                    VIEW_DEFINITION AS definition
                FROM
                    information_schema.VIEWS
                WHERE
                    TABLE_SCHEMA = @database
            ";
            foreach (DataRow row in GetResult(query, null, new MySqlParameter("database", _server.Database)).Rows)
            {
                views.Add(row.Field<string>("name"), row.Field<string>("definition"));
            }

            var db = new DataBase() {Tables = tables, Views = views};
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
        public QueryResult Create(DataBase db, bool queryOnly)
        {
            var query = new StringBuilder();
            query.AppendLine(Utility.CreateQueryHeader(_server));
            query.AppendLine(CreateQuery(db));
            var queryResult = new QueryResult
            {
                Query = query.ToString()
            };
            if (queryOnly)
            {
                return queryResult;
            }
            
            Transaction(queryResult);
            return queryResult;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public QueryResult ReCreate(DataBase db, bool queryOnly)
        {
            var queryResult = new QueryResult();
            var tables = new List<string>();
            var views = new List<string>();
            
            foreach (DataRow row in GetResult("SELECT TABLE_NAME AS name, TABLE_TYPE AS type from information_schema.TABLES WHERE TABLE_SCHEMA = @database", null, new MySqlParameter("database", _server.Database)).Rows)
            {
                if (row.Field<string>("type") == "VIEW")
                {
                    views.Add(row.Field<string>("name"));
                }
                else if (row.Field<string>("type") == "BASE TABLE")
                {
                    tables.Add(row.Field<string>("name"));
                }
                
            }

            var query = new StringBuilder();
            query.AppendLine(Utility.CreateQueryHeader(_server));
            if (tables.Any())
            {
                query.AppendLine("SET FOREIGN_KEY_CHECKS = 0;");
                if (tables.Any())
                {
                    query.AppendLine($"DROP TABLE {string.Join(",", tables.Select(x => $"`{x}`"))};");
                }

                if (views.Any())
                {
                    query.AppendLine($"DROP VIEW {string.Join(",", views.Select(x => $"`{x}`"))};");
                }
                query.AppendLine("SET FOREIGN_KEY_CHECKS = 1;");
            }

            query.AppendLine(CreateQuery(db));

            queryResult.Query = query.ToString();
            if (queryOnly)
            {
                return queryResult;
            }
            
            Transaction(queryResult);
            return queryResult;
            
        }

        public QueryResult Update(DataBase db, bool queryOnly, bool dropTable)
        {
            var queryResult = new QueryResult();
            var diff = Diff(db);
            if (!diff.HasDiff)
            {
                return queryResult;
            }


            var orgDb = diff.CurrentDb;
            var query = new StringBuilder();
            query.AppendLine(Utility.CreateQueryHeader(_server));
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
                               (newColumn.LengthInt > 0 && !string.IsNullOrWhiteSpace(newColumn.Length)
                                   ? $"({newColumn.Length})"
                                   : "");
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
                foreach (var (indexName, index) in table.AddedIndexes)
                {
                    query.AppendLine(
                        $"ALTER TABLE `{tableName}` ADD {((index.Unique ?? false) ? "UNIQUE " : "")}{((index.Type ?? "").ToLower() == "fulltext" ? "FULLTEXT " : "")}INDEX `{indexName}` ({string.Join(",", index.Columns.Select(x => $"`{x.Key}` {x.Value}"))}));");
                }

                // modify index
                foreach (var (indexName, columns) in table.ModifiedIndexes)
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
            
            // views
            var viewQuery = new StringBuilder();
            if (diff.AddedViews.Any())
            {
                viewQuery.AppendLine(CreateQuery(new DataBase() {Views = diff.AddedViews}));
            }

            foreach(var viewName in diff.DeletedViewNames)
            {
                viewQuery.AppendLine($"DROP VIEW `{viewName}`");
            }

            foreach (var (viewName, definition) in diff.ModifiedViews)
            {
                viewQuery.AppendLine($"DROP VIEW `{viewName}`;");

                viewQuery.AppendLine(CreateQuery(new DataBase()
                    {Views = new Dictionary<string, string>() {{viewName, definition[1]}}}));
            }

            queryResult.Query = string.Join("\n", dropFkQuery) + "\n" + query + "\n" +
                                string.Join("\n", createFkQuery)+ "\n" + viewQuery;

            if (queryOnly)
            {
                return queryResult;
            }

            Transaction(queryResult);
            return queryResult;
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
                var columnQuery = new List<string>();
                var pk = new List<string>();

                foreach (var (columnName, column) in table.Columns)
                {

                    if (column.Id ?? false)
                    {
                        column.NotNull = true;
                        column.Type = "int";
                    }

                    var notNull = (column.NotNull ?? false) ? " NOT NULL " : "";
                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $" CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";
                    var type = column.Type + (column.LengthInt > 0 && !string.IsNullOrWhiteSpace(column.Length) ? $"({column.Length})" : "");

                    columnQuery.Add(
                        $"    `{columnName}` {type}{((column.Id ?? false) ? " AUTO_INCREMENT " : "")}{notNull}{def}{check}");
                    if ((column.Pk ?? false) || (column.Id ?? false))
                    {
                        pk.Add(columnName);
                    }
                }

                query.AppendLine(string.Join(",\n", columnQuery) + (pk.Any() ? "," : ""));

                if (pk.Any())
                {
                    query.AppendLine("    PRIMARY KEY");
                    query.AppendLine("    (");
                    query.AppendLine(string.Join(",\n", pk.Select(x => $"        `{x}`")));
                    query.AppendLine($"    ){((table.Indexes?.Any() ?? false) ? "," : "")}");
                }

                var indexQuery = new StringBuilder();
                foreach (var (indexName, index) in (table.Indexes ?? new Dictionary<string, Index>()))
                {
                    indexQuery.AppendLine(
                        $"    {((index.Unique ?? false) ? "UNIQUE " : "")}{((index.Type ?? "").ToLower() == "fulltext" ? "FULLTEXT " : "")}INDEX `{indexName}`({string.Join(",", index.Columns.Select(x => $"`{x.Key}` {x.Value}"))})");
                }

                query.AppendLine(indexQuery.ToString());
                query.AppendLine(");");

            }

            // foreign key
            foreach (var (tableName, table) in db.Tables)
            {
                foreach (var (columnName, column) in table.Columns.Where(x => x.Value.ForeignKeys?.Any() ?? false))
                {

                    foreach (var (fkName, fk) in column.ForeignKeys)
                    {
                        query.AppendLine(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                            fk.Update, fk.Delete));
                    }
                }
            }
            
            // Views
            foreach (var (viewName, definition) in db.Views ?? new Dictionary<string, string>())
            {
                query.AppendLine($"CREATE VIEW `{viewName}` AS {definition};");
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
                $"ALTER TABLE `{table}` ADD CONSTRAINT `{name}` FOREIGN KEY (`{column}`) REFERENCES `{targetTable}`(`{targetColumn}`){onupdate}{ondelete};";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queryResult"></param>
        private void Transaction(QueryResult queryResult)
        {
            using (var trn = _mySqlConnection.BeginTransaction())
            {
                try
                {
                    using (var cmd = new MySqlCommand(queryResult.Query, trn.Connection, trn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    queryResult.Success = false;
                    queryResult.Exception = e;
                    trn.Rollback();
                    return;
                }

                if (_dryRun)
                {
                    trn.Rollback();
                }
                else
                {
                    trn.Commit();
                }
            }
        }
    }


}