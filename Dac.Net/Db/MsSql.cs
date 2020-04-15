using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Dac.Net.Core;
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataBase Extract()
        {
            var tables = new Dictionary<string, Table>();

            var columns = new Dictionary<string, Dictionary<string, Column>>();
            var indices = new Dictionary<string, Dictionary<string, Index>>();
            var pk = new Dictionary<string, List<string>>();

            var query = @"
                SELECT
                    t.name AS table_name,
                    i.name AS index_name,
                    col.name AS column_name,
                    i.is_primary_key,
                    c.is_descending_key,
                    i.is_unique
                FROM
                    sys.indexes as i 
                INNER JOIN
                    sys.tables as t
                ON
                    i.object_id = t.object_id 
                INNER JOIN
                    sys.index_columns AS c
                ON
                    i.object_id = c.object_id 
                AND
                    i.index_id = c.index_id
                INNER JOIN
                    sys.columns as col
                ON
                    c.object_id = col.object_id
                AND
                    c.column_id = col.column_id
                ORDER BY t.name, i.name, c.key_ordinal
        ";

            foreach (DataRow row in GetResult(query).Rows)
            {
                var tableName = row.Field<string>("table_name");

                if (!pk.ContainsKey(tableName))
                {
                    pk.Add(tableName, new List<string>());
                }

                if (!indices.ContainsKey(tableName))
                {
                    indices.Add(tableName, new Dictionary<string, Index>());
                }

                if (row.Field<bool>("is_primary_key"))
                {
                    pk[tableName].Add(row.Field<string>("column_name"));

                }
                else
                {
                    var indexName = row.Field<string>("index_name");
                    if (!indices[tableName].ContainsKey(indexName))
                    {
                        indices[tableName].Add(indexName, new Index() {Unique = row.Field<bool>("is_unique")});
                    }

                    indices[tableName][indexName].Columns[row.Field<string>("column_name")] =
                        row.Field<bool>("is_descending_key") ? "desc" : "asc";
                }
            }

            query = @"
            SELECT
                t.name AS table_name,
                c.name AS column_name,
                type.name AS type,
                c.max_length,
                c.is_nullable ,
                c.is_identity
            FROM
                sys.tables AS t 
            INNER JOIN
                sys.columns AS c 
            ON
                c.object_id = t.object_id 
            INNER JOIN 
                sys.types as type 
            ON 
                c.system_type_id = type.system_type_id 
            AND
                c.user_type_id = type.user_type_id 
            ORDER BY t.name, c.object_id
        ";

            // get column list
            foreach (DataRow row in GetResult(query).Rows)
            {
                var tableName = row.Field<string>("table_name");
                if (!columns.ContainsKey(tableName))
                {
                    columns.Add(tableName, new Dictionary<string, Column>());
                }

                var length = row.Field<short>("max_length");
                var type = row.Field<string>("type");
                switch (type)
                {
                    case "nvarchar":
                    case "nchar":
                        length /= 2;
                        break;
                    case "int":
                    case "datetime":
                        length = 0;
                        break;
                }

                columns[tableName].Add(
                    row.Field<string>("column_name"),
                    new Column()
                    {
                        Id = row.Field<bool>("is_identity"),
                        Type = row.Field<string>("type"),
                        Length = Convert.ToString(length),
                        NotNull = !row.Field<bool>("is_nullable"),
                        Pk = pk.ContainsKey(tableName) && pk[tableName].Contains(row.Field<string>("column_name"))
                    }
                );
            }

            foreach (var tableName in columns.Keys)
            {
                tables.Add(tableName, new Table()
                {
                    Columns = columns[tableName],
                    Indices = indices[tableName]
                });
            }

            foreach (var tableName in tables.Keys)
            {
                // get check list
                query = @"
                SELECT
                    t.name AS table_name, 
                    col.name AS column_name,
                    ch.definition,
                    ch.name
                FROM
                    sys.check_constraints AS ch 
                INNER JOIN
                    sys.tables AS t 
                ON
                    ch.parent_object_id = t.object_id 
                INNER JOIN
                    sys.columns AS col 
                ON
                    ch.parent_column_id = col.column_id 
                AND
                    t.object_id = col.object_id 
                WHERE
                    t.name = @table
            ";
                foreach (DataRow row in GetResult(query, null, new SqlParameter("table", tableName)).Rows)
                {

                    var columnName = row.Field<string>("column_name");
                    var definition = row.Field<string>("definition");
                    var m = Regex.Match(definition, @"\((.*)\)");
                    if (m.Success)
                    {
                        definition = m.Groups[1].Value;
                    }


                    if (tables[tableName].Columns.ContainsKey(columnName))
                    {
                        tables[tableName].Columns[columnName].Check = definition;
                        tables[tableName].Columns[columnName].CheckName = row.Field<string>("name");
                    }
                }

                // get default list
                query = @"
                SELECT
                    t.name AS table_name, 
                    col.name AS column_name,
                    d.definition,
                    d.name
                FROM
                    sys.default_constraints AS d 
                INNER JOIN
                    sys.tables AS t 
                ON
                    d.parent_object_id = t.object_id 
                INNER JOIN
                    sys.columns AS col 
                ON
                    d.parent_column_id = col.column_id 
                AND
                    t.object_id = col.object_id 
                WHERE
                    t.name = @table
            ";
                foreach (DataRow row in GetResult(query, null, new SqlParameter("table", tableName)).Rows)
                {

                    var columnName = row.Field<string>("column_name");

                    var definition = row.Field<string>("definition");
                    var m = Regex.Match(definition, @"\((.*)\)");
                    if (m.Success)
                    {
                        definition = m.Groups[1].Value;
                    }


                    if (tables[tableName].Columns.ContainsKey(columnName))
                    {
                        tables[tableName].Columns[columnName].Default = definition;
                        tables[tableName].Columns[columnName].DefaultName = row.Field<string>("name");
                    }
                }

                // get foreign key list
                query = @"
                SELECT
                    fk.name AS fk_name,
                    t1.name AS table_name,
                    c1.name AS column_name,
                    t2.name AS foreign_table,
                    c2.name AS foreign_column,
                    fk.update_referential_action_desc AS onupdate,
                    fk.delete_referential_action_desc AS ondelete 
                FROM
                    sys.foreign_key_columns AS fkc 
                INNER JOIN
                    sys.tables AS t1 
                ON
                    fkc.parent_object_id = t1.object_id 
                INNER JOIN
                    sys.columns AS c1 
                ON
                    c1.object_id = t1.object_id 
                AND
                    fkc.parent_column_id = c1.column_id 
                INNER JOIN
                    sys.tables AS t2 
                ON
                    fkc.referenced_object_id = t2.object_id 
                INNER JOIN
                    sys.columns AS c2 
                ON
                    c2.object_id = t2.object_id 
                AND
                    fkc.referenced_column_id = c2.column_id 
                INNER JOIN
                    sys.foreign_keys AS fk 
                ON
                    fkc.constraint_object_id = fk.object_id 
                WHERE
                    t1.name = @table
            ";
                foreach (DataRow row in GetResult(query, null, new SqlParameter("table", tableName)).Rows)
                {

                    var columnName = row.Field<string>("column_name");
                    if (tables[tableName].Columns.ContainsKey(columnName))
                    {
                        tables[tableName].Columns[columnName].ForeignKeys.Add(row.Field<string>("fk_name"),
                            new ForeignKey()
                            {

                                Table = row.Field<string>("foreign_table"),
                                Column = row.Field<string>("foreign_column"),
                                Update = row.Field<string>("onupdate"),
                                Delete = row.Field<string>("ondelete")
                            }
                        );
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
        /// <exception cref="NotImplementedException"></exception>
        public string Query(DataBase db)
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <param name="dropTable"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public string Update(DataBase db, bool queryOnly, bool dropTable)
        {
            var diff = Diff(db);
            var query = new StringBuilder();
            var createFkQuery = new List<string>();
            var dropFkQuery = new List<string>();

            var droppedIndexNames = new List<string>();

            // add tables
            if (diff.AddedTables.Any())
            {
                query.AppendLine(CreateQuery(new DataBase() {Tables = diff.AddedTables}));
            }

            foreach (var (tableName, table) in diff.ModifiedTables)
            {
                var orgTable = diff.CurrentDb.Tables[tableName];

                // add columns
                foreach (var (columnName, column) in table.AddedColumns)
                {
                    var type = (column.Id ?? false) ? "int" : column.Type;
                    if (column.LengthInt > 0)
                    {
                        type += $"({column.Length})";
                    }

                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $" CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";

                    query.AppendLine($"ALTER TABLE [{tableName}] ADD [{columnName}] {type}{((column.Id ?? false) ? " IDENTITY" : "")}{((column.NotNull ?? false) ? " NOT NULL" : "")}{def}{check};");

                    foreach (var (fkName, fk) in (column.ForeignKeys ?? new Dictionary<string, ForeignKey>()))
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

                    // if change execute alter
                    foreach (var (indexName, index ) in orgTable.Indices.Where(x =>
                        x.Value.Columns.ContainsKey(columnName))
                    )
                    {
                        if (!droppedIndexNames.Contains(indexName))
                        {
                            query.AppendLine($"DROP INDEX [{indexName}] ON [{tableName}];");

                            droppedIndexNames.Add(indexName);
                        }
                    }


                    var type = (newColumn.Id ?? false) ? "int" : newColumn.Type;
                    if (newColumn.LengthInt > 0)
                    {
                        type += $"({newColumn.Length})";
                    }

                    var check = !string.IsNullOrWhiteSpace(newColumn.Check) ? $" CHECK({newColumn.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(newColumn.Default) ? $" DEFAULT {newColumn.Default} " : "";

                    query.AppendLine($"ALTER TABLE [{tableName}] ALTER COLUMN [{columnName}] {type}{((newColumn.Id ?? false) ? " IDENTITY" : "")}{((newColumn.NotNull ?? false) ? " NOT NULL" : "")};");

                    if (orgColumn.Default != newColumn.Default)
                    {
                        if (!string.IsNullOrWhiteSpace(orgColumn.DefaultName))
                        {
                            query.AppendLine("ALTER TABLE");
                            query.AppendLine($"    [{tableName}]");
                            query.AppendLine("DROP CONSTRAINT");
                            query.AppendLine($"    [{orgColumn.DefaultName}];");
                        }

                        if (!string.IsNullOrWhiteSpace(newColumn.Default))
                        {
                            query.AppendLine("ALTER TABLE");
                            query.AppendLine($"    [{tableName}]");
                            query.AppendLine($"ADD DEFAULT {newColumn.Default} FOR [${columnName}];");
                        }
                    }

                    if (orgColumn.Check != newColumn.Check)
                    {
                        // drop old check
                        if (!string.IsNullOrWhiteSpace(orgColumn.CheckName))
                        {
                            query.AppendLine("ALTER TABLE");
                            query.AppendLine($"    [{tableName}]");
                            query.AppendLine("DROP CONSTRAINT");
                            query.AppendLine($"    [{orgColumn.CheckName}];");
                        }

                        // add new check
                        if (!string.IsNullOrWhiteSpace(newColumn.Check))
                        {
                            query.AppendLine("ALTER TABLE");
                            query.AppendLine($"    [{tableName}]");
                            query.AppendLine($"ADD CHECK({newColumn.Check});");
                        }
                    }

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

                            dropFkQuery.Add("ALTER TABLE");
                            dropFkQuery.Add($"    [dbo].[{tableName}]");
                            dropFkQuery.Add($"DROP CONSTRAINT [{fkName}];");
                            continue;
                        }

                        if ((orgFk[fkName].Update != newFk[fkName].Update) ||
                            (orgFk[fkName].Delete != newFk[fkName].Delete) ||
                            (orgFk[fkName].Table != newFk[fkName].Table) ||
                            (orgFk[fkName].Column != newFk[fkName].Column))
                        {

                            dropFkQuery.Add("ALTER TABLE");
                            dropFkQuery.Add($"    [dbo].[{tableName}]");
                            dropFkQuery.Add($"DROP CONSTRAINT [{fkName}];");

                            var fk = newFk[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                        }
                    }
                }

                // drop columns
                foreach (var columnName in table.DeletedColumnName)
                {
                    if (!string.IsNullOrWhiteSpace(diff.CurrentDb.Tables[tableName].Columns[columnName].DefaultName))
                    {
                        query.AppendLine("ALTER TABLE");
                        query.AppendLine($"    [{tableName}]");
                        query.AppendLine(
                            $"DROP CONSTRAINT [{diff.CurrentDb.Tables[tableName].Columns[columnName].DefaultName}];");
                    }

                    query.AppendLine("ALTER TABLE");
                    query.AppendLine($"    [{tableName}]");
                    query.AppendLine($"DROP COLUMN [{columnName}];");

                }

                // create index
                foreach (var (indexName, index) in table.AddedIndices)
                {
                    query.AppendLine("CREATE");
                    query.AppendLine($"    {((index.Unique ?? false) ? "UNIQUE " : "")}INDEX [{indexName}]");
                    query.AppendLine("ON");
                    query.AppendLine(
                        $"    [dbo].[{tableName}](${string.Join(",", index.Columns.Select(x => $"[{x.Key}] {x.Value}"))});");
                }

                // modify index
                foreach (var (indexName, indices) in table.ModifiedIndices)
                {
                    var index = indices[1];
                    if (!droppedIndexNames.Contains(indexName))
                    {
                        query.AppendLine("DROP INDEX");
                        query.AppendLine($"    [{tableName}].[{indexName}];");
                    }

                    query.AppendLine("CREATE");
                    query.AppendLine($"    {((index.Unique ?? false) ? "UNIQUE " : "")}INDEX [{indexName}]");
                    query.AppendLine("ON");
                    query.AppendLine(
                        $"    [dbo].[{tableName}](${string.Join(",", index.Columns.Select(x => $"[{x.Key}] {x.Value}"))});");
                }

                // drop index
                foreach (var indexName in table.DeletedIndexNames)
                {
                    if (!droppedIndexNames.Contains(indexName))
                    {
                        query.AppendLine($"DROP INDEX [{tableName}].[{indexName}];");
                    }
                }

            }

            // drop tables
            if (dropTable)
            {
                foreach (var tableName in diff.DeletedTableNames)
                {
                    query.AppendLine($"DROP TABLE [dbo].[{tableName}];");
                }
            }

            var result = string.Join("\n", dropFkQuery) + "\n" + query.ToString() + "\n" +
                         string.Join("\n", createFkQuery);

            if (query.Length == 0 && !createFkQuery.Any() && !dropFkQuery.Any())
            {
                return null;
            }

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

                    if (column.ForeignKeys?.Any() ?? false)
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
                    query.AppendLine($"CREATE {((index.Unique ?? false) ? "UNIQUE " : "")}INDEX [{name}] ON [dbo].[{tableName}](");
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
        private DataTable GetResult(string query, SqlTransaction trn = null, params SqlParameter[] parameters)
        {
            var res = new DataTable();
            using (var cmd = trn == null ? _sqlConnection.CreateCommand() : new SqlCommand("", trn.Connection, trn))
            {
                cmd.CommandText = query;
                if (parameters.Any())
                {
                    cmd.Parameters.AddRange(parameters);
                }

                using (var da = new SqlDataAdapter(cmd))
                {
                    da.Fill(res);
                }
            }

            return res;
        }

    }
}