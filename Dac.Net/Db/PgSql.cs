using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Dac.Net.Core;
using Npgsql;

namespace Dac.Net.Db
{
    public class PgSql : IDb
    {

        private readonly Server _server;
        private NpgsqlConnection _npgsqlConnection;
        private readonly bool _dryRun = false;

        public PgSql(Server server, bool dryRun)
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
            foreach (var (tableName, table) in db.Tables)
            {
                queries.AppendLine($"DROP TABLE IF EXISTS \"{tableName}\" CASCADE;");
            }
            foreach (var (viewName, definition) in db.Views)
            {
                queries.AppendLine($"DROP VIEW IF EXISTS \"{viewName}\";");
            }

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

            foreach (DataRow row in GetResult("SELECT relname FROM \"pg_stat_user_tables\" WHERE schemaname='public'")
                .Rows)
            {
                tables.Add(row.Field<string>("relname"), new Table());

            }

            // get sequence list
            var seqData = GetResult("SELECT sequence_name FROM information_schema.sequences");
            var sequences = seqData.AsEnumerable().Select(row => row.Field<string>("sequence_name")).ToList();


            foreach (var (tableName, table) in tables)
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

                foreach (DataRow row in GetResult(query, null, new NpgsqlParameter("table_name", tableName)).Rows)
                {
                    var id = sequences.Any(seq => (row.Field<string>("column_default") ?? "").Contains(seq));
                    var type = id ? "serial" : row.Field<string>("data_type");
                    var length = row.Field<int?>("character_maximum_length") ?? 0;

                    type = Define.ColumnType.PgSql.ContainsKey(type) ? Define.ColumnType.PgSql[type] : type;

                    var column = new Column()
                    {
                        Type = type,
                        Id = id,
                        Length = Convert.ToString(length),
                        NotNull = row.Field<string>("is_nullable") == "NO"
                    };
                    if (!string.IsNullOrWhiteSpace(row.Field<string>("column_default")) && !id)
                    {
                        column.Default = row.Field<string>("column_default");
                    }

                    tables[tableName].Columns.Add(row.Field<string>("column_name"), column);
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

                foreach (DataRow row in GetResult(query, null, new NpgsqlParameter("table_catalog", _server.Database),
                    new NpgsqlParameter("table_name", tableName)).Rows)
                {
                    var columnName = row.Field<string>("column_name");
                    if (tables[tableName].Columns.ContainsKey(columnName))
                    {
                        tables[tableName].Columns[columnName].Pk = true;
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

                foreach (DataRow row in GetResult(query, null, new NpgsqlParameter("table_name", tableName)).Rows)
                {
                    var indexDef = row.Field<string>("indexdef");
                    var indexName = row.Field<string>("indexname");
                    if (!tables[tableName].Indices.ContainsKey(indexName))
                    {
                        tables[tableName].Indices.Add(indexName, new Index()
                        {
                            Unique = indexDef.Contains("UNIQUE INDEX")
                        });

                    }

                    var m = Regex.Match(indexDef, @"\((.*)\)");
                    if (m.Success)
                    {
                        foreach (var col in m.Groups[1].Value.Split(",")) {
                            var tmp = col.Trim().Split(" ");
                            if (table.Columns.ContainsKey(tmp[0])) {
                                table.Indices[indexName].Columns.Add(tmp[0], tmp.Length > 1 ? tmp[1] : "ASC");
                            }
                        }
                    }

/*
                const m = (indexdef.match(/\(.*\)/) || [])[0];
                if (!m) {
                    continue;
                }
                for (const col of m.replace('(', '').replace(')', '').split(',')) {
                    const tmp = col.trim().split(' ');
                    if (tables[tableName].columns[tmp[0]]) {
                        tables[tableName].indexes[indexName].columns[tmp[0]] = tmp.Length > 1 ? tmp[1] : 'ASC';
                    }
                }*/
                }

                // remove primarykey index
                var pkColumns = new List<string>();
                foreach (var (columnName, column) in table.Columns)
                {
                    if (column.Pk ?? false)
                    {
                        pkColumns.Add(columnName);
                    }
                }

                foreach (var (indexName, index) in table.Indices)
                {
                    var columns = new List<string>();
                    foreach (var (columnName, column) in index.Columns)
                    {
                        columns.Add(columnName);
                    }

                    if (string.Join("__", columns) == string.Join("__", pkColumns))
                    {
                        table.Indices.Remove(indexName);
                    }
                }

                // get check list
                query = @"
                        SELECT
                            pg_get_constraintdef(co.oid) as def,
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

                foreach (DataRow row in GetResult(query, null, new NpgsqlParameter("relname", tableName)).Rows)
                {
                    var m = Regex.Match(row.Field<string>("def"), @"\((.*)\)");
                    if (!m.Success)
                    {
                        continue;
                    }
                    var check = m.Groups[1].Value;
                    foreach (var (columnName, column) in table.Columns)
                    {
                        if (!check.Contains(columnName))
                        {
                            continue;
                        }
                        column.Check = check;
                        column.CheckName = row.Field<string>("conname");
                        break;
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
                var fkData = GetResult(query, null, new NpgsqlParameter("table_name", tableName));
                var conf = new Dictionary<char, string>
                {
                    {'a', ""},
                    {'r', "RESTRICT"},
                    {'c', "CASCADE"},
                    {'n', "SET NULL"},
                    {'d', "SET DEFAULT"}
                };
                foreach (DataRow row in fkData.Rows)
                {

                    var columnName = row.Field<string>("column_name");
                    if (!tables[tableName].Columns.ContainsKey(columnName))
                    {
                        continue;
                    }

                    var update = conf.ContainsKey(row.Field<char>("confupdtype"))
                        ? conf[row.Field<char>("confupdtype")]
                        : "";
                    var del = conf.ContainsKey(row.Field<char>("confdeltype"))
                        ? conf[row.Field<char>("confdeltype")]
                        : "";

                    var key = row.Field<string>("foreign_table_name") + '.' + row.Field<string>("foreign_column_name");
                    tables[tableName].Columns[columnName].ForeignKeys.Add(row.Field<string>("constraint_name"),
                        new ForeignKey()
                        {
                            Table = row.Field<string>("foreign_table_name"),
                            Column = row.Field<string>("foreign_column_name"),
                            Update = update,
                            Delete = del
                        });
                }
            }
            
            // views
            var views = new Dictionary<string, string>();
            foreach (DataRow row in GetResult(
                "SELECT viewname, definition FROM pg_views WHERE schemaname = 'public' AND viewowner = @database", null,
                new NpgsqlParameter("database", _server.Database)).Rows)
            {
                views.Add(row.Field<string>("viewname"), row.Field<string>("definition"));
            }

            var db = new DataBase() {Tables = tables, Views = views};
            Utility.TrimDataBaseProperties(db);
            return db;
        }

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
            var queryResult = new QueryResult()
            {
                Query = CreateQuery(db)
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
            
            var query = new StringBuilder();
            var tables = new Dictionary<string, Table>();
            foreach (DataRow row in GetResult("SELECT relname FROM \"pg_stat_user_tables\" WHERE schemaname='public'")
                .Rows)
            {
                query.AppendLine($"DROP TABLE \"{row.Field<string>("relname")}\" CASCADE;");
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

            var query = new StringBuilder();
            var createFkQuery = new List<string>();
            var dropFkQuery = new List<string>();


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
                    var type = (column.Id ?? false) ? "serial" : column.Type;
                    if (column.LengthInt > 0)
                    {
                        type += $"({column.Length})";
                    }

                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $") CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";

                    query.AppendLine(
                        $"ALTER TABLE \"{tableName}\" ADD COLUMN \"{columnName}\" {type}{((column.NotNull ?? false) ? " NOT NULL" : "")}{check}{def};");

                    foreach (var (fkName, fk) in column.ForeignKeys)
                    {
                        createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                            fk.Update, fk.Delete));
                    }
                }

                // modify columns
                foreach (var (columnName, column) in table.ModifiedColumns)
                {
                    var orgColumn = column[0];
                    var newColumn = column[1];

                    // change type
                    if (orgColumn.Type != newColumn.Type || orgColumn.Length != newColumn.Length)
                    {
                        var type = (newColumn.Id ?? false) ? "serial" : newColumn.Type;
                        if (newColumn.LengthInt > 0)
                        {
                            type += $"({newColumn.Length})";
                        }

                        query.AppendLine($"ALTER TABLE \"{tableName}\" ALTER COLUMN \"{columnName}\" TYPE {type};");
                    }

                    // not null
                    if (!(newColumn.Pk ?? false) && orgColumn.NotNull != newColumn.NotNull)
                    {
                        query.AppendLine(
                            $"ALTER TABLE \"${tableName}\" ALTER COLUMN \"{columnName}\" {((newColumn.NotNull ?? false) ? "SET NOT NULL" : "DROP NOT NULL")};");
                    }

                    // default
                    if ((orgColumn.Default ?? "").ToLower() != (newColumn.Default ?? "").ToLower())
                    {
                        if (!string.IsNullOrWhiteSpace(newColumn.Default))
                        {
                            query.AppendLine(
                                $"ALTER TABLE \"{tableName}\" ALTER COLUMN \"{columnName}\" SET DEFAULT {newColumn.Default};");
                        }
                        else
                        {
                            query.AppendLine($"ALTER TABLE \"{tableName}\" ALTER COLUMN \"{columnName}\" DROP DEFAULT");
                        }
                    }

                    if (orgColumn.Check != newColumn.Check)
                    {
                        // drop old check
                        if (!string.IsNullOrWhiteSpace(orgColumn.DefaultName))
                        {
                            query.AppendLine(
                                $"ALTER TABLE \"{tableName}\" DROP CONSTRAINT \"{orgColumn.DefaultName}\";");
                        }

                        // add new check
                        if (!string.IsNullOrWhiteSpace(newColumn.Check))
                        {
                            query.AppendLine($"ALTER TABLE \"{tableName}\" ADD CHECK({newColumn.Check});");
                        }
                    }

                    // foreign key
                    var orgFk = orgColumn.ForeignKeys ?? new Dictionary<string, ForeignKey>();
                    var newFk = newColumn.ForeignKeys ?? new Dictionary<string, ForeignKey>();
                    foreach (var fkName in orgFk.Keys.Concat(newFk.Keys).Distinct())
                    {
                        if (!orgFk.ContainsKey(fkName))
                        {
                            var fk = newFk[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                            continue;
                        }

                        if (!newFk.ContainsKey(fkName))
                        {

                            dropFkQuery.Add($"ALTER TABLE \"{tableName}\" DROP CONSTRAINT \"{fkName}\";");
                            continue;
                        }

                        if ((orgFk[fkName].Update != newFk[fkName].Update) ||
                            (orgFk[fkName].Delete != newFk[fkName].Delete) ||
                            (orgFk[fkName].Table != newFk[fkName].Table) ||
                            (orgFk[fkName].Column != newFk[fkName].Column))
                        {

                            dropFkQuery.Add($"ALTER TABLE \"{tableName}\" DROP CONSTRAINT \"{fkName}\";");

                            var fk = newFk[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                        }
                    }
                }

                // drop columns
                foreach (var columnName in table.DeletedColumnName)
                {
                    query.AppendLine($"ALTER TABLE \"{tableName}\" DROP COLUMN \"{columnName}\";");
                }

                // create index
                foreach (var (indexName, index) in table.AddedIndices)
                {
                    query.AppendLine(
                        $"CREATE {((index.Unique ?? false) ? "UNIQUE " : "")}INDEX \"{indexName}\" ON \"{tableName}\" ({string.Join(",", index.Columns.Select(x => $"\"{x}\""))});");
                }

                // modify index
                foreach (var (indexName, index) in table.ModifiedIndices)
                {

                    query.AppendLine($"DROP INDEX \"{indexName}\";");
                    query.AppendLine(
                        $"CREATE {((index[1].Unique ?? false) ? "UNIQUE " : "")}INDEX \"{indexName}\" ON \"{tableName}\" ({string.Join(",", index[1].Columns.Select(x => $"\"{x}\""))});");
                }

                // drop index
                foreach (var indexName in table.DeletedIndexNames)
                {
                    query.AppendLine($"DROP INDEX \"{indexName}\";");
                }

            }

            // drop table
            if (dropTable)
            {
                foreach (var tableName in diff.DeletedTableNames)
                {
                    query.AppendLine($"DROP TABLE \"{tableName}\" CASCADE;");
                }
            }
            
            // views
            var viewQuery = new StringBuilder();
            if (diff.AddedViews.Any())
            {
                viewQuery.AppendLine(CreateQuery(new DataBase() {Views = diff.AddedViews}));
            }

            foreach(var viewName in diff.DeletedViewNames)
            {
                viewQuery.AppendLine($"DROP VIEW \"{viewName}\"");
            }

            foreach (var (viewName, definition) in diff.ModifiedViews)
            {
                viewQuery.AppendLine($"DROP VIEW \"{viewName}\";");

                viewQuery.AppendLine(CreateQuery(new DataBase()
                    {Views = new Dictionary<string, string>() {{viewName, definition[1]}}}));
            }


            queryResult.Query = string.Join("\n", dropFkQuery) + '\n' + query + '\n' +
                                string.Join("\n", createFkQuery) + '\n' + viewQuery;
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
            return new Diff(Extract(), db);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Connect()
        {
            try
            {
                var connectionString = new NpgsqlConnectionStringBuilder()
                {
                    Host = _server.Host,
                    Username = _server.User,
                    Password = _server.Password,
                    Database = _server.Database

                };
                if (_server.Port != null)
                {
                    connectionString.Port = (int) _server.Port;
                }

                _npgsqlConnection = new NpgsqlConnection(connectionString.ToString());
                _npgsqlConnection.Open();

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
                _npgsqlConnection.Close();
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
        private DataTable GetResult(string query, NpgsqlTransaction trn = null, params NpgsqlParameter[] parameters)
        {
            var res = new DataTable();
            using (var cmd =
                trn == null ? _npgsqlConnection.CreateCommand() : new NpgsqlCommand("", trn.Connection, trn))
            {
                cmd.CommandText = query;
                if (parameters.Any())
                {
                    cmd.Parameters.AddRange(parameters);
                }

                using (var da = new NpgsqlDataAdapter(cmd))
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

                query.AppendLine($"CREATE TABLE {tableName}(");

                var columnQuery = new List<string>();
                var pk = new List<string>();
                foreach (var (columnName, column) in table.Columns)
                {
                    if (column.Id ?? false)
                    {
                        column.NotNull = true;
                        column.Type = "serial";
                    }

                    var notNull = (column.NotNull ?? false) ? " NOT NULL " : "";
                    var check = !string.IsNullOrWhiteSpace(column.Check) ? $" CHECK({column.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(column.Default) ? $" DEFAULT {column.Default} " : "";
                    var type = column.Type + (column.LengthInt > 0 && !string.IsNullOrWhiteSpace(column.Length) ? $"({column.Length})" : "");

                    columnQuery.Add($"    \"{columnName}\" {type}{notNull}{check}{def}");
                    if ((column.Pk ?? false) || (column.Id ?? false))
                    {
                        pk.Add(columnName);
                    }
                }

                query.AppendLine(string.Join(",\n", columnQuery) + (pk.Any() ? "," : ""));

                if (pk.Any())
                {
                    query.AppendLine($"    CONSTRAINT \"PK_{tableName}\" PRIMARY KEY ");
                    query.AppendLine("    (");
                    query.AppendLine(string.Join(",\n", pk.Select(x => $"        {x}")));
                    query.AppendLine("    )");
                }

                query.AppendLine(");");

                foreach (var (indexName, index) in (table.Indices ?? new Dictionary<string, Index>()))
                {
                    query.AppendLine(
                        $"CREATE {((index.Unique ?? false) ? "UNIQUE " : "")}INDEX \"{indexName}\" ON \"{tableName}\"(");
                    query.AppendLine($"    {string.Join(",", index.Columns.Select(x => $"{x.Key} {x.Value}"))}");
                    query.AppendLine(");");
                }

            }

            // foreign key
            if (db.Tables.Any())
            {
                foreach (var (tableName, table) in db.Tables)
                {
                    foreach (var (columnName, column) in table.Columns.Where(c => c.Value.ForeignKeys?.Any() ?? false))
                    {
                        foreach (var (fkName, fk) in column.ForeignKeys)
                        {
                            query.AppendLine(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                        }
                    }
                }
            }

            // views
            foreach (var (viewName, definition) in db.Views ?? new Dictionary<string, string>())
            {
                query.AppendLine($"CREATE VIEW \"{viewName}\" AS {definition};");
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

            return
                $"ALTER TABLE \"{table}\" ADD CONSTRAINT \"{name}\" FOREIGN KEY (\"{column}\") REFERENCES \"{targetTable}\"(\"{targetColumn}\"){onupdate}{ondelete};";
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="queryResult"></param>
        private void Transaction(QueryResult queryResult)
        {
            using (var trn = _npgsqlConnection.BeginTransaction())
            {
                try
                {
                    using (var cmd = new NpgsqlCommand(queryResult.Query, trn.Connection, trn))
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