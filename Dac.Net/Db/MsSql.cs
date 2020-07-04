using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Dac.Net.Core;
using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic;

namespace Dac.Net.Db
{
    public class MsSql : IDb
    {
        private readonly Server _server;
        private SqlConnection _sqlConnection;
        private bool _dryRun = false;

        public MsSql(Server server, bool dryRun)
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

            const string query = @"
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
                var kfName = row.Field<string>("fk_name");
                if (db.Tables.ContainsKey(tableName) && !string.IsNullOrWhiteSpace(kfName))
                {
                    queries.AppendLine(
                        $"ALTER TABLE [{tableName}] DROP CONSTRAINT [{kfName}];");
                }
            }


            foreach (var (tableName, table) in db.Tables)
            {
                queries.AppendLine($"DROP TABLE IF EXISTS [{tableName}];");
            }

            foreach (var (viewName, view) in db.Views ?? new Dictionary<string, string>())
            {
                queries.AppendLine($"DROP VIEW IF EXISTS [{viewName}];");
            }
            
            foreach (var (synonymName, synonym) in db.Synonyms ?? new Dictionary<string, Synonym>())
            {
                queries.AppendLine($"DROP SYNONYM IF EXISTS [{synonymName}];");
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

            var columns = new Dictionary<string, Dictionary<string, Column>>();
            var indexes = new Dictionary<string, Dictionary<string, Index>>();
            var pk = new Dictionary<string, List<string>>();
            var tableIds = new Dictionary<string,string>();

            var query = @"
                SELECT
                    t.name AS table_name,
                    i.name AS index_name,
                    col.name AS column_name,
                    i.is_primary_key,
                    c.is_descending_key,
                    i.is_unique,
                    i.type_desc,
                    sit.tessellation_scheme,
                    sit.level_1_grid_desc,
                    sit.level_2_grid_desc,
                    sit.level_3_grid_desc,
                    sit.level_4_grid_desc,
                    sit.cells_per_object
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
                LEFT OUTER JOIN
                    sys.spatial_index_tessellations as sit
                ON 
                    i.object_id = sit.object_id
                AND
                    i.index_id = sit.index_id
                ORDER BY t.name, i.name, c.key_ordinal
        ";

            foreach (DataRow row in GetResult(query).Rows)
            {
                var tableName = row.Field<string>("table_name");

                if (!pk.ContainsKey(tableName))
                {
                    pk.Add(tableName, new List<string>());
                }

                if (!indexes.ContainsKey(tableName))
                {
                    indexes.Add(tableName, new Dictionary<string, Index>());
                }

                if (row.Field<bool>("is_primary_key"))
                {
                    pk[tableName].Add(row.Field<string>("column_name"));

                }
                else
                {
                    var indexName = row.Field<string>("index_name");
                    if (!indexes[tableName].ContainsKey(indexName))
                    {
                        indexes[tableName].Add(indexName,
                            new Index()
                            {
                                Unique = row.Field<bool>("is_unique"), Type = row.Field<string>("type_desc")
                            });
                    }

                    var type = row.Field<string>("type_desc").ToLower();

                    if (type == "spatial")
                    {
                        indexes[tableName][indexName].Columns[row.Field<string>("column_name")] = "";
                        indexes[tableName][indexName].Spatial = new Spatial()
                        {
                            TessellationSchema = row.Field<string>("tessellation_scheme"),
                            Level1 = row.Field<string>("level_1_grid_desc"),
                            Level2 = row.Field<string>("level_2_grid_desc"),
                            Level3 = row.Field<string>("level_3_grid_desc"),
                            Level4 = row.Field<string>("level_4_grid_desc"),
                            CellsPerObject = row.Field<int>("cells_per_object"),
                        };
                        
                    }
                    else
                    {
                        indexes[tableName][indexName].Columns[row.Field<string>("column_name")] =
                            row.Field<bool>("is_descending_key") ? "desc" : "asc";
                    }
                    
                }
            }

            query = @"
            SELECT
                t.name AS table_name,
                t.object_id AS table_id,
                c.name AS column_name,
                c.column_id AS column_id,
                type.name AS type,
                c.max_length,
                c.is_nullable ,
                c.is_identity,
                type.max_length AS type_max_length,
                c.precision,
                c.scale
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
                if (!tableIds.ContainsKey(tableName))
                {
                    tableIds.Add(tableName, Convert.ToString(row.Field<int>("table_id")));
                }
                if (!columns.ContainsKey(tableName))
                {
                    columns.Add(tableName, new Dictionary<string, Column>());
                }

                var length = row.Field<short>("max_length");
                var lengthString = length == row.Field<short>("type_max_length") ? "" : length.ToString();
                var type = row.Field<string>("type");
                switch (type)
                {
                    case "nvarchar":
                    case "nchar":
                        if (length > 0)
                        {
                            length /= 2;
                            lengthString = length.ToString();
                        }
                        else
                        {
                            lengthString = "max";
                        }

                        break;
                    case "varbinary":
                        if (length < 0)
                        {
                            lengthString = "max";
                        }
                        break;
                    case "int":
                    case "datetime":
                        lengthString = "0";
                        break;
                    case "numeric":
                        lengthString = $"{row.Field<byte>("precision")},{row.Field<byte>("scale")}";
                        break;
                }

                columns[tableName].Add(
                    row.Field<string>("column_name"),
                    new Column()
                    {
                        ColumnId = Convert.ToString(row.Field<int>("column_id")),
                        Id = row.Field<bool>("is_identity"),
                        Type = row.Field<string>("type"),
                        Length = lengthString,
                        NotNull = !row.Field<bool>("is_nullable"),
                        Pk = pk.ContainsKey(tableName) && pk[tableName].Contains(row.Field<string>("column_name"))
                    }
                );
            }

            foreach (var tableName in columns.Keys)
            {
                tables.Add(tableName, new Table()
                {
                    TableId = tableIds[tableName],
                    Columns = columns.ContainsKey(tableName) ? columns[tableName] : new Dictionary<string, Column>(),
                    Indexes = indexes.ContainsKey(tableName) ? indexes[tableName] : new Dictionary<string, Index>()
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
            
            // synonyms
            var synonyms = new Dictionary<string, Synonym>();
            query = @"
                    SELECT
                        name,
                        base_object_name
                    FROM
                        sys.synonyms
            ";
            foreach (DataRow row in GetResult(query).Rows)
            {
                
                var tmp = row.Field<string>("base_object_name").Split(".");
                if (tmp.Length == 3)
                {
                    var synonym = new Synonym
                    {
                        Database = tmp[0].Replace("[", "").Replace("]", ""),
                        Schema = tmp[1].Replace("[", "").Replace("]", ""),
                        Object = tmp[2].Replace("[", "").Replace("]", "")
                    };
                    synonyms.Add(row.Field<string>("name"), synonym);
                    
                }
                
            }
            
            // views
            var views = new Dictionary<string, string>();
            query = @"
                SELECT 
                    v.name,
                    m.definition
                FROM
                    sys.views AS v
                INNER JOIN 
                    sys.sql_modules AS m
                ON  
                    v.object_id = m.object_id";
            foreach (DataRow row in GetResult(query).Rows)
            {
                
                var definition = row.Field<string>("definition");
                var tmp = Regex.Split(definition, "as|AS");
                if (tmp.Length > 1)
                {
                    definition = string. Join("", tmp.Where((x,i) => i > 0  ));
                }

                definition = Utility.TrimQuery(definition);
                views.Add(row.Field<string>("name"), definition.Trim());
            }

            var db = new DataBase() {Tables = tables, Synonyms = synonyms, Views = views };
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
            var queryResult = new QueryResult();
            var queries = new StringBuilder();
            queries.AppendLine($"USE [{_server.Database}];");
            queries.AppendLine(CreateQuery(db));
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
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <returns></returns>
        public QueryResult ReCreate(DataBase db, bool queryOnly)
        {
            var queryResult = new QueryResult();
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
                queries.AppendLine($"ALTER TABLE [{tableName}] DROP CONSTRAINT [{fkName}];");
            }

            // drop exist tables
            foreach (var tableName in tables)
            {
                queries.AppendLine($"DROP TABLE [{tableName}];");
            }
            
            // drop synonyms
            query = @"
                    SELECT
                        name
                    FROM
                        sys.synonyms
            ";
            foreach (DataRow row in GetResult(query).Rows)
            {
                queries.AppendLine($"DROP SYNONYM [{row.Field<string>("name")}];");
            }
            
            // drop views
            query = @"
                    SELECT
                        name
                    FROM
                        sys.views
            ";
            foreach (DataRow row in GetResult(query).Rows)
            {
                queries.AppendLine($"DROP VIEW [{row.Field<string>("name")}];");
            }
            
            queries.AppendLine(CreateQuery(db));

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
        /// <param name="db"></param>
        /// <param name="queryOnly"></param>
        /// <param name="dropTable"></param>
        /// <returns></returns>
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

            var droppedIndexNames = new List<string>();

            // add tables
            if (diff.AddedTables.Any())
            {
                query.AppendLine(CreateQuery(new DataBase() {Tables = diff.AddedTables}));
            }

            foreach (var (tableName, table) in diff.ModifiedTables)
            {
                // rename
                var (currentTableName, newTableName) = table.Name;
                if (currentTableName != newTableName)
                {
                    query.AppendLine($"EXEC sp_rename '{currentTableName}', {newTableName}, 'OBJECT';");
                }
                else
                {
                    currentTableName = tableName;
                }
                
                var orgTable = diff.CurrentDb.Tables[!string.IsNullOrWhiteSpace(currentTableName) ? currentTableName : tableName];
                
                

                // add columns
                foreach (var (columnName, column) in table.AddedColumns)
                {
                    var type = (column.Id ?? false) ? "int" : column.Type;
                    if (column.LengthInt > 0 && !string.IsNullOrWhiteSpace(column.Length))
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
                    
                    // rename
                    if (orgColumn.Name != newColumn.Name)
                    {
                        query.AppendLine($"EXEC sp_rename '{tableName}.{orgColumn.Name}', {newColumn.Name}, 'COLUMN';");
                    }

                    // if change execute alter
                    foreach (var (indexName, index ) in (orgTable.Indexes ?? new Dictionary<string, Index>()).Where(x =>
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
                    if (newColumn.LengthInt > 0 && !string.IsNullOrWhiteSpace(newColumn.Length))
                    {
                        type += $"({newColumn.Length})";
                    }

                    var check = !string.IsNullOrWhiteSpace(newColumn.Check) ? $" CHECK({newColumn.Check}) " : "";
                    var def = !string.IsNullOrWhiteSpace(newColumn.Default) ? $" DEFAULT {newColumn.Default} " : "";

                    if (orgColumn.Type != newColumn.Type || orgColumn.Id != newColumn.Id ||
                        orgColumn.Length != newColumn.Length || orgColumn.NotNull != newColumn.NotNull)
                    {
                        query.AppendLine(
                            $"ALTER TABLE [{tableName}] ALTER COLUMN [{columnName}] {type}{((newColumn.Id ?? false) ? " IDENTITY" : "")}{((newColumn.NotNull ?? false) ? " NOT NULL" : "")};");
                    }

                    if (orgColumn.Default != newColumn.Default)
                    {
                        if (!string.IsNullOrWhiteSpace(orgColumn.DefaultName))
                        {
                            query.AppendLine($"ALTER TABLE [{tableName}] DROP CONSTRAINT [{orgColumn.DefaultName}];");
                        }

                        if (!string.IsNullOrWhiteSpace(newColumn.Default))
                        {
                            query.AppendLine($"ALTER TABLE [{tableName}] ADD DEFAULT {newColumn.Default} FOR [{columnName}];");
                        }
                    }

                    if (orgColumn.Check != newColumn.Check)
                    {
                        // drop old check
                        if (!string.IsNullOrWhiteSpace(orgColumn.CheckName))
                        {
                            query.AppendLine($"ALTER TABLE [{tableName}] DROP CONSTRAINT [{orgColumn.CheckName}];");
                        }

                        // add new check
                        if (!string.IsNullOrWhiteSpace(newColumn.Check))
                        {
                            query.AppendLine($"ALTER TABLE [{tableName}] ADD CHECK({newColumn.Check});");
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

                            dropFkQuery.Add($"ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [{fkName}];");
                            continue;
                        }

                        if ((orgFk[fkName].Update != newFk[fkName].Update) ||
                            (orgFk[fkName].Delete != newFk[fkName].Delete) ||
                            (orgFk[fkName].Table != newFk[fkName].Table) ||
                            (orgFk[fkName].Column != newFk[fkName].Column))
                        {

                            dropFkQuery.Add($"ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [{fkName}];");

                            var fk = newFk[fkName];
                            createFkQuery.Add(CreateAlterForeignKey(fkName, tableName, columnName, fk.Table, fk.Column,
                                fk.Update, fk.Delete));
                        }
                    }
                }

                // drop columns
                foreach (var columnName in table.DeletedColumnName)
                {
                    if (!string.IsNullOrWhiteSpace(diff.CurrentDb.Tables[currentTableName].Columns[columnName].DefaultName))
                    {
                        query.AppendLine($"ALTER TABLE [{tableName}] DROP CONSTRAINT [{diff.CurrentDb.Tables[tableName].Columns[columnName].DefaultName}];");
                    }

                    query.AppendLine($"ALTER TABLE [{tableName}] DROP COLUMN [{columnName}];");

                }

                // create index
                foreach (var (indexName, index) in table.AddedIndexes)
                {
                    query.AppendLine(IndexQuery(tableName, indexName, index));
                }

                // modify index
                foreach (var (indexName, indices) in table.ModifiedIndexes)
                {
                    var index = indices[1];
                    if (!droppedIndexNames.Contains(indexName))
                    {
                        query.AppendLine($"DROP INDEX [{indexName}] ON [{tableName}];");
                    }
                    query.AppendLine(IndexQuery(tableName, indexName, index));
                }

                // drop index
                foreach (var indexName in table.DeletedIndexNames)
                {
                    if (!droppedIndexNames.Contains(indexName))
                    {
                        query.AppendLine($"DROP INDEX [{indexName}] ON [{tableName}];");
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
            
            // add synonym
            query.AppendLine(CreateQuery(new DataBase() {Synonyms = diff.AddedSynonyms}));
            
            // drop synonyms
            foreach (var synonymName in diff.DeletedSynonymNames)
            {
                query.AppendLine($"DROP SYNONYM [{synonymName}];");
            }
            
            // modified synonyms
            foreach (var (synonymName, synonyms) in diff.ModifiedSynonyms)
            {
                query.AppendLine($"DROP SYNONYM [{synonymName}];");
                query.AppendLine(CreateQuery(new DataBase()
                    {Synonyms = new Dictionary<string, Synonym>() {{synonymName, synonyms[1]}}}));
            }

            queryResult.Query = string.Join("\n", dropFkQuery) + "\n" + query.ToString() + "\n" +
                         string.Join("\n", createFkQuery);

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

            foreach (var (tableName, table) in (db.Tables ?? new Dictionary<string, Table>()))
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
                    var type = column.Type + ((column.LengthInt > 0 && !string.IsNullOrWhiteSpace(column.Length)) ? $"({column.Length})" : "");

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
                foreach (var (indexName, index) in (table.Indexes ?? new Dictionary<string, Index>()))
                {
                    var name = !string.IsNullOrWhiteSpace(indexName) ? indexName : $"INDEX_{tableName}_${num++}";
                    query.AppendLine(IndexQuery(tableName, name, index));
                }

            }
            
            var viewQuery = new StringBuilder();
            if (db.Views?.Any() ?? false)
            {
                
                foreach (var (viewName, define) in db.Views)
                {
                    viewQuery.AppendLine("GO");
                    viewQuery.AppendLine($"CREATE VIEW [{viewName}]");
                    viewQuery.AppendLine("AS");
                    viewQuery.AppendLine($"{Utility.TrimQuery(define)};");
                }
            }

            var synonymQuery = new StringBuilder();
            foreach (var (synonymName, synonym) in (db.Synonyms ?? new Dictionary<string, Synonym>()))
            {
                var objectName = $"{((!string.IsNullOrWhiteSpace(synonym.Database) ? $"[{synonym.Database}]." : ""))}" +
                                 $"{((!string.IsNullOrWhiteSpace(synonym.Schema) ? $"[{synonym.Schema}]." : ""))}" +
                                 $"[{synonym.Object}]";
                synonymQuery.AppendLine($"CREATE SYNONYM [{synonymName}] FOR {objectName};");
            }

            return $"{query}\n{fkQuery}\n{synonymQuery}\n{viewQuery}";
            
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

        private string IndexQuery(string tableName, string indexName, Index index)
        {
            var query = new StringBuilder();
            query.AppendLine(
                $"CREATE {((index.Unique ?? false) ? "UNIQUE " : "")}{(!string.IsNullOrWhiteSpace(index.Type) ? index.Type + " " : "")}INDEX [{indexName}] ON [{tableName}]");
            query.AppendLine("(");
            query.AppendLine($"    {string.Join(",", index.Columns.Select(x => $"[{x.Key}] {x.Value}"))}");
            query.AppendLine(")");
            if (index.Spatial != null)
            {
                query.AppendLine($"USING {index.Spatial.TessellationSchema}");
                var with = new List<string>();
                if (!string.IsNullOrWhiteSpace(index.Spatial.Level1) ||
                    !string.IsNullOrWhiteSpace(index.Spatial.Level2) ||
                    !string.IsNullOrWhiteSpace(index.Spatial.Level3) ||
                    !string.IsNullOrWhiteSpace(index.Spatial.Level4) || index.Spatial.CellsPerObject != null)
                {
                    var grids = new List<string>();
                    if(!string.IsNullOrWhiteSpace(index.Spatial.Level1))
                    {
                        grids.Add($"LEVEL_1 = {index.Spatial.Level1}");
                    }
                    if(!string.IsNullOrWhiteSpace(index.Spatial.Level2))
                    {
                        grids.Add($"LEVEL_2 = {index.Spatial.Level2}");
                    }
                    if(!string.IsNullOrWhiteSpace(index.Spatial.Level3))
                    {
                        grids.Add($"LEVEL_3 = {index.Spatial.Level3}");
                    }
                    if(!string.IsNullOrWhiteSpace(index.Spatial.Level4))
                    {
                        grids.Add($"LEVEL_4 = {index.Spatial.Level4}");
                    }

                    if (grids.Any())
                    {
                        with.Add($"GRIDS =({ string.Join(",", grids)})");
                    }
          
                }

                if (index.Spatial.CellsPerObject != null)
                {
                    with.Add($"CELLS_PER_OBJECT = {index.Spatial.CellsPerObject}");
                }

                if (with.Any())
                {
                    query.AppendLine($"WITH({string.Join(",", with)})");
                }
            }

            return $"{query};";
            
          //  return
          //      $"CREATE {((index.Unique ?? false) ? "UNIQUE " : "")}{(!string.IsNullOrWhiteSpace(index.Type) ? index.Type + " " : "")}INDEX [{indexName}] ON [{tableName}]({string.Join(",", index.Columns.Select(x => $"[{x.Key}] {x.Value}"))});";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="queryResult"></param>
        private void Transaction(QueryResult queryResult)
        {
            using (var trn = _sqlConnection.BeginTransaction())
            {
                try
                {
                    var reader = new StringReader(queryResult.Query);
                    var bat = new StringBuilder();
                    while (reader.Peek() > -1)
                    {
                        var line = (reader.ReadLine() ?? "").Trim();
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            continue;
                        }

                        if (line.ToLower() != "go")
                        {
                            bat.AppendLine(line);
                            continue;
                        }

                        using (var cmd = new SqlCommand(bat.ToString(), trn.Connection, trn))
                        {
                            cmd.ExecuteNonQuery();
                        }

                        bat.Clear();
                    }

                    if (bat.Length > 0)
                    {
                        using (var cmd = new SqlCommand(bat.ToString(), trn.Connection, trn))
                        {
                            cmd.ExecuteNonQuery();
                        }
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