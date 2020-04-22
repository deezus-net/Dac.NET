using System;
using System.IO;
using System.Linq;
using Dac.Net.Db;

namespace Dac.Net.Core
{
    public class Main
    {
        private readonly CommandLine _commandLine;
        public Action<string> OutPut { get; set; }

        public Main(params string[] args)
        {
            _commandLine = new CommandLine(args);
        }

        public void Run()
        {
            if (!_commandLine.Check())
            {
                OutPut?.Invoke(_commandLine.ErrorMessage);
                return;
            }
            
            foreach (var server in _commandLine.Servers)
            {
                IDb db = null;
                switch (server.Type)
                {
                    case Define.DatabaseType.Mysql:
                        db = new Db.MySql(server, _commandLine.DryRun);
                        break;
                    case Define.DatabaseType.Postgres:
                        db = new PgSql(server, _commandLine.DryRun);
                        break;
                    case Define.DatabaseType.MsSql:
                        db = new MsSql(server, _commandLine.DryRun);
                        break;
                }

                switch (_commandLine.Command)
                {
                    case Define.Command.Create:
                        Create(db);
                        break;
                    case Define.Command.Diff:
                        Diff(db);
                        break;
                    case Define.Command.Drop:
                        Drop(db);
                        break;
                    case Define.Command.Extract:
                        Extract(server.Name, db);
                        break;
                    case Define.Command.Query:
                        Query(db);
                        break;
                    case Define.Command.Update:
                        Update(db);
                        break;
                    case Define.Command.ReCreate:
                        ReCreate(db);
                        break;

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Create(IDb db)
        {
            db?.Connect();
            var result = db?.Create(_commandLine.DataBase, _commandLine.Query);
            db?.Close();
            
            if (result.Success)
            {
                if(_commandLine.Query)
                {
                    OutPut?.Invoke($"{result.Query}");
                }
                else if(_commandLine.DryRun)
                {
                    OutPut?.Invoke("create is success (dry run)");
                }
                else
                {
                    OutPut?.Invoke("create is success");
                }
            }
            else
            {
                OutPut?.Invoke("create is failed");
                OutPut?.Invoke($"{result.Exception.Message}");
                OutPut?.Invoke("-------------------------");
                OutPut?.Invoke($"{result.Query}");
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Diff(IDb db)
        {
            db?.Connect();
            var diff = db?.Diff(_commandLine.DataBase);
            db?.Close();

            if (!diff.HasDiff)
            {
                OutPut?.Invoke("no difference");
                return;
            }

            foreach (var (tableName, table) in diff.AddedTables)
            {
                //  console.log(`${ConsoleColor.fgCyan}%s${ConsoleColor.reset}`, `+ ${tableName}`);
                OutPut?.Invoke($"+ {tableName}");
            }

            foreach (var tableName in diff.DeletedTableNames)
            {
                OutPut?.Invoke($"- {tableName}");
                //    console.log(`${ConsoleColor.fgRed}%s${ConsoleColor.reset}`, `- ${tableName}`);
            }

            foreach (var (tableName, table) in diff.ModifiedTables)
            {
                OutPut?.Invoke($"# {tableName}");
                // console.log(`${ConsoleColor.fgGreen}%s${ConsoleColor.reset}`, `# ${tableName}`);

                foreach (var (columnName, column) in table.AddedColumns)
                {
                    OutPut?.Invoke($"  + {columnName}");
                    //   console.log(`${ConsoleColor.fgCyan}%s${ConsoleColor.reset}`, `  + ${columnName}`);
                }

                foreach (var columnName in table.DeletedColumnName)
                {
                    OutPut?.Invoke($"  - {columnName}");
                    //     console.log(`${ConsoleColor.fgRed}%s${ConsoleColor.reset}`, `  - ${columnName}`);
                }

                foreach (var (columnName, column) in table.ModifiedColumns)
                {
                    var orgColumn = column[0];
                    var newColumn = column[1];
                    OutPut?.Invoke($"  # {columnName}");

                    //console.log(`${ConsoleColor.fgGreen}%s${ConsoleColor.reset}`, `  # ${columnName}`);

                    if (orgColumn.Type != newColumn.Type || orgColumn.Length != newColumn.Length)
                    {
                        var orgType = orgColumn.Type;
                        if (orgColumn.LengthInt > 0 && !string.IsNullOrWhiteSpace(orgColumn.Length))
                        {
                            orgType += $"({orgColumn.Length})";
                        }

                        var newType = newColumn.Type;
                        if (newColumn.LengthInt > 0 && !string.IsNullOrWhiteSpace(newColumn.Length))
                        {
                            newType += $"({newColumn.Length})";
                        }


                        OutPut?.Invoke($"      type: {orgType} -> {newType}");
                        //   console.log(`      type: ${orgColumn.type}${orgColumn.length ? `(${orgColumn.length})` : ``} -> ${column.type}${column.length ? `(${column.length})` : ``}`);
                    }

                    if (orgColumn.Pk != newColumn.Pk)
                    {
                        OutPut?.Invoke($"      pk: {orgColumn.Pk} -> {newColumn.Pk}");
                        //    console.log(`      pk: ${orgColumn.pk} -> ${column.pk}`);
                    }

                    if (orgColumn.NotNull != newColumn.NotNull)
                    {
                        OutPut?.Invoke($"      not null: {orgColumn.NotNull} -> {newColumn.NotNull}");
                        //   console.log(`      not null: ${orgColumn.notNull} -> ${column.notNull}`);
                    }
                }

                foreach (var (indexName, index) in table.AddedIndices)
                {
                    OutPut?.Invoke($"  + {indexName}");
                    // console.log(`${ConsoleColor.fgCyan}%s${ConsoleColor.reset}`, `  + ${indexName}`);
                }

                foreach (var indexName in table.DeletedIndexNames)
                {
                    OutPut?.Invoke($"  - {indexName}");
                    //     console.log(`${ConsoleColor.fgRed}%s${ConsoleColor.reset}`, `  - ${indexName}`);
                }

                foreach (var (indexName, indices) in table.ModifiedIndices)
                {
                    var orgIndex = indices[0];
                    var newIndex = indices[1];

                    OutPut?.Invoke($"  # {indexName}");
                    // console.log(`${ConsoleColor.fgGreen}%s${ConsoleColor.reset}`, `  # ${indexName}`);

                    if (orgIndex.Type != newIndex.Type)
                    {
                        OutPut?.Invoke($"      type: {orgIndex.Type} -> {newIndex.Type}");
                        //   console.log(`      columns: ${orgIndexColumns} -> ${indexColumns}`);
                    }

                    var orgIndexColumns = string.Join(",", orgIndex.Columns.Select(x => $"{x.Key} {x.Value}"));
                    var newIndexColumns = string.Join(",", newIndex.Columns.Select(x => $"{x.Key} {x.Value}"));
                    if (orgIndexColumns != newIndexColumns)
                    {
                        OutPut?.Invoke($"      columns: {orgIndexColumns} -> {newIndexColumns}");
                        //   console.log(`      columns: ${orgIndexColumns} -> ${indexColumns}`);
                    }

                    if (orgIndex.Unique != newIndex.Unique)
                    {
                        OutPut?.Invoke($"      unique: ${orgIndex.Unique} -> ${newIndex.Unique}");
                        //   console.log(`      unique: ${orgIndex.unique} -> ${index.unique}`);
                    }

                    if (!(orgIndex.Spatial ?? new Spatial()).Equals(newIndex.Spatial ?? new Spatial()))
                    {
                        OutPut?.Invoke($"      spatial:");
                        OutPut?.Invoke($"        tessellationSchema: {orgIndex.Spatial?.TessellationSchema} -> {newIndex.Spatial?.TessellationSchema}");
                        OutPut?.Invoke($"        level1: {orgIndex.Spatial?.Level1} -> {newIndex.Spatial?.Level1}");
                        OutPut?.Invoke($"        level2: {orgIndex.Spatial?.Level2} -> {newIndex.Spatial?.Level2}");
                        OutPut?.Invoke($"        level3: {orgIndex.Spatial?.Level3} -> {newIndex.Spatial?.Level3}");
                        OutPut?.Invoke($"        level4: {orgIndex.Spatial?.Level4} -> {newIndex.Spatial?.Level4}");
                        OutPut?.Invoke($"        cellsPerObject: {orgIndex.Spatial?.CellsPerObject} -> {newIndex.Spatial?.CellsPerObject}");
                    }

                }
            }


            foreach (var (synonymName, synonym) in diff.AddedSynonyms)
            {
                //  console.log(`${ConsoleColor.fgCyan}%s${ConsoleColor.reset}`, `+ ${tableName}`);
                OutPut?.Invoke($"+ {synonymName}");
            }

            foreach (var synonymName in diff.DeletedSynonymNames)
            {
                OutPut?.Invoke($"- {synonymName}");
                //    console.log(`${ConsoleColor.fgRed}%s${ConsoleColor.reset}`, `- ${tableName}`);
            }
            
            foreach (var (synonymName, synonyms) in diff.ModifiedSynonyms)
            {
                OutPut?.Invoke($"# {synonymName}");
                if (synonyms[0].Database != synonyms[1].Database)
                {
                    OutPut?.Invoke($"  database: {synonyms[0].Database} -> {synonyms[1].Database}");
                }
                if (synonyms[0].Schema != synonyms[1].Schema)
                {
                    OutPut?.Invoke($"  schema: {synonyms[0].Schema} -> {synonyms[1].Schema}");
                }
                if (synonyms[0].Object != synonyms[1].Object)
                {
                    OutPut?.Invoke($"  object: {synonyms[0].Object} -> {synonyms[1].Object}");
                }
                //    console.log(`${ConsoleColor.fgRed}%s${ConsoleColor.reset}`, `- ${tableName}`);
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Drop(IDb db)
        {
            db?.Connect();
            var result = db?.Drop(_commandLine.DataBase, _commandLine.Query);
            db?.Close();
            
            if (result.Success)
            {
                if(_commandLine.Query)
                {
                    OutPut?.Invoke($"{result.Query}");
                }
                else if(_commandLine.DryRun)
                {
                    OutPut?.Invoke("drop is success (dry run)");
                }
                else
                {
                    OutPut?.Invoke("drop is success");
                }
            }
            else
            {
                OutPut?.Invoke("drop is failed");
                OutPut?.Invoke($"{result.Exception.Message}");
                OutPut?.Invoke("-------------------------");
                OutPut?.Invoke($"{result.Query}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="db"></param>
        private void Extract(string serverName, IDb db)
        {
            db?.Connect();
            var extractDb = db?.Extract();
            var yaml = Utility.DataBaseToYaml(extractDb);

            var file = _commandLine.OutputFile;
            if (!string.IsNullOrWhiteSpace(file) && !string.IsNullOrEmpty(serverName))
            {
                file = Path.Combine(file, $"{serverName}.yml");
            }

            if (!string.IsNullOrWhiteSpace(file))
            {
                Directory.CreateDirectory(Path.GetDirectoryName(file));
            }

            File.WriteAllText(file, yaml);
            db?.Close();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Query(IDb db)
        {
            var query = db?.Query(_commandLine.DataBase);
            OutPut?.Invoke(query);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Update(IDb db)
        {
            db?.Connect();
            var result = db?.Update(_commandLine.DataBase, _commandLine.Query, _commandLine.Drop);
            db?.Close();
            if (result.Success)
            {
                if (string.IsNullOrWhiteSpace(result.Query))
                {
                    OutPut?.Invoke($"nothing to do");
                }
                else if(_commandLine.Query)
                {
                    OutPut?.Invoke($"{result.Query}");
                }
                else if(_commandLine.DryRun)
                {
                    OutPut?.Invoke("update is success (dry run)");
                }
                else
                {
                    OutPut?.Invoke("update is success");
                }
            }
            else
            {
                OutPut?.Invoke("update is failed");
                OutPut?.Invoke($"{result.Exception.Message}");
                OutPut?.Invoke("-------------------------");
                OutPut?.Invoke($"{result.Query}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void ReCreate(IDb db)
        {
            db?.Connect();
            var result = db?.ReCreate(_commandLine.DataBase, _commandLine.Query);
            db?.Close();
            if (result.Success)
            {
                if(_commandLine.Query)
                {
                    OutPut?.Invoke($"{result.Query}");
                }
                else if(_commandLine.DryRun)
                {
                    OutPut?.Invoke("recreate is success (dry run)");
                }
                else
                {
                    OutPut?.Invoke("recreate is success");
                }
            }
            else
            {
                OutPut?.Invoke("recreate is failed");
                OutPut?.Invoke($"{result.Exception.Message}");
                OutPut?.Invoke("-------------------------");
                OutPut?.Invoke($"{result.Query}");
            }
        }
    }
}