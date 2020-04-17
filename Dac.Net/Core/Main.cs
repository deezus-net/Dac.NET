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
            foreach (var server in _commandLine.Servers)
            {
                IDb db = null;
                switch (server.Type)
                {
                    case Define.DatabaseType.Mysql:
                        break;
                    case Define.DatabaseType.Postgres:
                        break;
                    case Define.DatabaseType.MsSql:
                        db = new MsSql(server);
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
                    case Define.Command.Trim:
                        Trim(db);
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
            var query = db?.Create(_commandLine.DataBase, _commandLine.Query);
            db?.Close();
            OutPut?.Invoke(query);
            
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

            if (diff == null)
            {
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

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Drop(IDb db)
        {
            db?.Connect();
            var query = db?.Drop(_commandLine.DataBase, _commandLine.Query);
            db.Close();
            OutPut?.Invoke(query);
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
        private void Trim(IDb db)
        {
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void Update(IDb db)
        {
            db?.Connect();
            var query = db?.Update(_commandLine.DataBase, _commandLine.Query, _commandLine.Drop);
            db?.Close();
            OutPut?.Invoke(query);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="db"></param>
        private void ReCreate(IDb db)
        {
            db?.Connect();
            var query = db?.ReCreate(_commandLine.DataBase, _commandLine.Query);
            db?.Close();
            OutPut?.Invoke(query);
        }
    }
}