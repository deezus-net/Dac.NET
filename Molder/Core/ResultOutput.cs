using System;
using System.Linq;
using Molder.Db;

namespace Molder.Core
{
    public class ResultOutput
    {
        private readonly IOutput _output;
        private const ConsoleColor AddColor = ConsoleColor.Cyan;
        private const ConsoleColor ModifyColor = ConsoleColor.Green;
        private const ConsoleColor DeleteColor = ConsoleColor.Red;

        public ResultOutput(IOutput output)
        {
            _output = output;
        }


        public void WriteLine(string message)
        {
            _output.WriteLine(message);
        }

        public void Error(string message)
        {
            _output.WriteLine(message);
        }

        public void Create(QueryResult result, CommandLine commandLine, string dbName)
        {
            if (result.Success)
            {

                if (commandLine.Query)
                {
                    _output.WriteLine($"{result.Query}");
                }
                else if (commandLine.DryRun)
                {
                    _output.WriteLine($"[{dbName}] create is success (dry run)");
                }
                else
                {
                    _output.WriteLine($"[{dbName}] create is success");
                }
            }
            else
            {
                _output.WriteLine($"[{dbName}] create is failed");
                _output.WriteLine($"{result.Exception.Message}");
                _output.WriteLine("-------------------------");
                _output.WriteLine($"{result.Query}");
            }
        }

        public void Diff(Diff diff, string dbName)
        {
            if (!diff.HasDiff)
            {
                _output.WriteLine($"[{dbName}] no difference");
                return;
            }

            _output.WriteLine($"[{dbName}]");
            ShowTableDiff(diff);
            ShowViewDiff(diff);
            ShowSynonymDiff(diff);

            _output.WriteLine("");
        }

        private void ShowTableDiff(Diff diff)
        {
            if (!diff.AddedTables.Any() && !diff.DeletedTableNames.Any() && !diff.ModifiedTables.Any())
            {
                return;
            }

            _output.WriteLine("* tables");
            foreach (var (tableName, table) in diff.AddedTables)
            {
                _output.SetColor(AddColor);
                _output.WriteLine($"  + {tableName}");
                _output.ClearColor();
            }

            foreach (var tableName in diff.DeletedTableNames)
            {
                _output.SetColor(DeleteColor);
                _output.WriteLine($"  - {tableName}");
                _output.ClearColor();
            }

            foreach (var (tableName, table) in diff.ModifiedTables)
            {
                _output.SetColor(ModifyColor);
                _output.WriteLine($"  # {tableName}");
                _output.ClearColor();

                foreach (var (columnName, column) in table.AddedColumns)
                {
                    _output.SetColor(AddColor);
                    _output.WriteLine($"    + {columnName}");
                    _output.ClearColor();
                }

                foreach (var columnName in table.DeletedColumnName)
                {
                    _output.SetColor(DeleteColor);
                    _output.WriteLine($"    - {columnName}");
                    _output.ClearColor();
                }

                foreach (var (columnName, column) in table.ModifiedColumns)
                {
                    var orgColumn = column[0];
                    var newColumn = column[1];
                    _output.SetColor(ModifyColor);
                    _output.WriteLine($"    # {columnName}");


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


                        _output.WriteLine($"        type: {orgType} -> {newType}");
                    }

                    if (orgColumn.Pk != newColumn.Pk)
                    {
                        _output.WriteLine($"        pk: {orgColumn.Pk} -> {newColumn.Pk}");
                    }

                    if (orgColumn.NotNull != newColumn.NotNull)
                    {
                        _output.WriteLine($"        not null: {orgColumn.NotNull} -> {newColumn.NotNull}");
                    }

                    _output.ClearColor();
                }

                foreach (var (indexName, index) in table.AddedIndexes)
                {
                    _output.SetColor(AddColor);
                    _output.WriteLine($"    + {indexName}");
                    _output.ClearColor();
                }

                foreach (var indexName in table.DeletedIndexNames)
                {
                    _output.SetColor(DeleteColor);
                    _output.WriteLine($"    - {indexName}");
                    _output.ClearColor();
                }

                foreach (var (indexName, indices) in table.ModifiedIndexes)
                {
                    var orgIndex = indices[0];
                    var newIndex = indices[1];

                    _output.SetColor(ModifyColor);
                    _output.WriteLine($"    # {indexName}");


                    if (orgIndex.Type != newIndex.Type)
                    {
                        _output.WriteLine($"        type: {orgIndex.Type} -> {newIndex.Type}");
                    }

                    var orgIndexColumns = string.Join(",", orgIndex.Columns.Select(x => $"{x.Key} {x.Value}"));
                    var newIndexColumns = string.Join(",", newIndex.Columns.Select(x => $"{x.Key} {x.Value}"));
                    if (orgIndexColumns != newIndexColumns)
                    {
                        _output.WriteLine($"        columns: {orgIndexColumns} -> {newIndexColumns}");
                    }

                    if (orgIndex.Unique != newIndex.Unique)
                    {
                        _output.WriteLine($"        unique: ${orgIndex.Unique} -> ${newIndex.Unique}");
                    }

                    if (!(orgIndex.Spatial ?? new Spatial()).Equals(newIndex.Spatial ?? new Spatial()))
                    {
                        _output.WriteLine($"        spatial:");
                        _output.WriteLine(
                            $"          tessellationSchema: {orgIndex.Spatial?.TessellationSchema} -> {newIndex.Spatial?.TessellationSchema}");
                        _output.WriteLine($"          level1: {orgIndex.Spatial?.Level1} -> {newIndex.Spatial?.Level1}");
                        _output.WriteLine($"          level2: {orgIndex.Spatial?.Level2} -> {newIndex.Spatial?.Level2}");
                        _output.WriteLine($"          level3: {orgIndex.Spatial?.Level3} -> {newIndex.Spatial?.Level3}");
                        _output.WriteLine($"          level4: {orgIndex.Spatial?.Level4} -> {newIndex.Spatial?.Level4}");
                        _output.WriteLine(
                            $"          cellsPerObject: {orgIndex.Spatial?.CellsPerObject} -> {newIndex.Spatial?.CellsPerObject}");
                    }

                    _output.ClearColor();
                }
            }
        }

        private void ShowViewDiff(Diff diff)
        {
            if (!diff.AddedViews.Any() && !diff.DeletedViewNames.Any() && !diff.ModifiedViews.Any())
            {
                return;
            }

            _output.WriteLine("* views");
            foreach (var (viewName, definition) in diff.AddedViews)
            {
                _output.SetColor(AddColor);
                _output.WriteLine($"  + {viewName}");
                _output.ClearColor();
            }

            foreach (var viewName in diff.DeletedViewNames)
            {
                _output.SetColor(DeleteColor);
                _output.WriteLine($"  - {viewName}");
                _output.ClearColor();
            }

            foreach (var (viewName, definitions) in diff.ModifiedViews)
            {
                _output.SetColor(ModifyColor);
                _output.WriteLine($"  # {viewName}");
                _output.WriteLine($"    {definitions[0]} -> {definitions[1]}");
                _output.ClearColor();
            }
        }

        private void ShowSynonymDiff(Diff diff)
        {
            if (!diff.AddedSynonyms.Any() && !diff.DeletedSynonymNames.Any() && !diff.ModifiedSynonyms.Any())
            {
                return;
            }

            _output.WriteLine("* synonyms");

            foreach (var (synonymName, synonym) in diff.AddedSynonyms)
            {
                _output.SetColor(AddColor);
                _output.WriteLine($"  + {synonymName}");
                _output.ClearColor();
            }

            foreach (var synonymName in diff.DeletedSynonymNames)
            {
                _output.SetColor(DeleteColor);
                _output.WriteLine($"  - {synonymName}");
                _output.ClearColor();
            }

            foreach (var (synonymName, synonyms) in diff.ModifiedSynonyms)
            {
                _output.SetColor(ModifyColor);
                _output.WriteLine($"  # {synonymName}");
                if (synonyms[0].Database != synonyms[1].Database)
                {
                    _output.WriteLine($"    database: {synonyms[0].Database} -> {synonyms[1].Database}");
                }

                if (synonyms[0].Schema != synonyms[1].Schema)
                {
                    _output.WriteLine($"    schema: {synonyms[0].Schema} -> {synonyms[1].Schema}");
                }

                if (synonyms[0].Object != synonyms[1].Object)
                {
                    _output.WriteLine($"    object: {synonyms[0].Object} -> {synonyms[1].Object}");
                }

                _output.ClearColor();
            }
        }

        public void Drop(QueryResult result, CommandLine commandLine, string dbName)
        {
            if (result.Success)
            {
                if (commandLine.Query)
                {
                    _output.WriteLine($"{result.Query}");
                }
                else if (commandLine.DryRun)
                {
                    _output.WriteLine($"[{dbName}] drop is success (dry run)");
                }
                else
                {
                    _output.WriteLine($"[{dbName}] drop is success");
                }
            }
            else
            {
                _output.WriteLine($"[{dbName}] drop is failed");
                _output.WriteLine($"{result.Exception.Message}");
                _output.WriteLine("-------------------------");
                _output.WriteLine($"{result.Query}");
            }
        }

        public void Update(QueryResult result, CommandLine commandLine, string dbName)
        {
            if (result.Success)
            {
                if (string.IsNullOrWhiteSpace(result.Query))
                {
                    _output.WriteLine($"[{dbName}] nothing to do");
                }
                else if (commandLine.Query)
                {
                    _output.WriteLine($"{result.Query}");
                }
                else if (commandLine.DryRun)
                {
                    _output.WriteLine($"[{dbName}] update is success (dry run)");
                }
                else
                {
                    _output.WriteLine($"[{dbName}] update is success");
                }
            }
            else
            {
                _output.WriteLine($"[{dbName}] update is failed");
                _output.WriteLine($"{result.Exception.Message}");
                _output.WriteLine("-------------------------");
                _output.WriteLine($"{result.Query}");
            }
        }

        public void ReCreate(QueryResult result, CommandLine commandLine, string dbName)
        {
            if (result.Success)
            {
                if (commandLine.Query)
                {
                    _output.WriteLine($"{result.Query}");
                }
                else if (commandLine.DryRun)
                {
                    _output.WriteLine($"[{dbName}] recreate is success (dry run)");
                }
                else
                {
                    _output.WriteLine($"[{dbName}] recreate is success");
                }
            }
            else
            {
                _output.WriteLine($"[{dbName}] recreate is failed");
                _output.WriteLine($"{result.Exception.Message}");
                _output.WriteLine("-------------------------");
                _output.WriteLine($"{result.Query}");
            }
        }
    }
}