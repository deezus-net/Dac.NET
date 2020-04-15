using System.Collections.Generic;
using System.Linq;
using Dac.Net.Core;

namespace Dac.Net.Db
{
    public class Diff
    {
        public Dictionary<string, Table> AddedTables { get; set; } = new Dictionary<string, Table>();

        public List<string> DeletedTableNames { get; set; } = new List<string>();
        public Dictionary<string, ModifiedTable> ModifiedTables { get; set; } = new Dictionary<string, ModifiedTable>();
        public DataBase CurrentDb { get; set; }
        public DataBase NewDb { get; set; }

        public bool HasDiff => AddedTables.Any() || DeletedTableNames.Any() || ModifiedTables.Any();

        public Diff()
        {

        }

        public Diff(DataBase currentDb, DataBase newDb)
        {
            CurrentDb = currentDb;
            NewDb = newDb;

            Check();
        }

        public void Check()
        {
            // tables
            var tableNames = CurrentDb.Tables.Keys.Concat(NewDb.Tables.Keys).Distinct();

            foreach (var tableName in tableNames)
            {
                if (!NewDb.Tables.ContainsKey(tableName))
                {
                    DeletedTableNames.Add(tableName);
                }
                else if (!CurrentDb.Tables.ContainsKey(tableName))
                {
                    AddedTables.Add(tableName, NewDb.Tables[tableName]);

                }
                else
                {
                    // columns
                    var columnNames = CurrentDb.Tables[tableName].Columns.Keys
                        .Concat(NewDb.Tables[tableName].Columns.Keys).Distinct();

                    foreach (var columnName in columnNames)
                    {
                        if (!NewDb.Tables[tableName].Columns.ContainsKey(columnName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].DeletedColumnName.Add(columnName);

                        }
                        else if (!CurrentDb.Tables[tableName].Columns.ContainsKey(columnName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].AddedColumns.Add(columnName, NewDb.Tables[tableName].Columns[columnName]);

                        }
                        else if (!CurrentDb.Tables[tableName].Columns[columnName]
                            .Equals(NewDb.Tables[tableName].Columns[columnName]))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].ModifiedColumns[columnName] = new[]
                            {
                                CurrentDb.Tables[tableName].Columns[columnName],
                                NewDb.Tables[tableName].Columns[columnName]
                            };
                        }
                    }

                    // indexes
                    var indexNames = CurrentDb.Tables[tableName].Indices.Keys
                        .Concat(NewDb.Tables[tableName].Indices.Keys).Distinct();

                    foreach (var indexName in indexNames)
                    {
                        if (!NewDb.Tables[tableName].Indices.ContainsKey(indexName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].DeletedIndexNames.Add(indexName);

                        }
                        else if (!CurrentDb.Tables[tableName].Indices.ContainsKey(indexName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].AddedIndices.Add(indexName, NewDb.Tables[tableName].Indices[indexName]);

                        }
                        else if (!NewDb.Tables[tableName].Indices[indexName]
                            .Equals(CurrentDb.Tables[tableName].Indices[indexName]))
                        {
                            InitModifiedTable(tableName);
                      //     NewDb.Tables[tableName].Indices[indexName].Name = indexName;
                            ModifiedTables[tableName].ModifiedIndices[indexName] = new[]
                            {
                                CurrentDb.Tables[tableName].Indices[indexName],
                                NewDb.Tables[tableName].Indices[indexName]
                            };

                        }

                    }
                }

            }

        }

        private void InitModifiedTable(string tableName)
        {
            if (!ModifiedTables.ContainsKey(tableName))
            {
                ModifiedTables.Add(tableName, new ModifiedTable());
            }
        }
    }

    public class ModifiedTable
    {
        public Dictionary<string, Column> AddedColumns { get; set; } = new Dictionary<string, Column>();
        public Dictionary<string, Column[]> ModifiedColumns { get; set; } = new Dictionary<string, Column[]>();
        public List<string> DeletedColumnName { get; set; } = new List<string>();
        public Dictionary<string, Index> AddedIndices { get; set; } = new Dictionary<string, Index>();
        public Dictionary<string, Index[]> ModifiedIndices { get; set; } = new Dictionary<string, Index[]>();
        public List<string> DeletedIndexNames { get; set; } = new List<string>();
    }


}