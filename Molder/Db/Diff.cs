using System.Collections.Generic;
using System.Linq;
using Molder.Core;

namespace Molder.Db
{
    public class Diff
    {
        public Dictionary<string, Table> AddedTables { get; set; } = new Dictionary<string, Table>();
        public List<string> DeletedTableNames { get; set; } = new List<string>();
        public Dictionary<string, ModifiedTable> ModifiedTables { get; set; } = new Dictionary<string, ModifiedTable>();
        
        public Dictionary<string, Synonym> AddedSynonyms { get; set; } = new Dictionary<string, Synonym>();
        public List<string> DeletedSynonymNames { get; set; } = new List<string>();
        public Dictionary<string, Synonym[]> ModifiedSynonyms { get; set; } = new Dictionary<string, Synonym[]>();
        
        public Dictionary<string, string> AddedViews { get; set; } = new Dictionary<string, string>();
        public List<string> DeletedViewNames { get; set; } = new List<string>();
        public Dictionary<string, string[]> ModifiedViews { get; set; } = new Dictionary<string, string[]>();
        
        public DataBase CurrentDb { get; set; }
        public DataBase NewDb { get; set; }

        public bool HasDiff => AddedTables.Any() || DeletedTableNames.Any() || ModifiedTables.Any() || AddedSynonyms.Any() || DeletedSynonymNames.Any() || ModifiedSynonyms.Any() || AddedViews.Any() || DeletedViewNames.Any() || ModifiedViews.Any();

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
                            ModifiedTables[tableName].AddedColumns
                                .Add(columnName, NewDb.Tables[tableName].Columns[columnName]);

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
                    var currentIndexes = CurrentDb.Tables[tableName].Indexes ?? new Dictionary<string, Index>();
                    var newIndexes = NewDb.Tables[tableName].Indexes ?? new Dictionary<string, Index>();

                    var indexNames = currentIndexes.Keys.Concat(newIndexes.Keys).Distinct();

                    foreach (var indexName in indexNames)
                    {
                        if (!newIndexes.ContainsKey(indexName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].DeletedIndexNames.Add(indexName);

                        }
                        else if (!currentIndexes.ContainsKey(indexName))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].AddedIndexes.Add(indexName, newIndexes[indexName]);

                        }
                        else if (!newIndexes[indexName]
                            .Equals(currentIndexes[indexName]))
                        {
                            InitModifiedTable(tableName);
                            ModifiedTables[tableName].ModifiedIndexes[indexName] = new[]
                            {
                                currentIndexes[indexName],
                                newIndexes[indexName]
                            };

                        }

                    }
                }

            }

            // synonyms
            var currentSynonyms = CurrentDb.Synonyms ?? new Dictionary<string, Synonym>();
            var newSynonyms = NewDb.Synonyms ?? new Dictionary<string, Synonym>();
            foreach (var synonymName in currentSynonyms.Keys.Concat(newSynonyms.Keys).Distinct())
            {
                if (!newSynonyms.ContainsKey(synonymName))
                {
                    DeletedSynonymNames.Add(synonymName);
                }
                else if (!currentSynonyms.ContainsKey(synonymName))
                {
                    AddedSynonyms.Add(synonymName, newSynonyms[synonymName]);
                }
                else if (!currentSynonyms[synonymName].Equals(newSynonyms[synonymName]))
                {
                    ModifiedSynonyms.Add(synonymName, new[]
                    {
                        currentSynonyms[synonymName],
                        newSynonyms[synonymName]
                    });
                }
            }
            
            // views
            var currentViews = CurrentDb.Views ?? new Dictionary<string, string>();
            var newViews = NewDb.Views ?? new Dictionary<string, string>();
            foreach (var viewName in currentViews.Keys.Concat(newViews.Keys).Distinct())
            {
                if (!newViews.ContainsKey(viewName))
                {
                    DeletedViewNames.Add(viewName);
                }
                else if (!currentViews.ContainsKey(viewName))
                {
                    AddedViews.Add(viewName, newViews[viewName]);
                }
                else if (currentViews[viewName] != newViews[viewName])
                {
                    ModifiedViews.Add(viewName, new[]
                    {
                        currentViews[viewName],
                        newViews[viewName]
                    });
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
        public Dictionary<string, Index> AddedIndexes { get; set; } = new Dictionary<string, Index>();
        public Dictionary<string, Index[]> ModifiedIndexes { get; set; } = new Dictionary<string, Index[]>();
        public List<string> DeletedIndexNames { get; set; } = new List<string>();
    }


}