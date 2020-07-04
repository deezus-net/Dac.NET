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
            var currentTables = CurrentDb.Tables.Select(x =>
            {
                var (key, value) = x;
                value.Name = key;
                if (string.IsNullOrWhiteSpace(value.TableId))
                {
                    var tableId = NewDb.Tables.ContainsKey(key) ? NewDb.Tables[key].TableId : "";
                    value.TableId = !string.IsNullOrWhiteSpace(tableId) ? tableId : key;
                }
                return value;
            }).ToList();
            
            var newTables = NewDb.Tables.Select(x =>
            {
                var (key, value) = x;
                value.Name = key;
                if (string.IsNullOrWhiteSpace(value.TableId))
                {
                    var tableId = CurrentDb.Tables.ContainsKey(key) ? CurrentDb.Tables[key].TableId : "";
                    value.TableId = !string.IsNullOrWhiteSpace(tableId) ? tableId : key;;
                }
                return value;
            }).ToList();

            var tableIds = currentTables.Select(x => x.TableId).Concat(newTables.Select(x => x.TableId)).Distinct()
                .ToList();


            foreach (var tableId in tableIds)
            {
                var currentTable = currentTables.FirstOrDefault(x => x.TableId == tableId);
                var newTable = newTables.FirstOrDefault(x => x.TableId == tableId);
                
                if (newTable == null)
                {
                    DeletedTableNames.Add(currentTable.Name);
                }
                else if (currentTable == null)
                {
                    AddedTables.Add(newTable.Name, newTable);
                }
                else
                {
                    // table name
                    if (currentTable.Name != newTable.Name)
                    {
                        InitModifiedTable(newTable.Name);
                        ModifiedTables[newTable.Name].Name = (currentTable.Name, newTable.Name);
                    }
                    
                    // columns
                    var currentColumns = currentTable.Columns.Select(x =>
                    {
                        var (key, value) = x;
                        value.Name = key;
                        if (string.IsNullOrWhiteSpace(value.ColumnId))
                        {
                            value.ColumnId = key;
                        }
                        return x.Value;
                    }).ToList();
                    
                    var newColumns = newTable.Columns.Select(x =>
                    {
                        var (key, value) = x;
                        value.Name = key;
                        if (string.IsNullOrWhiteSpace(value.ColumnId))
                        {
                            value.ColumnId = key;
                        }
                        return x.Value;
                    }).ToList();
                    
                    var columnIds = currentColumns.Select(x => x.ColumnId).Concat(newColumns.Select(x => x.ColumnId)).Distinct()
                        .ToList();

                    foreach (var columnId in columnIds)
                    {
                        var currentColumn = currentColumns.FirstOrDefault(x => x.ColumnId == columnId);
                        var newColumn = newColumns.FirstOrDefault(x => x.ColumnId == columnId);
                        
                        if (newColumn == null)
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].DeletedColumnName.Add(currentColumn.Name);

                        }
                        else if (currentColumn == null)
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].AddedColumns
                                .Add(newColumn.Name, newColumn);

                        }
                        else if (!currentColumn.Equals(newColumn))
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].ModifiedColumns[newColumn.Name] = new[]
                            {
                                currentColumn,
                                newColumn
                            };
                        }
                    }

                    // indexes
                    var currentIndexes = currentTable.Indexes ?? new Dictionary<string, Index>();
                    var newIndexes = newTable.Indexes ?? new Dictionary<string, Index>();

                    var indexNames = currentIndexes.Keys.Concat(newIndexes.Keys).Distinct();

                    foreach (var indexName in indexNames)
                    {
                        if (!newIndexes.ContainsKey(indexName))
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].DeletedIndexNames.Add(indexName);

                        }
                        else if (!currentIndexes.ContainsKey(indexName))
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].AddedIndexes.Add(indexName, newIndexes[indexName]);

                        }
                        else if (!newIndexes[indexName]
                            .Equals(currentIndexes[indexName]))
                        {
                            InitModifiedTable(newTable.Name);
                            ModifiedTables[newTable.Name].ModifiedIndexes[indexName] = new[]
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
        public (string, string) Name { get; set; }
        
        public Dictionary<string, Column> AddedColumns { get; set; } = new Dictionary<string, Column>();
        public Dictionary<string, Column[]> ModifiedColumns { get; set; } = new Dictionary<string, Column[]>();
        public List<string> DeletedColumnName { get; set; } = new List<string>();
        public Dictionary<string, Index> AddedIndexes { get; set; } = new Dictionary<string, Index>();
        public Dictionary<string, Index[]> ModifiedIndexes { get; set; } = new Dictionary<string, Index[]>();
        public List<string> DeletedIndexNames { get; set; } = new List<string>();
    }


}