using System.Collections.Generic;
using System.Linq;

namespace Dac.Net.Db
{
    public static class DbUtility
    {
        public static void TrimDbProperties(this Db db)
        {

            foreach (var (tableName, dbTable) in db.Tables)
            {

                foreach (var (columnName, dbColumn) in dbTable.Columns)
                {
                    if (dbColumn.Id ?? false)
                    {
                        dbColumn.Type = null;
                        dbColumn.NotNull = null;
                        dbColumn.Pk = null;
                        dbColumn.Length = null;
                    }
                    else
                    {
                        dbColumn.Id = null;
                    }

                    if (!(dbColumn.Pk ?? false))
                    {
                        dbColumn.Pk = null;
                    }
                    else
                    {
                        dbColumn.NotNull = true;
                    }

                    if (!(dbColumn.NotNull ?? false))
                    {
                        dbColumn.NotNull = null;
                    }

                    if (string.IsNullOrWhiteSpace(dbColumn.Default))
                    {
                        dbColumn.Default = null;
                    }

                    if ((dbColumn.Length ?? 0) == 0)
                    {
                        dbColumn.Length = null;
                    }

                    foreach (var (fkName, dbForeignKey) in dbColumn.Fk)
                    {
                        if (string.IsNullOrWhiteSpace(dbForeignKey.Update))
                        {
                            dbForeignKey.Update = null;
                        }

                        if (string.IsNullOrWhiteSpace(dbForeignKey.Delete))
                        {
                            dbForeignKey.Delete = null;
                        }
                    }

                }

                foreach (var (indexName, dbIndex) in dbTable.Indices)
                {
                    if (!(dbIndex.Unique ?? false))
                    {
                        dbIndex.Unique = null;
                    }

                    foreach (var (indexColumnName, direction) in dbIndex.Columns)
                    {
                        dbIndex.Columns[indexColumnName] = (direction ?? "").ToLower();
                    }
                }

            }

        }


        public static DbDiff Diff(this Db org, Db target)
        {
            var result = new DbDiff
            {
                CurrentTables = org.Tables,
                NewTables = target.Tables
            };

            // tables

            foreach (var tableName in org.Tables.Keys.Concat(target.Tables.Keys).Distinct())
            {
                if (!target.Tables.ContainsKey(tableName))
                {
                    result.DeletedTableNames.Add(tableName);

                }
                else if (!org.Tables.ContainsKey(tableName))
                {
                    result.AddedTables[tableName] = target.Tables[tableName];

                }
                else
                {
                    // columns

                    foreach (var columnName in org.Tables[tableName].Columns.Keys.Concat(target.Tables[tableName].Columns.Keys)
                        .Distinct())
                    {
                        if (!target.Tables[tableName].Columns.ContainsKey(columnName))
                        {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].DeletedColumnName.Add(columnName);

                        }
                        else if (!org.Tables[tableName].Columns.ContainsKey(columnName))
                        {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].AddedColumns.Add(columnName, target.Tables[tableName].Columns[columnName]);

                        }
                        else if (!org.Tables[tableName].Columns[columnName].Equal(target.Tables[tableName].Columns[columnName]))
                        {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].ModifiedColumns.Add(columnName, new[]
                            {
                                org.Tables[tableName].Columns[columnName],
                                target.Tables[tableName].Columns[columnName]
                            });
                        }
                    }

                    // indexes
                    foreach (var indexName in org.Tables[tableName].Indices.Keys.Concat(target.Tables[tableName].Indices.Keys).Distinct()) {
                        
                        
                        
                        if (!target.Tables[tableName].Indices.ContainsKey(indexName)) {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].DeletedIndexNames.Add(indexName);

                        } else if (!org.Tables[tableName].Indices.ContainsKey(indexName)) {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].AddedIndices.Add(indexName, target.Tables[tableName].Indices[indexName]);

                        } else if (!target.Tables[tableName].Indices[indexName].Equal(org.Tables[tableName].Indices[indexName])) {
                            InitModifiedTable(result, tableName);
                            result.ModifiedTables[tableName].ModifiedIndices.Add(indexName, new[]
                                {
                                    org.Tables[tableName].Indices[indexName],
                                    target.Tables[tableName].Indices[indexName]
                                }
                            );

                        }

                    }
                }

            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="org"></param>
        /// <param name="target"></param>
        /// <returns></returns>
        public static bool Equal(this DbColumn org, DbColumn target)
        {
            // foreign key check
            var fkDiff = false;
            foreach (var fkName in org.Fk.Keys.Concat(target.Fk.Keys).Distinct())
            {
                if (!org.Fk.Keys.Contains(fkName) || !target.Fk.Keys.Contains(fkName))
                {
                    fkDiff = true;
                    break;
                }

                if ((org.Fk[fkName].Update ?? "") == (target.Fk[fkName].Update ?? "") &&
                    (org.Fk[fkName].Delete ?? "") == (target.Fk[fkName].Delete ?? "") &&
                    (org.Fk[fkName].Table == target.Fk[fkName].Table) &&
                    (org.Fk[fkName].Column == target.Fk[fkName].Column))
                {
                    continue;
                }
                fkDiff = true;
                break;
            }

            return org.Type == target.Type &&
                   org.Length == target.Length &&
                   org.NotNull == target.NotNull &&
                   org.Id == target.Id &&
                   org.Default == target.Default &&
                   !fkDiff;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="org"></param>
        /// <param name="target"></param>
        /// <returns></returns>
        public static bool Equal(this DbIndex org, DbIndex target)
        {
            var col1 = string.Join("¥t", org.Columns.Select(x => $"{x.Key},{x.Value}"));
            var col2 = string.Join("¥t", target.Columns.Select(x => $"{x.Key},{x.Value}"));
            return org.Unique == target.Unique && org.Type == target.Type && col1 == col2;
        }


        private static void InitModifiedTable(DbDiff result, string tableName)
        {
            if (!result.ModifiedTables.ContainsKey(tableName))
            {
                result.ModifiedTables.Add(tableName, new ModifiedTable());
            }
        }
    }
}