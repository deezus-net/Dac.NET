using System;
using System.Collections.Generic;
using System.IO;

 using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Dac.Net.Db;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Dac.Net.Core
{
    public class Utility
    {
        private static readonly IDeserializer Deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
        private static readonly ISerializer Serializer = new SerializerBuilder().ConfigureDefaultValuesHandling(DefaultValuesHandling.OmitNull).WithNamingConvention(CamelCaseNamingConvention.Instance).Build();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static Dictionary<string, Server> LoadServers(string file)
        {
            try
            {
                var yml = File.ReadAllText(file);
                return Deserializer.Deserialize<Dictionary<string, Server>>(yml);
            }
            catch (Exception e)
            {

            }

            return null;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataBase LoadDataBase(string file)
        {
          //  try
          //  {
                var yml = File.ReadAllText(file);
                return Deserializer.Deserialize<DataBase>(yml);
           // }
           // catch (Exception e)
           // {

           // }

            return null;
        }

        public static void TrimDataBaseProperties(DataBase db)
        {
            foreach (var (tableName, table) in db.Tables)
            {

                foreach (var (columnName, column) in table.Columns)
                {
                    if (column.Id ?? false)
                    {
                        column.Type = null;
                        column.NotNull = null;
                        column.Pk = null;
                        column.Length = null;
                    }
                    else
                    {
                        column.Id = null;
                    }

                    if (!(column.Pk ?? false))
                    {
                        column.Pk = null;
                    }
                    else
                    {
                        column.NotNull = true;
                    }

                    if (!(column.NotNull ?? false))
                    {
                        column.NotNull = null;
                    }

                    if (string.IsNullOrWhiteSpace(column.Default))
                    {
                        column.Default = null;
                    }

                    if (column.LengthInt == 0 || string.IsNullOrWhiteSpace(column.Length))
                    {
                        column.Length = null;
                    }


                    if (column.ForeignKeys != null)
                    {
                        foreach (var (fkName, fk) in column.ForeignKeys)
                        {
                            if (string.IsNullOrEmpty(fk.Update) || fk.Update == "NO_ACTION")
                            {
                                fk.Update = null;
                            }

                            if (string.IsNullOrWhiteSpace(fk.Delete) || fk.Delete == "NO_ACTION")
                            {
                                fk.Delete = null;
                            }
                        }

                        if (!column.ForeignKeys.Any())
                        {
                            column.ForeignKeys = null;
                        }
                    }

                }

                if (table.Indices != null)
                {
                    foreach (var (indexName, index) in table.Indices)
                    {
                        if (!(index.Unique ?? false))
                        {
                            index.Unique = null;
                        }

                        index.Type = index.Type?.ToLower();

                        var indexColumns = new Dictionary<string, string>();
                        foreach (var (indexColumnName, direction) in index.Columns)
                        {
                            indexColumns.Add(indexColumnName, (direction ?? "").ToLower());
                        }

                        index.Columns = indexColumns;

                    }

                    if (!table.Indices.Any())
                    {
                        table.Indices = null;
                    }
                }
            }
        }

        public static string DataBaseToYaml(DataBase db)
        {
            TrimDataBaseProperties(db);
            var sb = new StringBuilder();
            using (var tw = new StringWriter(sb))
            {
                Serializer.Serialize(tw, db);
            }

            return sb.ToString();

        }
    }
}