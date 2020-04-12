using System;
using System.Collections.Generic;
using System.IO;
using Dac.Net.Db;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Dac.Net.Core
{
    public class Utility
    {
        private static readonly IDeserializer Deserializer = new DeserializerBuilder().WithNamingConvention(CamelCaseNamingConvention.Instance).Build();
        
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
            try
            {
                var yml = File.ReadAllText(file);
                return Deserializer.Deserialize<DataBase>(yml);
            }
            catch (Exception e)
            {

            }

            return null;
        }
    }
}