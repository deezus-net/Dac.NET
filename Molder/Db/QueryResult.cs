using System;

namespace Molder.Db
{
    public class QueryResult
    {
        public bool Success { get; set; } = true;
        public string Query { get; set; } = "";
        public Exception Exception { get; set; }
        
    }
}