namespace Dac.Net.Db
{
    public interface IDb
    {

        public QueryResult Drop(DataBase db, bool queryOnly);
        public DataBase Extract();
        public string Query(DataBase db);
        public QueryResult Create(DataBase db, bool queryOnly);
        public QueryResult ReCreate(DataBase db, bool queryOnly);
        public QueryResult Update(DataBase db, bool queryOnly, bool dropTable);
        public Diff Diff(DataBase db);
        public bool Connect();
        public bool Close();
        public string GetName();
    }
}