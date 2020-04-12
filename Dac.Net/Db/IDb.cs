namespace Dac.Net.Db
{
    public interface IDb
    {

        public string Drop(DataBase db, bool queryOnly);
        public DataBase Extract();
        public string Query(DataBase db);
        public string Create(DataBase db, bool queryOnly);
        public string ReCreate(DataBase db, bool queryOnly);
        public string Update(DataBase db, bool queryOnly, bool dropTable);
        public Diff Diff(DataBase db);
        public bool Connect();
        public bool Close();
    }
}