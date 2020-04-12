namespace Dac.Net.Db
{
    public class MsSql : IDb
    {
        public string Drop(DataBase db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public DataBase Extract()
        {
            throw new System.NotImplementedException();
        }

        public string Query(DataBase db)
        {
            throw new System.NotImplementedException();
        }

        public string Create(DataBase db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public string ReCreate(DataBase db, bool queryOnly)
        {
            throw new System.NotImplementedException();
        }

        public string Update(DataBase db, bool queryOnly, bool dropTable)
        {
            throw new System.NotImplementedException();
        }

        public Diff Diff(DataBase db)
        {
            throw new System.NotImplementedException();
        }

        public bool Connect()
        {
            throw new System.NotImplementedException();
        }

        public bool Close()
        {
            throw new System.NotImplementedException();
        }
    }
}