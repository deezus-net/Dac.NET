namespace Molder.Db
{
    public class Synonym
    {
        public string Database { get; set; }
        
        public string Schema { get; set; }
        public string Object { get; set; }

        public bool Equals(Synonym target)
        {
            return Database == target.Database && Schema == target.Schema && Object == target.Object;
        }
    }
}