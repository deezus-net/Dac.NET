namespace Dac.Net.Db
{
    public class ForeignKey
    {
        public string Table { get; set; }
        public string Column { get; set; }
        public string Update { get; set; }
        public string Delete { get; set; }
    }
}