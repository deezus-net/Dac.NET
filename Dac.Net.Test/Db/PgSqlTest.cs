using System.Collections.Generic;
using Dac.Net.Db;
using Xunit;
using Xunit.Abstractions;

namespace Dac.Net.Test.Db
{
    public class PgSqlTest
    {
        private readonly ITestOutputHelper _output;

        private PgSql _pg = new PgSql()
        {
            Host = "localhost",
            Username = "dac",
            Password = "dac",
            Database = "dac"
        };

        private Dictionary<string, DbTable> _db = new Dictionary<string, DbTable>
        {
            {
                "users", new DbTable()
                {
                    DbColumns = new Dictionary<string, DbColumn>()
                    {
                        {"id", new DbColumn() {Id = true}},
                        {"name", new DbColumn() {Type = "varchar", Length = 100}},
                        {"user_group_id", new DbColumn() {Type = "int", NotNull = true}}
                    },
                    DbIndices = new Dictionary<string, DbIndex>()
                    {
                        {
                            "ix_name", new DbIndex()
                            {
                                Columns = new Dictionary<string, string>
                                {
                                    {"name", "asc"}
                                }
                            }
                        }
                    }
                }
            },
            {
                "user_groups", new DbTable()
                {
                    DbColumns = new Dictionary<string, DbColumn>()
                    {
                        {
                            "id", new DbColumn()
                            {
                                Id = true, Fk = new Dictionary<string, DbForeignKey>()
                                {
                                    {
                                        "f", new DbForeignKey()
                                        {
                                            Table = "users",
                                            Column = "user_group_id"
                                        }
                                    }
                                }
                            }
                        },
                        {"name", new DbColumn() {Type = "varchar", Length = 100}}
                    },
                    DbIndices = new Dictionary<string, DbIndex>()
                    {
                        {
                            "ix_name", new DbIndex()
                            {
                                Columns = new Dictionary<string, string>
                                {
                                    {"name", "asc"}
                                }
                            }
                        }
                    }
                }
            }
        };

        public PgSqlTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async void ConnectTest()
        {
            await _pg.Connect();
        }

        [Fact]
        public async void CreateTest()
        {
            var query = await _pg.Create(_db, true);
            Assert.True(!string.IsNullOrWhiteSpace(query));
            _output.WriteLine(query);
        }
        
        [Fact]
        public async void ExtractTest()
        {
            var tables = await _pg.Extract();
            Assert.NotEmpty(tables);
        }
    }
}