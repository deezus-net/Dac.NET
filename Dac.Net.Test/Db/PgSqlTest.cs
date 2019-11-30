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

        private readonly Dac.Net.Db.Db _db = new Dac.Net.Db.Db()
        {
            Tables = new Dictionary<string, DbTable>()
            {
                {
                    "users", new DbTable()
                    {
                        Columns = new Dictionary<string, DbColumn>()
                        {
                            {"id", new DbColumn() {Id = true}},
                            {"name", new DbColumn() {Type = "varchar", Length = 100}},
                            {
                                "user_group_id", new DbColumn()
                                {
                                    Type = "int",
                                    NotNull = true,
                                    Fk = new Dictionary<string, DbForeignKey>()
                                    {
                                        {
                                            "fk_users", new DbForeignKey()
                                            {
                                                Table = "user_groups",
                                                Column = "id"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Indices = new Dictionary<string, DbIndex>()
                        {
                            {
                                "ix_users_name", new DbIndex()
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
                        Columns = new Dictionary<string, DbColumn>()
                        {
                            {
                                "id", new DbColumn()
                                {
                                    Id = true
                                }
                            },
                            {"name", new DbColumn() {Type = "varchar", Length = 100}}
                        },
                        Indices = new Dictionary<string, DbIndex>()
                        {
                            {
                                "ix_user_groups_name", new DbIndex()
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
            var db = await _pg.Extract();
            Assert.NotEmpty(db.Tables);
        }

        [Fact]
        public async void ReCreateTest()
        {
            var query = await _pg.ReCreate(_db, false);
            Assert.True(!string.IsNullOrWhiteSpace(query));
            _output.WriteLine(query);
        }

        [Fact]
        public async void DiffTest()
        {
            var diff = await _pg.Diff(_db);
            Assert.False(diff.HasDiff);
        }

        [Fact]
        public async void UpdateTest()
        {
            var query = await _pg.Update(_db, true, false);
            Assert.True(!string.IsNullOrWhiteSpace(query));
            _output.WriteLine(query);
        }
    }
}