using System.Collections.Generic;
using Molder.Db;
using Xunit;
using Xunit.Abstractions;

namespace Molder.Test.Db
{
    public class DiffTest
    {
        private readonly ITestOutputHelper _output;
        public DiffTest(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void CheckTest()
        {
            var db1 = new DataBase();
            var db2 = new DataBase();

            var diff = new Diff(db1, db2);
            Assert.False(diff.HasDiff);

        }

        [Fact]
        public void Check2Test()
        {
            var db1 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                }
                            }
                        }
                    }
                }
            };
            var db2 = new DataBase();

            var diff = new Diff(db1, db2);
            Assert.True(diff.HasDiff);

        }
        
        [Fact]
        public void Check3Test()
        {
            var db1 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                }
                            }
                        }
                    }
                }
            };
            var db2 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Type = "varchar",
                                        Length = "100",
                                        Pk = true
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var diff = new Diff(db1, db2);
            Assert.True(diff.HasDiff);

        }
        
        [Fact]
        public void Check4Test()
        {
            var db1 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                },
                                {
                                    "name", new Column()
                                    {
                                        Type = "nvarchar",
                                        Length = "100",
                                        NotNull = true
                                    }
                                }
                            },
                            Indexes = new Dictionary<string, Index>()
                            {
                                { "ix_name", new Index()
                                {
                                    Columns = new Dictionary<string, string>(){ {"name", "asc" }}
                                }}
                            }
                        }
                    }
                }
            };
            var db2 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                },
                                {
                                    "name", new Column()
                                    {
                                        Type = "nvarchar",
                                        Length = "100",
                                        NotNull = true
                                    }
                                }
                            },
                            Indexes = new Dictionary<string, Index>()
                            {
                                { "ix_name", new Index()
                                {
                                    Columns = new Dictionary<string, string>(){ {"name", "asc" }}
                                }}
                            }
                        }
                    }
                }
            };

            var diff = new Diff(db1, db2);
            Assert.False(diff.HasDiff);

        }
        
        [Fact]
        public void Check5Test()
        {
            var db1 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                },
                                {
                                    "name", new Column()
                                    {
                                        Type = "nvarchar",
                                        Length = "100",
                                        NotNull = true
                                    }
                                }
                            },
                            /*Indices = new Dictionary<string, Index>()
                            {
                                { "ix_name", new Index()
                                {
                                    Columns = new Dictionary<string, string>(){ {"name", "asc" }}
                                }}
                            }*/
                        }
                    }
                }
            };
            var db2 = new DataBase()
            {
                Tables = new Dictionary<string, Table>()
                {
                    {
                        "users", new Table()
                        {
                            Columns = new Dictionary<string, Column>()
                            {
                                {
                                    "id", new Column()
                                    {
                                        Id = true
                                    }
                                },
                                {
                                    "name", new Column()
                                    {
                                        Type = "nvarchar",
                                        Length = "100",
                                        NotNull = true
                                    }
                                }
                            },
                            Indexes = new Dictionary<string, Index>()
                            {
                                { "ix_name", new Index()
                                {
                                    Columns = new Dictionary<string, string>(){ {"name", "desc" }}
                                }}
                            }
                        }
                    }
                }
            };

            var diff = new Diff(db1, db2);
            Assert.True(diff.HasDiff);

        }
    }
}