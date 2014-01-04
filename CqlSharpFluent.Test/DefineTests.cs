using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent;
using CqlSharp.Serialization;
using Xunit;

namespace CqlSharpFluent.Test
{
    public class DefineTests
    {
        [CqlTable("Test1", Keyspace="Test")]
        class TestClass1
        {
            [CqlColumn("id", PartitionKeyIndex = 1)]
            public Guid Id { get; set; }

            public int TestInt { get; set; }

            [CqlColumn("name", ClusteringKeyIndex=1)]
            public string Name { get; set; }

            [CqlIgnore]
            public DateTime Time { get; set; }
        }

        [CqlTable("Test2")]
        class TestClass2 // should fail to build table because no partition key set
        {
            public int Test { get; set; } 
        }

        [CqlTable("Test3")]
        class TestClass3 // should fail to build table because no value column set
        {
            [CqlColumn("id", PartitionKeyIndex = 1)]
            public Guid Id { get; set; }

            [CqlColumn("name", PartitionKeyIndex = 2)]
            public string Name { get; set; }

            [CqlColumn("isAdmin", ClusteringKeyIndex = 1)]
            public bool IsAdmin { get; set; }

        }

        [CqlTable("Test4")]
        class TestClass4 // should fail to build table because no value set
        {
            [CqlColumn("id", PartitionKeyIndex = 1)]
            public Guid Id { get; set; }

            [CqlColumn("name", PartitionKeyIndex = 2)]
            public string Name { get; set; }

            [CqlColumn("isAdmin", ClusteringKeyIndex = 1)]
            public bool IsAdmin { get; set; }

            [CqlColumn("value1", IndexName="valueIndex1")]
            public string Value { get; set; }

            [CqlColumn("value2", IndexName = "valueIndex2")]
            public int Value2 { get; set; }

            public HashSet<string> Set { get; set; }

            public List<int> List { get; set; }

            public Dictionary<int, DateTime> Dictionary { get; set; }
        }

        #region CreateKeyspace

        [Fact]
        public void CreateKeyspace()
        {
            var create = Define.CreateKeyspace("Excelsior").WithSimpleStrategy(3);
            Assert.Equal(
                "CREATE KEYSPACE Excelsior WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceNoError()
        {
            var create = Define.CreateKeyspace("Excelsior").WithSimpleStrategy(3).SetThrowOnError(false);
            Assert.Equal(
                "CREATE KEYSPACE IF NOT EXISTS Excelsior WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceDurableFalse()
        {
            var create = Define.CreateKeyspace("Excelsior").WithSimpleStrategy(3).SetDurability(false);
            Assert.Equal(
                "CREATE KEYSPACE Excelsior WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3} AND durable_writes = false;",
                create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceNoErrorAndDurableFalse()
        {
            var create = Define.CreateKeyspace("Excelsior").WithSimpleStrategy(3).SetThrowOnError(false).SetDurability(false);
            Assert.Equal(
                "CREATE KEYSPACE IF NOT EXISTS Excelsior WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3} AND durable_writes = false;",
                create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceNetwork()
        {
            var create = Define.CreateKeyspace("Excelsior").WithNetworkTopologyStrategy(new Dictionary<string, int>{{"DC1",1},{"DC2",3}}).SetThrowOnError(false).SetDurability(false);
            Assert.Equal(
                "CREATE KEYSPACE IF NOT EXISTS Excelsior WITH replication = {'class' : 'NetworkTopologyStrategy', 'DC1' : 1, 'DC2' : 3} AND durable_writes = false;",
                create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceFromClass()
        {
            var create = Define.CreateKeyspace<TestClass1>().WithSimpleStrategy(3);
            Assert.Equal(
               "CREATE KEYSPACE Test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
               create.BuildString);
        }

        [Fact]
        public void CreateKeyspaceFromClassInvalid()
        {
            Assert.Throws<InvalidOperationException>(() => Define.CreateKeyspace<TestClass2>().WithSimpleStrategy(3));
        }

        #endregion

        [Fact]
        public void Use()
        {
            Assert.Equal("USE myApp;", Define.Use("myApp").BuildString);
        }

        #region AlterKeyspace

        [Fact]
        public void AlterKeyspace()
        {
            var alter = Define.AlterKeyspace("Excelsior").WithSimpleStrategy(4);
            Assert.Equal(
                "ALTER KEYSPACE Excelsior WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 4};",
                alter.BuildString);
        }

        [Fact]
        public void AlterKeyspaceClass()
        {
            var alter = Define.AlterKeyspace<TestClass1>().WithSimpleStrategy(4);
            Assert.Equal(
                "ALTER KEYSPACE Test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 4};",
                alter.BuildString);
        }

        [Fact]
        public void AlterKeyspaceClassThrows()
        {
            Assert.Throws<InvalidOperationException>(() => Define.AlterKeyspace<TestClass2>().WithSimpleStrategy(4));
        }

        #endregion

        #region DropKeyspace

        [Fact]
        public void DropKeyspace()
        {
            Assert.Equal("DROP KEYSPACE myApp;", Define.DropKeyspace("myApp").BuildString);
        }

        [Fact]
        public void DropKeyspaceNoError()
        {
            Assert.Equal("DROP KEYSPACE IF EXISTS myApp;", Define.DropKeyspace("myApp").SetThrowOnError(false).BuildString);
        }

        #endregion

        #region CreateTable
        // WITH compaction = { 'class' : 'LeveledCompactionStrategy' };
        [Fact]
        public void CreateTable()
        {
            var str =
                "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time));";

            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof (Guid))
                .AddClusteringKey("posted_month", typeof (int))
                .AddClusteringKey("posted_time", typeof (DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof (string))
                .AddColumn("posted_by", typeof (string))
                .FinishedDefiningColumns;

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCompaction()
        {
            var str =
                "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH compaction = {'class' : 'LeveledCompactionStrategy'};";

            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithLeveledCompaction();

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCompactionWithOptions()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH compaction = {'class' : 'LeveledCompactionStrategy', 'tombstone_threshold' : 0.5, 'tombstone_compaction_interval' : 1};";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithLeveledCompaction(.5, 1);

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCompactionWithOptionsErrored()
        {
            Assert.Throws<ArgumentException>(() => Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithLeveledCompaction(.5, tombstoneCompactionInterval: 0)); //tombstoneCompaction has to be greater than 0
        }

        [Fact]
        public void CreateTableComment()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH comment = 'This is a comment';";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithComment("This is a comment");

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCommentAndBloom()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH bloom_filter_fp_chance = 0.2 AND comment = 'This is a comment';";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithBloomFilterFalsePositiveChance(0.2)
                .WithComment("This is a comment");

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCommentAndCachingAndBloom()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH bloom_filter_fp_chance = 0.2 AND caching = 'all' AND comment = 'This is a comment';";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithBloomFilterFalsePositiveChance(0.2)
                .WithCaching("all")
                .WithComment("This is a comment");

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableClusteringOrder()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH CLUSTERING ORDER;";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithClusterOrdering(true);

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableClusteringOrderAndOption()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH CLUSTERING ORDER AND dclocal_read_repair_chance = 0.2;";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithClusterOrdering(true)
                .WithDcLocalRepairChance(0.2);

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableClusteringOrderAndCompactStorageAndOption()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH CLUSTERING ORDER AND COMPACT STORAGE AND dclocal_read_repair_chance = 0.2;";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithClusterOrdering(true)
                .WithCompactStorage(true)
                .WithDcLocalRepairChance(0.2);

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableCompressor()
        {
            var str = "CREATE TABLE timeline (userid uuid, posted_month int, posted_time timestamp, body varchar, posted_by varchar, PRIMARY KEY (userid, posted_month, posted_time)) WITH compression = {'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 32};";
            var create = Define.CreateTable("timeline")
                .HasPartitionKey("userid", typeof(Guid))
                .AddClusteringKey("posted_month", typeof(int))
                .AddClusteringKey("posted_time", typeof(DateTime))
                .FinishedClusteringKeys
                .AddColumn("body", typeof(string))
                .AddColumn("posted_by", typeof(string))
                .FinishedDefiningColumns
                .WithCompression("DeflateCompressor", 32);

            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableFromClass()
        {
            var str = "CREATE TABLE Test.Test1 (id uuid, name varchar, testint int, PRIMARY KEY (id, name));";
            var create = Define.CreateTable<TestClass1>();
            Assert.Equal(str, create.BuildString);
        }

        [Fact]
        public void CreateTableFromClass2()
        {
           var ex = Assert.Throws<InvalidOperationException>(() => Define.CreateTable<TestClass2>().BuildString);
           Assert.Equal("There must be a PartitionKey", ex.Message);
        }

        [Fact]
        public void CreateTableFromClass3()
        {
            var ex = Assert.Throws<InvalidOperationException>(() => Define.CreateTable<TestClass3>().BuildString);
            Assert.Equal("There must be at least one value column (not a clustering or partition key)", ex.Message);
        }

        [Fact]
        public void CreateTableFromClass4()
        {
            var str = "CREATE TABLE Test4 (id uuid, name varchar, isAdmin boolean, value1 varchar, value2 int, set set<varchar>, list list<int>, dictionary map<int,timestamp>, PRIMARY KEY ((id, name), isAdmin));";
            var create = Define.CreateTable<TestClass4>();
            Assert.Equal(str, create.BuildString);
        }
        #endregion

        #region AlterTable

        [Fact]
        public void AlterTable()
        {
            var str = "ALTER TABLE addamsFamily ALTER lastKnownLocation TYPE uuid;";
            var alter = Define.AlterTable("addamsFamily")
                    .AlterColumn("lastKnownLocation", typeof(Guid));
            Assert.Equal(str, alter.BuildString);
        }

        [Fact]
        public void AlterTableAddColumn()
        {
            var str = "ALTER TABLE addamsFamily ADD gravesite varchar;";
            var alter = Define.AlterTable("addamsFamily")
                    .AddColumn("gravesite", typeof(string));
            Assert.Equal(str, alter.BuildString);
        }

        [Fact]
        public void AlterTableProperty()
        {
            var str = "ALTER TABLE addamsFamily WITH comment = 'A most excellent and useful column family' AND read_repair_chance = 0.2;";
            var alter = Define.AlterTable("addamsFamily")
                .ChangeOptions()
                .WithComment("A most excellent and useful column family")
                .WithReadRepairChance(0.2);
            Assert.Equal(str, alter.BuildString);
        }

        [Fact]
        public void AlterTableDropColumn()
        {
            var str = "ALTER TABLE addamsFamily DROP gravesite;";
            var alter = Define.AlterTable("addamsFamily")
                    .DropColumn("gravesite");
            Assert.Equal(str, alter.BuildString);
        }
        #endregion

        #region DropTable

        [Fact]
        public void DropTable()
        {
            var str = "DROP TABLE worldSeriesAttendees;";
            var drop = Define.DropTable("worldSeriesAttendees");
            Assert.Equal(str, drop.BuildString);
        }

        [Fact]
        public void DropTableNoError()
        {
            var str = "DROP TABLE IF EXISTS worldSeriesAttendees;";
            var drop = Define.DropTable("worldSeriesAttendees").SetThrowOnError(false);
            Assert.Equal(str, drop.BuildString);
        }

        [Fact]
        public void DropTableClass1()
        {
            var str = "DROP TABLE Test.Test1;";
            var drop = Define.DropTable<TestClass1>();
            Assert.Equal(str, drop.BuildString);
        }
               
        [Fact]
        public void DropTableClass2()
        {
            var str = "DROP TABLE Test2;";
            var drop = Define.DropTable<TestClass2>();
            Assert.Equal(str, drop.BuildString);
        }

        #endregion

        #region Truncate
        [Fact]
        public void Truncate()
        {
            var str = "TRUNCATE superImportantData;";
            var truncate = Define.TruncateTable("superImportantData");
            Assert.Equal(str, truncate.BuildString);
        }

        [Fact]
        public void TruncateClass1()
        {
            var str = "TRUNCATE Test.Test1;";
            var truncate = Define.TruncateTable<TestClass1>();
            Assert.Equal(str, truncate.BuildString);
        }

        [Fact]
        public void TruncateClass2()
        {
            var str = "TRUNCATE Test2;";
            var truncate = Define.TruncateTable<TestClass2>();
            Assert.Equal(str, truncate.BuildString);
        }
        #endregion

        #region CreateIndex

        [Fact]
        public void CreateIndex()
        {
            var str = "CREATE INDEX userIndex ON NerdMovies (user);";
            var createIndex = Define.CreateIndex("NerdMovies")
                .OnColumn("user")
                .WithName("userIndex");
            Assert.Equal(str, createIndex.BuildString);
        }

        [Fact]
        public void CreateIndexNoError()
        {
            var str = "CREATE INDEX IF NOT EXISTS userIndex ON NerdMovies (user);";
            var createIndex = Define.CreateIndex("NerdMovies")
                .OnColumn("user")
                .WithName("userIndex")
                .SetThrowOnError(false);
            Assert.Equal(str, createIndex.BuildString);
        }

        [Fact]
        public void CreateIndexNoName()
        {
            var str = "CREATE INDEX ON Mutants (abilityId);";
            var createIndex = Define.CreateIndex("Mutants")
                .OnColumn("abilityId");
            Assert.Equal(str, createIndex.BuildString);
        }

        [Fact]
        public void CreateCustomIndex()
        {
            var str = "CREATE CUSTOM INDEX ON users (email) USING 'path.to.the.IndexClass';";
            var createIndex = Define.CreateIndex("users")
                .OnColumn("email")
                .WithCustomClass("path.to.the.IndexClass");
            Assert.Equal(str, createIndex.BuildString);
        }

        [Fact]
        public void CreateIndexesFromClass()
        {
            var createIndex = Define.CreateIndexes<TestClass4>();
            Assert.Equal<int>(2, createIndex.Length);
            for (int i = 0; i < createIndex.Length; i++)
            {
                var str = String.Format("CREATE INDEX valueIndex{0} ON Test4 (value{0});", (i+1) );
                Assert.Equal(str, createIndex[i].BuildString);
            }
        }
        #endregion

        #region DropIndex

        [Fact]
        public void DropIndex()
        {
            var str = "DROP INDEX userIndex;";
            var drop = Define.DropIndex("userIndex");
            Assert.Equal(str, drop.BuildString);
        }

        [Fact]
        public void DropIndexNoError()
        {
            var str = "DROP INDEX IF EXISTS userIndex;";
            var drop = Define.DropIndex("userIndex")
                .SetThrowOnError(false);
            Assert.Equal(str, drop.BuildString);
        }

        #endregion
    }
}
