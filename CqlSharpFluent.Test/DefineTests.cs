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
            
        }

        [CqlTable("Test2")]
        class TestClass2
        {

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
                "CREATE TABLE timeline (userid uuid, posted_month int, posted_time uuid, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_time))";

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
        #endregion
    }
}
