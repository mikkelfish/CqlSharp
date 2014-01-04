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
    public class ModifyTests
    {
        #region UpdateTests

        [Fact]
        public void Update()
        {
            var str = "UPDATE NerdMovies USING TTL ? SET director = ?, main_actor = ?, year = ? WHERE movie = ?;";
            var update = Modify.Update("NerdMovies")
                .AddAssignment("director")
                .AddAssignment("main_actor")
                .AddAssignment("year")
                .FinishedSet
                .AddWhere("movie")
                .FinishedWhere
                .WithTimeToLive();

            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateWithNamed()
        {
            var str = "UPDATE NerdMovies USING TTL ? SET director = ?, main_actor = ?, year = :customYear WHERE movie = ? AND date = :customDate;";
            var update = Modify.Update("NerdMovies")
                .AddAssignment("director")
                .AddAssignment("main_actor")
                .AddAssignment("year", "customYear")
                .FinishedSet
                .AddWhere("movie")
                .AddWhere("date","customDate")
                .FinishedWhere
                .WithTimeToLive();

            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateSet()
        {
            var str = "UPDATE UserActions SET total = total + ? WHERE user = ? AND action = ?;";
            var update = Modify.Update("UserActions")
                .AddCounterIncrement("total")
                .FinishedSet
                .AddWhere("user")
                .AddWhere("action")
                .FinishedWhere;
            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateAddMapItem()
        {
            var str = "UPDATE UserActions SET map = map + ? WHERE user = ? AND action = ?;";
            var update = Modify.Update("UserActions")
                .AddMapItem("map")
                .FinishedSet
                .AddWhere("user")
                .AddWhere("action")
                .FinishedWhere;
            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateSetMapItem()
        {
            var str = "UPDATE UserActions SET map[:key] = :value WHERE user = ? AND action = ?;";
            var update = Modify.Update("UserActions")
                .AddMapSet("map", "key", "value")
                .FinishedSet
                .AddWhere("user")
                .AddWhere("action")
                .FinishedWhere;
            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateSetIn()
        {
            var str = "UPDATE UserActions SET name = ? WHERE user IN :inVariable;";
            var update = Modify.Update("UserActions")
                .AddAssignment("name")
                .FinishedSet
                .AddWhereIn("user", "inVariable")
                .FinishedWhere;
            Assert.Equal(str, update.BuildString);
        }

        [CqlTable("Test1", Keyspace = "Test")]
        class TestClass1
        {
            [CqlColumn("id", PartitionKeyIndex = 1)]
            public Guid Id { get; set; }

            public int TestInt { get; set; }

            [CqlColumn("name", ClusteringKeyIndex = 1)]
            public string Name { get; set; }

            [CqlIgnore]
            public DateTime Time { get; set; }
        }

        [Fact]
        public void UpdateWithClass()
        {
            var str = "UPDATE Test.Test1 SET testint = ? WHERE id = ? AND name = ?;";
            var update = Modify.Update<TestClass1>();
            Assert.Equal(str, update.BuildString);
        }

        [Fact]
        public void UpdateWithClassCustom()
        {
            var str = "UPDATE Test.Test1 SET custom = :customVar WHERE id = ? AND name = ?;";
            var update = Modify.Update(Utilities.Table<TestClass1>())
                .AddAssignment("custom", "customVar")
                .FinishedSet
                .WithStandardWhere<TestClass1>();
            Assert.Equal(str, update.BuildString);
        }
        #endregion

        #region DeleteTests
        [Fact]
        public void Delete()
        {
            var str = "DELETE FROM NerdMovies USING TIMESTAMP ? WHERE movie = ?;";
            var del = Modify.Delete("NerdMovies")
                .DeletesDefined
                .AddWhere("movie")
                .FinishedWhere
                .WithTimestamp();
            Assert.Equal(str, del.BuildString);
        }

        [Fact]
        public void DeleteWithIn()
        {
            var str = "DELETE phone FROM Users WHERE userid IN ?;";
            var del = Modify.Delete("Users")
                .DeleteColumn("phone")
                .DeletesDefined
                .AddWhereIn("userid")
                .FinishedWhere;
            Assert.Equal(str, del.BuildString);
        }

        #endregion

        #region BatchTests
        [Fact]
        public void BatchTest()
        {
            var str = "BEGIN BATCH INSERT INTO users (userid, password, name) VALUES (?, ?, ?); UPDATE users SET password = ? WHERE userid = ?; INSERT INTO users (userid, password) VALUES (?, ?); DELETE name FROM users WHERE userid = ?; APPLY BATCH;";
            var batch = Modify.Batch()
                .AddCommand(
                    Modify.Insert("users").With("userid").With("password").With("name").Finished)
                .AddCommand(
                    Modify.Update("users").AddAssignment("password").FinishedSet.AddWhere("userid").FinishedWhere)
                .AddCommand(
                    Modify.Insert("users").With("userid").With("password").Finished)
                .AddCommand(
                    Modify.Delete("users").DeleteColumn("name").DeletesDefined.AddWhere("userid").FinishedWhere)
                .BatchDefined;
            Assert.Equal(str, batch.BuildString);
        }

        [Fact]
        public void BatchTestWithTime()
        {
            var str = "BEGIN BATCH USING TIMESTAMP ? INSERT INTO users (userid, password, name) VALUES (?, ?, ?); UPDATE users SET password = ? WHERE userid = ?; INSERT INTO users (userid, password) VALUES (?, ?); DELETE name FROM users WHERE userid = ?; APPLY BATCH;";
            var batch = Modify.Batch()
                .AddCommand(
                    Modify.Insert("users").With("userid").With("password").With("name").Finished)
                .AddCommand(
                    Modify.Update("users").AddAssignment("password").FinishedSet.AddWhere("userid").FinishedWhere)
                .AddCommand(
                    Modify.Insert("users").With("userid").With("password").Finished)
                .AddCommand(
                    Modify.Delete("users").DeleteColumn("name").DeletesDefined.AddWhere("userid").FinishedWhere)
                .BatchDefined
                .WithTimestamp();
            Assert.Equal(str, batch.BuildString);
        }

        #endregion
    }
}
