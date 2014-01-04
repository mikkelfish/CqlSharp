using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent;
using Xunit;

namespace CqlSharpFluent.Test
{
    public class SelectTests
    {
        [Fact]
        public void SelectBasic()
        {
            var str = "SELECT name, occupation FROM users WHERE userid IN ?;";
            var select = Query.Select("users")
                .DefineSelectClauses
                .AddColumn("name")
                .AddColumn("occupation")
                .FinishedSelect
                .AddWhereIn("userid")
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
                
        }

        [Fact]
        public void SelectAs()
        {
            var str = "SELECT name AS user_name, occupation AS user_occupation FROM users;";
            var select = Query.Select("users")
                .DefineSelectClauses
                .AddColumn("name", "user_name")
                .AddColumn("occupation", "user_occupation")
                .FinishedSelect
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectRange()
        {
            var str = "SELECT time, value FROM events WHERE event_type = ? AND time > :time_min AND time <= :time_max;";
            var select = Query.Select("events")
                .DefineSelectClauses
                .AddColumn("time")
                .AddColumn("value")
                .FinishedSelect
                .AddWhere("event_type")
                .AddWhereRangeGreaterThan("time", "time_min")
                .AddWhereRangeLessThanEqual("time", "time_max")
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectCount()
        {
            var str = "SELECT COUNT(*) FROM users;";
            var select = Query.Select("users")
                .SelectCount()
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectCountAs()
        {
            var str = "SELECT COUNT(*) AS user_count FROM users;";
            var select = Query.Select("users")
                .SelectCount("user_count")
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectToken()
        {
            var str = "SELECT COUNT(*) FROM users WHERE TOKEN(p1, p2) = ? AND col = ?;";
            var select = Query.Select("users")
                .SelectCount()
                .AddWhereToken(new []{"p1","p2"}, CqlSharp.Fluent.Manipulation.RangeOperator.Equal)
                .AddWhere("col")
                .FinishedWhere;
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectWithTime()
        {
            var str = "SELECT * FROM users WHERE col = ? ORDER BY sort ASC ALLOW FILTERING;";
            var select = Query.Select("users")
                .SelectWholeRow
                .AddWhere("col")
                .FinishedWhere
                .OrderByAscending("sort")
                .SetAllowFiltering(true);
            Assert.Equal(str, select.BuildString);
        }

        [Fact]
        public void SelectWithLimit()
        {
            var str = "SELECT name, occupation FROM users WHERE userid IN ? LIMIT 10;";
            var select = Query.Select("users")
                .DefineSelectClauses
                .AddColumn("name")
                .AddColumn("occupation")
                .FinishedSelect
                .AddWhereIn("userid")
                .FinishedWhere
                .WithLimit(10);
            Assert.Equal(str, select.BuildString);

        }

        [Fact]
        public void SelectWithFunc()
        {
            var str = "SELECT name, occupation, dateOf(date) FROM users WHERE userid IN ? LIMIT 10;";
            var select = Query.Select("users")
                .DefineSelectClauses
                .AddColumn("name")
                .AddColumn("occupation")
                .AddGetDateOf("date")
                .FinishedSelect
                .AddWhereIn("userid")
                .FinishedWhere
                .WithLimit(10);
            Assert.Equal(str, select.BuildString);
        }
    }
}
