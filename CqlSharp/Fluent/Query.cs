using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent.Querying;

namespace CqlSharp.Fluent
{
    public static class Query
    {
        /// <summary>
        /// The SELECT statements reads one or more columns for one or more rows in a table.
        /// It returns a result-set of rows, where each row contains the collection of columns corresponding to the query.
        /// </summary>
        /// <param name="tableName">The name of the table to query</param>
        /// <returns></returns>
        public static CqlSelectNamed Select(string tableName)
        {
            return new CqlSelectNamed(tableName);
        }
    }
}
