using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent.Manipulation;

namespace CqlSharp.Fluent
{
    public static class Manipulate
    {
        /// <summary>
        /// The INSERT statement writes one or more columns for a given row in a table. Note that since a row is identified by its PRIMARY KEY, the columns that compose it must be specified. 
        /// Also, since a row only exists when it contains one value for a column not part of the PRIMARY KEY, one such value must be specified too. Note that unlike in SQL, INSERT does not 
        /// check the prior existence of the row by default: the row is created if none existed before, and updated otherwise. Furthermore, there is no mean to know which of creation or 
        /// update happened.
        /// </summary>
        /// <param name="tableName">The name of the table to insert into</param>
        /// <returns></returns>
        public static CqlInsert Insert(string tableName)
        {
            return new CqlInsert(tableName);
        }


    }
}
