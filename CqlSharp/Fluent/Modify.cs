﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent.Manipulation;
using CqlSharp.Serialization;

namespace CqlSharp.Fluent
{
    public static class Modify
    {
        /// <summary>
        /// The INSERT statement writes one or more columns for a given row in a table. Note that since a row is identified by its PRIMARY KEY, the columns that compose it must be specified. 
        /// Also, since a row only exists when it contains one value for a column not part of the PRIMARY KEY, one such value must be specified too. Note that unlike in SQL, INSERT does not 
        /// check the prior existence of the row by default: the row is created if none existed before, and updated otherwise. Furthermore, there is no mean to know which of creation or 
        /// update happened.
        /// </summary>
        /// <param name="tableName">The name of the table to insert into</param>
        /// <returns></returns>
        public static CqlInsertNamed Insert(string tableName)
        {
            return new CqlInsertNamed(tableName);
        }

        /// <summary>
        /// Prepare an insert statment for a type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static CqlInsert Insert<T>()
        {
            var accessor = ObjectAccessor<T>.Instance;
            return new CqlInsertNamed(accessor.KeySpaceTableAndName)
                .With(accessor.PartitionKeys.Union(accessor.ClusteringKeys).Union(accessor.NormalColumns))
                .Finished;
        }

        /// <summary>
        /// The UPDATE statement writes one or more columns for a given row in a table. The where clause is used to select the row to update 
        /// and must include all columns composing the PRIMARY KEY (the IN relation is only supported for the last column of the partition key). 
        /// Other columns values are specified through the set clauses.
        /// </summary>
        /// <param name="tableName">The name of the table to update rows in</param>
        /// <returns></returns>
        public static CqlUpdateNamed Update(string tableName)
        {
            return new CqlUpdateNamed(tableName);
        }


        /// <summary>
        /// Creates a basic update statement with a Where for all primary keys and a set for all other columns
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static CqlUpdate Update<T>()
        {
            var accessor = ObjectAccessor<T>.Instance;
            var toRet = new CqlUpdateNamed(accessor.KeySpaceTableAndName);
            foreach (var col in accessor.NormalColumns)
            {
                toRet = toRet.AddAssignment(col.ColumnName);
            }

            var finishedSet = toRet.FinishedSet;
            foreach (var col in accessor.PartitionKeys.Union(accessor.ClusteringKeys))
            {
                finishedSet = finishedSet.AddWhere(col.ColumnName);
            }

            return finishedSet.FinishedWhere;
        }

        /// <summary>
        /// Use to attach all the primary keys to an update statement with a custom set statement
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="set"></param>
        /// <returns></returns>
        public static CqlUpdate WithStandardWhere<T>(this CqlUpdateNamedSet set)
        {
            var accessor = ObjectAccessor<T>.Instance;
            foreach (var col in accessor.PartitionKeys.Union(accessor.ClusteringKeys))
            {
                set = set.AddWhere(col.ColumnName);
            }
            return set.FinishedWhere;
        }

        /// <summary>
        /// The DELETE statement deletes columns and rows. If column names are provided directly after the DELETE keyword, only those columns are deleted
        /// from the row indicated by the where clause. Otherwise whole rows are removed.
        /// </summary>
        /// <param name="tableName">The name of the table to delete rows from</param>
        /// <returns></returns>
        public static CqlDeleteNamed Delete(string tableName)
        {
            return new CqlDeleteNamed(tableName);
        }

        /// <summary>
        /// The BATCH statement groups multiple modification statements (insertions/updates and deletions) into a single statement. 
        /// It mainly serves two purposes: it saves network round-trips between the client and the server (and sometimes between the 
        /// server coordinator and the replicas) when batching multiple updates. all updates in a BATCH belonging to a given partition 
        /// key are performed atomically and in isolation Note however that the BATCH statement only allows UPDATE, INSERT and DELETE 
        /// statements and is not a full analogue for SQL transactions.  
        /// </summary>
        /// <returns></returns>
        public static CqlBatchDefiner Batch()
        {
            return new CqlBatchDefiner();
        }
    }
}
