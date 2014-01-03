using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent.Manipulation;

namespace CqlSharp.Fluent.Querying
{
    public class CqlSelectNamed
    {
        private readonly CqlSelect select;
        internal CqlSelectNamed(string tableName)
        {
            this.select = new CqlSelect(tableName);
        }

        /// <summary>
        /// Use this if the SELECT statement should return the whole rows that match the WHERE clause
        /// </summary>
        /// <returns></returns>
        public CqlSelectNamedAndSelected SelectWholeRow
        {
            get { return new CqlSelectNamedAndSelected(this.select); }
        }

        /// <summary>
        /// Use this if the SELECT statement is going to return the count of the query
        /// </summary>
        /// <param name="alias"></param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected SelectCount(string alias = null)
        {
            this.select.SetCountAlias(alias);
            this.select.SetIsCountQuery(true);
            return new CqlSelectNamedAndSelected(this.select);
        }

        /// <summary>
        /// Use this if the SELECT statement is going to return specific columns (or the results of functions computed on columns)
        /// </summary>
        public CqlSelectNamedDefiner DefineSelectClauses
        {
            get { return new CqlSelectNamedDefiner(this.select); }
        }

        /// <summary>
        /// Use this if the SELECT statement is going to return the set of partition keys that match the query (a special case of DISTINCT that operates only on partition keys)
        /// </summary>
        public CqlSelectNamedDistinctDefiner SelectDistinct
        {
            get { return new CqlSelectNamedDistinctDefiner(this.select); }
        }
    }

    public class CqlSelectNamedDistinctDefiner
    {
        private readonly CqlSelect select;
        internal CqlSelectNamedDistinctDefiner(CqlSelect select)
        {
            this.select = select;
        }

        /// <summary>
        /// Add a partition key to the DISTINCT statement. All partition keys should be added
        /// </summary>
        /// <param name="partitionKey">The name of the partition key to add</param>
        /// <returns></returns>
        public CqlSelectNamedDistinctDefiner AddPartitionKey(string partitionKey)
        {
            this.select.AddSelectClause(partitionKey, null);
            return this;
        }

        /// <summary>
        /// Use after all partition keys have been added
        /// </summary>
        public CqlSelectNamedAndSelected Finished
        {
            get
            {
                return new CqlSelectNamedAndSelected(this.select);
            }
        }
    }

    public class CqlSelectNamedDefiner
    {
        private readonly CqlSelect select;
        internal CqlSelectNamedDefiner(CqlSelect select)
        {
            this.select = select;
        }

        /// <summary>
        /// Returns the value of the column
        /// </summary>
        /// <param name="column">The name of the column</param>
        /// <param name="alias">An optional alias to rename the column in the result. NOTE: Do NOT use this alias in a later WHERE clause, use the column name</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddColumn(string column, string alias = null)
        {
            this.select.AddSelectClause(column, alias);
            return this;
        }

        /// <summary>
        /// Returns the timestamp of the column
        /// </summary>
        /// <param name="column">The column to retrieve the timestamp of</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetWriteTime(string column)
        {
            this.select.AddFunctionClause("WRITETIME", new []{column});
            return this;
        }

        /// <summary>
        /// Returns the TimeToLive value for the column
        /// </summary>
        /// <param name="column">The column to retrieve the TTL value for</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetTimeToLive(string column)
        {
            this.select.AddFunctionClause("TTL", new []{column});
            return this;
        }

        /// <summary>
        /// The token function computes the token for a given partition key. The exact signature of the token function depends on the table concerned and of the partitioner used by the cluster. 
        /// The type of the arguments of the token depend on the type of the partition key columns. The return type depend on the partitioner in use: 
        /// For Murmur3Partitioner, the return type is bigint. 
        /// For RandomPartitioner, the return type is varint. 
        /// For ByteOrderedPartitioner, the return type is blob.
        /// </summary>
        /// <param name="partitionKey">The key to return the token for</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetToken(string partitionKey)
        {
            this.select.AddFunctionClause("TOKEN", new []{partitionKey});
            return this;
        }

        /// <summary>
        /// The DateOf function takes a timeuuid argument and extracts the embedded timestamp, returning it as a timestamp value.
        /// </summary>
        /// <param name="column">The column whose value will be passed into the function</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetDateOf(string column)
        {
            this.select.AddFunctionClause("dateOf", new[] { column });
            return this;
        }

        /// <summary>
        /// The UnixTimestampOf function takes a timeuuid argument and extracts the embedded timestamp, returning it as a bigint raw value.
        /// </summary>
        /// <param name="column">The column whose value will be passed into the function</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetUnixTimestampOf(string column)
        {
            this.select.AddFunctionClause("unixTimestampOf", new[] { column });
            return this;
        }

        /// <summary>
        /// The minTimeuuid function takes a timestamp value t (which can be either a timestamp or a date string)
        /// and return a fake timeuuid corresponding to the smallest possible timeuuid having timestamp t
        /// </summary>
        /// <param name="column">The column whose value will be passed into the function</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetMinTimeUUID(string column)
        {
            this.select.AddFunctionClause("minTimeuuid", new[] { column });
            return this;
        }

        /// <summary>
        /// The maxTimeuuid function takes a timestamp value t (which can be either a timestamp or a date string)
        /// and return a fake timeuuid corresponding to the largest possible timeuuid having timestamp t
        /// </summary>
        /// <param name="column">The column whose value will be passed into the function</param>
        /// <returns></returns>
        public CqlSelectNamedDefiner AddGetMaxTimeUUID(string column)
        {
            this.select.AddFunctionClause("maxTimeuuid", new[] { column });
            return this;
        }
    }

    public class CqlSelectNamedAndSelected : WhereBase<CqlSelectNamedAndSelected, CqlSelect>
    {
        internal CqlSelectNamedAndSelected(CqlSelect select)
            :base(select)
        {

        }


        private CqlSelectNamedAndSelected addRange(string colName, string customParameterName, RangeOperator operation)
        {
            var para = this.getPara(customParameterName);
            (this.update as IHasWhere<CqlSelect>).AddWhere(new RangeWhere(colName, para, operation));
            return this;
        }

        /// <summary>
        /// Define a less than range statement. For a given partition key, the clustering columns induce an ordering of rows and 
        /// relations on them is restricted to the relations that allow to select a contiguous (for the ordering) set of rows.
        /// </summary>
        /// <param name="colName">The name of the column to compare</param>
        /// <param name="valueParameterName">The name given to the value parameter</param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected AddWhereRangeLessThan(string colName, string valueParameterName = null)
        {
            return addRange(colName, valueParameterName, RangeOperator.LessThan);
        }

        /// <summary>
        /// Define a greater than range statement. For a given partition key, the clustering columns induce an ordering of rows and 
        /// relations on them is restricted to the relations that allow to select a contiguous (for the ordering) set of rows.
        /// </summary>
        /// <param name="colName">The name of the column to compare</param>
        /// <param name="valueParameterName">The name given to the value parameter</param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected AddWhereRangeGreaterThan(string colName, string valueParameterName = null)
        {
            return addRange(colName, valueParameterName, RangeOperator.GreaterThan);
        }

        /// <summary>
        /// Define a less than or equal range statement. For a given partition key, the clustering columns induce an ordering of rows and 
        /// relations on them is restricted to the relations that allow to select a contiguous (for the ordering) set of rows.
        /// </summary>
        /// <param name="colName">The name of the column to compare</param>
        /// <param name="valueParameterName">The name given to the value parameter</param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected AddWhereRangeLessThanEqual(string colName, string valueParameterName = null)
        {
            return addRange(colName, valueParameterName, RangeOperator.LessThanEqual);
        }

        /// <summary>
        /// Define a greater than or equal than range statement. For a given partition key, the clustering columns induce an ordering of rows and 
        /// relations on them is restricted to the relations that allow to select a contiguous (for the ordering) set of rows.
        /// </summary>
        /// <param name="colName">The name of the column to compare</param>
        /// <param name="valueParameterName">The name given to the value parameter</param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected AddWhereRangeGreaterThanEqual(string colName, string valueParameterName = null)
        {
            return addRange(colName, valueParameterName, RangeOperator.GreaterThanEqual);
        }

        /// <summary>
        /// When specifying relations, the TOKEN function can be used on the PARTITION KEY column to query. 
        /// In that case, rows will be selected based on the token of their PARTITION_KEY rather than on the value. 
        /// Note that the token of a key depends on the partitioner in use, and that in particular the RandomPartitioner 
        /// won’t yield a meaningful order. Also note that ordering partitioners always order token values by bytes 
        /// (so even if the partition key is of type int, token(-1) > token(0) in particular). Example: SELECT * FROM posts WHERE 
        /// token(partition_columns) > ?
        /// </summary>
        /// <param name="partitionColumns">Specify all of the partition columns</param>
        /// <param name="operation">The comparison operator</param>
        /// <param name="valueParameterName">The name of the parameter for the value to compare</param>
        /// <returns></returns>
        public CqlSelectNamedAndSelected AddWhereToken(string[] partitionColumns, RangeOperator operation, string valueParameterName = null)
        {
            var para = this.getPara(valueParameterName);
            (this.update as IHasWhere<CqlSelect>).AddWhere(new TokenWhere(partitionColumns, para, operation));
            return this;
        }
    }

    public class CqlSelect : IFluentCommand, IHasWhere<CqlSelect>
    {
        private class functionEntry
        {
            public string FunctionName { get; set; }
            public string[] FunctionArgs { get; set; }
        }

        private class orderEntry
        {
            public string Column { get; set; }
            public bool Descending { get; set; }
        }

        private bool isCountQuery = false;
        private string countAlias = null;
        private bool isDistinct = false;
        private readonly string tableName;
        private int limit = 0;
        private bool allowFiltering = false;

        private readonly Dictionary<string, string> selectClauses = new Dictionary<string, string>();
        private readonly List<functionEntry> functions = new List<functionEntry>();
        private readonly List<orderEntry> orderBy = new List<orderEntry>();
        private readonly List<IWhere> whereStmts = new List<IWhere>();

        internal CqlSelect(string tableName)
        {
            this.tableName = tableName;
        }

        internal CqlSelect AddSelectClause(string col, string alias)
        {
            this.selectClauses.Add(col, alias);
            return this;
        }

        internal CqlSelect SetCountAlias(string alias)
        {
            this.countAlias = alias;
            return this;
        }

        internal CqlSelect AddFunctionClause(string function, string[] cols)
        {
            var entry = new functionEntry { FunctionName = function, FunctionArgs = cols };
            this.functions.Add(entry);
            return this;
        }

        internal CqlSelect AddOrderBy(string col, bool desc)
        {
            this.orderBy.Add(new orderEntry { Column = col, Descending = desc });
            return this;
        }

        internal CqlSelect SetIsCountQuery(bool countQuery)
        {
            this.isCountQuery = countQuery;
            return this;
        }

        internal CqlSelect SetIsDistinct(bool distinct)
        {
            this.isDistinct = distinct;
            return this;
        }

        CqlSelect IHasWhere<CqlSelect>.AddWhere(IWhere where)
        {
            this.whereStmts.Add(where);
            return this;
        }

        /// <summary>
        /// The ORDER BY option allows to select the order of the returned results. It takes as argument a list of column names 
        /// along with the order for the column. Currently the possible orderings are limited (which depends on the table CLUSTERING ORDER): 
        /// if the table has been defined without any specific CLUSTERING ORDER, then then allowed orderings are the order induced by the
        /// clustering key and the reverse of that one. Otherwise, the orderings allowed are the order of the
        /// CLUSTERING ORDER option and the reversed one.
        /// </summary>
        /// <param name="col">The column to orderby</param>
        /// <returns></returns>
        public CqlSelect OrderByAscending(string col)
        {
            this.AddOrderBy(col, false);
            return this;
        }

        /// <summary>
        /// The ORDER BY option allows to select the order of the returned results. It takes as argument a list of column names 
        /// along with the order for the column. Currently the possible orderings are limited (which depends on the table CLUSTERING ORDER): 
        /// if the table has been defined without any specific CLUSTERING ORDER, then then allowed orderings are the order induced by the
        /// clustering key and the reverse of that one. Otherwise, the orderings allowed are the order of the
        /// CLUSTERING ORDER option and the reversed one.
        /// </summary>
        /// <param name="col">The column to orderby</param>
        /// <returns></returns>
        public CqlSelect OrderByDescending(string col)
        {
            this.AddOrderBy(col, true);
            return this;
        }

        /// <summary>
        /// The LIMIT option to a SELECT statement limits the number of rows returned by a query.
        /// </summary>
        /// <param name="numLimit">The number of rows to limit to</param>
        /// <returns></returns>
        public CqlSelect WithLimit(int numLimit)
        {
            this.limit = numLimit;
            return this;
        }

        /// <summary>
        /// By default, CQL only allows select queries that don’t involve “filtering” server side, i.e. queries where we know that all (live) record
        /// read will be returned (maybe partly) in the result set. The reasoning is that those “non filtering” queries have predictable performance
        /// in the sense that they will execute in a time that is proportional to the amount of data returned by the query (which can be controlled
        /// through LIMIT). The ALLOW FILTERING option allows to explicitly allow (some) queries that require filtering. Please note that a query using
        /// ALLOW FILTERING may thus have unpredictable performance (for the definition above), i.e. even a query that selects a handful of records may
        /// exhibit performance that depends on the total amount of data stored in the cluster.
        /// </summary>
        /// <param name="filteringAllowed">Set whether to allow filtering</param>
        /// <returns></returns>
        public CqlSelect SetAllowFiltering(bool filteringAllowed)
        {
            this.allowFiltering = filteringAllowed;
            return this;
        }

        public string BuildString
        {
            get 
            {
                var toRet = new StringBuilder();
                toRet.AppendFormat("SELECT {0} ", this.isDistinct ? "DISTINCT" : "");

                if (!this.isCountQuery)
                {
                    var allSelect = this.selectClauses.ToArray();

                    if (allSelect.Length > 0)
                    {
                        for (int i = 0; i < allSelect.Length - 1; i++)
                        {
                            toRet.AppendFormat("{0} {1}, ", allSelect[i].Key, allSelect[i].Value != null ? " AS " + allSelect[i].Value : "");
                        }
                        toRet.AppendFormat("{0} {1}", allSelect.Last().Key, allSelect.Last().Value != null ? " AS " + allSelect.Last().Value : "");

                        if (this.functions.Count > 0)
                            toRet.Append(",");
                    }

                    if (this.functions.Count > 0)
                    {
                        for (int i = 0; i < this.functions.Count - 1; i++)
                        {
                            toRet.AppendFormat("{0} ( ", this.functions[i].FunctionName);
                            for (int j = 0; j < this.functions[i].FunctionArgs.Length-1; j++)
                            {
                                toRet.AppendFormat("{0}, ", this.functions[i].FunctionArgs[j]);
                            }

                            toRet.AppendFormat("{0} ), ", this.functions[i].FunctionArgs.Last());
                        }

                        toRet.AppendFormat("{0} ( ", this.functions.Last().FunctionName);
                        for (int j = 0; j < this.functions.Last().FunctionArgs.Length - 1; j++)
                        {
                            toRet.AppendFormat("{0}, ", this.functions.Last().FunctionArgs[j]);
                        }

                        toRet.AppendFormat("{0} ) ", this.functions.Last().FunctionArgs.Last());
                    }

                    if (allSelect.Length == 0 && this.functions.Count == 0)
                        toRet.Append("* ");
                }
                else
                {
                    toRet.AppendFormat("COUNT (*) {0} ", this.countAlias != null ? "AS " + this.countAlias : "");
                }

                toRet.AppendFormat(" FROM {0} ", this.tableName);

                if (this.whereStmts.Count > 0)
                {
                    toRet.Append(" WHERE ");
                    for (int i = 0; i < this.whereStmts.Count - 1; i++)
                    {
                        toRet.AppendFormat("{0} AND ", this.whereStmts[i].WhereString);
                    }
                    toRet.AppendFormat("{0} ", this.whereStmts.Last().WhereString);
                }


                if (this.orderBy.Count != 0)
                {
                    for (int i = 0; i < this.orderBy.Count - 1; i++)
                    {
                        toRet.AppendFormat("{0} {1}, ", this.orderBy[i].Column, this.orderBy[i].Descending ? "DESC" : "ASC");
                    }
                    toRet.AppendFormat("{0} {1} ", this.orderBy.Last().Column, this.orderBy.Last().Descending ? "DESC" : "ASC");
                }

                if (this.limit != 0)
                    toRet.AppendFormat("LIMIT {0} ", this.limit);

                if (this.allowFiltering)
                    toRet.Append("ALLOW FILTERING");

                toRet.Append(";");

                return toRet.ToString();
            }
        }
    }
}
