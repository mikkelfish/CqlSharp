using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Manipulation
{
    public class CqlUpdateNamed
    {
        private readonly CqlUpdate update;

        internal CqlUpdateNamed(string tableName)
        {
            this.update = new CqlUpdate(tableName);
        }

        private string getPara(string customParameterName)
        {
            return customParameterName == null ? "?" : ":" + customParameterName;
        }

        /// <summary>
        /// Use this for a simple assignment of a column, e.g. year = ? 
        /// </summary>
        /// <param name="colName">The name of the column to assign</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the column name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddAssignment(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.SimpleAssign(colName, para));
            return this;
        }

        /// <summary>
        /// Use this for incrementing a counter, e.g. counter = counter + ?
        /// </summary>
        /// <param name="counterName">The name of the counter to increment</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the counter name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddCounterIncrement(string counterName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Append(counterName, para));
            return this;
        }

        /// <summary>
        /// Use this for decrementing a counter, e.g. counter = counter - ?
        /// </summary>
        /// <param name="counterName">The name of the counter to increment</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the counter name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddCounterDecrement(string counterName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Remove(counterName, para));
            return this;
        }

        /// <summary>
        /// Use this for prepending an item to a list, e.g. list = list.InsertAt(0, ?)
        /// </summary>
        /// <param name="colName">The name of the list to prepend to</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the list name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddListPrepend(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Prepend(colName, para));
            return this;
        }

        /// <summary>
        /// Use this for appending an item to a list, e.g. list.Add(?)
        /// </summary>
        /// <param name="colName">The name of the list to append to</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the list name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddListAppend(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Append(colName, para));
            return this;
        }

        /// <summary>
        /// Use this for inserting an item into a set, e.g. set.Add(?)
        /// </summary>
        /// <param name="colName">The name of the set to insert into</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the set name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddSetItem(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Append(colName, para));
            return this;
        }

        /// <summary>
        /// Use this for deleting an item from a set, e.g. set.Remove(?)
        /// </summary>
        /// <param name="colName">The name of the set to remove from</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the set name when setting command parameters.</param>
        /// <returns></returns>
        public CqlUpdateNamed AddSetItemDeletion(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddAssignment(new CqlUpdate.Remove(colName, para));
            return this;
        }

        /// <summary>
        /// Use this for changing an item in a list, e.g. list[?] = ?
        /// </summary>
        /// <param name="colName">The name of the list to change</param>
        /// <param name="indexParameterName">The key parameter name</param>
        /// <param name="valueParameterName">The value parameter name</param>
        /// <returns></returns>
        public CqlUpdateNamed AddListSet(string colName, string indexParameterName = null, string valueParameterName = null)
        {
            var indexPara = this.getPara(indexParameterName);
            var valPara = this.getPara(valueParameterName);
            this.update.AddAssignment(new CqlUpdate.Indexed(colName, indexPara, valPara));
            return this;
        }

        /// <summary>
        /// Use this for changing an item in a map, e.g. map[?] = ?
        /// </summary>
        /// <param name="colName">The name of the map to change</param>
        /// <param name="keyParameterName">The key parameter name</param>
        /// <param name="valueParameterName">The value parameter name</param>
        /// <returns></returns>
        public CqlUpdateNamed AddMapSet(string colName, string keyParameterName, string valueParameterName)
        {
            var keyPara = this.getPara(keyParameterName);
            var valPara = this.getPara(keyParameterName);
            this.update.AddAssignment(new CqlUpdate.Indexed(colName, keyPara, valPara));
            return this;
        }

        /// <summary>
        /// Use this for adding an item to a map, e.g. map.Add(?,?)
        /// </summary>
        /// <param name="colName">The name of the map to add to</param>
        /// <param name="keyParameterName">The key parameter name</param>
        /// <param name="valueParameterName">The value parameter name</param>
        /// <returns></returns>
        public CqlUpdateNamed AddMapItem(string colName, string keyParameterName, string valueParameterName)
        {
            var keyPara = this.getPara(keyParameterName);
            var valPara = this.getPara(keyParameterName);
            this.update.AddAssignment(new CqlUpdate.MapAdd(colName, keyPara, valPara));
            return this;
        }

        /// <summary>
        /// Use this when all set clauses have been added
        /// </summary>
        public CqlUpdateNamedSet FinishedSet
        {
            //TODO Add exceptions
            get { return new CqlUpdateNamedSet(this.update); }
        }
    }

    public class CqlUpdateNamedSet
    {
        private readonly CqlUpdate update;

        internal CqlUpdateNamedSet(CqlUpdate update)
        {
            this.update = update;
        }

        private string getPara(string customParameterName)
        {
            return customParameterName == null ? "?" : ":" + customParameterName;
        }

        /// <summary>
        /// Add a WHERE relation, e.g. WHERE age = ?
        /// </summary>
        /// <param name="colName">The name of the column (must be the last column of the partition key)</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the column name when setting command parameters</param>
        /// <returns></returns>
        public CqlUpdateNamedSet AddWhereRelation(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddWhere(new CqlUpdate.SimpleWhere(colName, para));
            return this;
        }

        /// <summary>
        /// The IN relation is only supported for the last column of the partition key, e.g. WHERE age IN ?
        /// </summary>
        /// <param name="colName">The name of the column (must be the last column of the partition key)</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the column name when setting command parameters</param>
        /// <returns></returns>
        public CqlUpdateNamedSet AddInWhere(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddWhere(new CqlUpdate.InWhere(colName, para));
            return this;
        }

        /// <summary>
        /// Use this when all where clauses have been added. NOTE: All primary keys (partition + cluster) must have a where clause associated with them for the update to execute
        /// </summary>
        public CqlUpdate FinishedWhere
        {
            get
            {
                //TODO Add exceptions
                return this.update;
            }
        }
    }

    public class CqlUpdateNamedSetAndWhere
    {
        private readonly CqlUpdate update;

        internal CqlUpdateNamedSetAndWhere(CqlUpdate update)
        {
            this.update = update;
        }

        /// <summary>
        /// Add an If clause, in which case the row will not be updated unless such condition are met. 
        /// But please note that using IF conditions will incur a non negligible performance cost (internally, Paxos will be used) so this should be used sparingly.
        /// </summary>
        /// <param name="colName">The name of the column</param>
        /// <param name="parameterName">The name of the parameter</param>
        /// <returns></returns>
        public CqlUpdateNamedSetAndWhere AddIf(string colName, string parameterName)
        {
            this.update.AddIf(new CqlUpdate.SimpleWhere(colName, parameterName));
            return this;
        }

        /// <summary>
        /// Use this when all If clauses have been added
        /// </summary>
        public CqlUpdate FinishedIf
        {
            get { return this.update; }
        }
    }

    public class CqlUpdate : IFluentCommand
    {
        #region Assignment

        internal interface IAssign
        {
            string AssignString { get; }
        }

        internal class SimpleAssign : IAssign
        {
            private readonly string colName;
            private readonly string paramName;

            public SimpleAssign(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string AssignString
            {
                get { return String.Format("{0} = {1}", this.colName, this.paramName); }
            }
        }

        internal class Prepend : IAssign
        {
             private readonly string colName;
            private readonly string paramName;

            public Prepend(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string AssignString
            {
                get { return String.Format("{0} = {1} + {0}", this.colName, this.paramName); }
            }
        }

        internal class Append : IAssign
        {
             private readonly string colName;
            private readonly string paramName;

            public Append(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string AssignString
            {
                get { return String.Format("{0} = {0} + {1}", this.colName, this.paramName); }
            }

        }

        internal class MapAdd : IAssign
        {
             private readonly string colName;
            private readonly string valParam;
            private readonly string keyParam;

            public MapAdd(string colname, string lookupParam, string valParam)
            {
                this.colName = colname;
                this.keyParam = lookupParam;
                this.valParam = valParam;
            }

            public string AssignString
            {
                get
                {
                    var toRet = new StringBuilder();
                    toRet.AppendFormat("{0} = ", this.colName);
                    toRet.Append("{");
                    toRet.AppendFormat("{0} : {1}", this.keyParam, this.valParam);
                    toRet.Append("}");
                    return toRet.ToString();
                }
            }

        }

        internal class Remove : IAssign
        {
            private readonly string colName;
            private readonly string paramName;

            public Remove(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string AssignString
            {
                get { return String.Format("{0} = {0} - {1}", this.colName, this.paramName); }
            }

        }

        internal class Indexed : IAssign
        {
            private readonly string colName;
            private readonly string valParam;
            private readonly string lookupParam;

            public Indexed(string colname, string lookupParam, string valParam)
            {
                this.colName = colname;
                this.lookupParam = lookupParam;
                this.valParam = valParam;
            }

            public string AssignString
            {
                get { return String.Format("{0}[{1}] = {2}", this.colName, this.lookupParam, this.valParam); }
            }

        }


        #endregion

        #region Where

        internal interface IWhere 
        {
            string WhereString { get; }
        }

        internal class SimpleWhere : IWhere
        {
            private readonly string colName;
            private readonly string paramName;

            public SimpleWhere(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string WhereString
            {
                get { return String.Format("{0} = {1}", this.colName, this.paramName); }
            }
        }

        internal class InWhere : IWhere
        {
            private readonly string colName;
            private readonly string paramName;

            public InWhere(string colname, string param)
            {
                this.colName = colname;
                this.paramName = param;
            }

            public string WhereString
            {
                get { return String.Format("{0} IN {1}", this.colName, this.paramName); }
            }
        }

        #endregion

        private readonly string tableName;
        private readonly List<IAssign> toSet = new List<IAssign>();
        private readonly List<IWhere> whereClauses = new List<IWhere>();
        private readonly List<SimpleWhere> ifClauses = new List<SimpleWhere>();
        private long timestamp = 0;
        private long ttl = 0;

        public CqlUpdate(string tableName)
        {
            this.tableName = tableName;
        }

        internal CqlUpdate AddAssignment(IAssign assign)
        {
            this.toSet.Add(assign);
            return this;
        }

        internal CqlUpdate AddWhere(IWhere where)
        {
            this.whereClauses.Add(where);
            return this;
        }

        internal CqlUpdate AddIf(SimpleWhere where)
        {
            this.ifClauses.Add(where);
            return this;
        }

        /// <summary>
        /// Set the timestamp. If not specified, the current time of the insertion (in microseconds) is used. This is usually a suitable default.
        /// </summary>
        /// <param name="time">The timestamp</param>
        /// <returns></returns>
        public CqlUpdate WithTimestamp(long time)
        {
            this.timestamp = time;
            return this;
        }

        /// <summary>
        /// Set the time to live. If set, the inserted values are automatically removed from the database after the specified time.
        /// Note that the TTL concerns the inserted values, not the column themselves. This means that any subsequent update of the column will also reset the TTL (to whatever TTL is specified in that update). By default, values never expire.
        /// </summary>
        /// <param name="timeToLive">The time to live in seconds</param>
        /// <returns></returns>
        public CqlUpdate WithTimeToLive(long timeToLive)
        {
            this.ttl = timeToLive;
            return this;
        }

        /// <summary>
        /// Call this when needing to add If clauses to the statement
        /// </summary>
        public CqlUpdateNamedSetAndWhere StartIf
        {
            get { return new CqlUpdateNamedSetAndWhere(this); }
        }

        public string BuildString
        {
            get
            {
                var toRet = new StringBuilder();
                toRet.AppendFormat("UPDATE {0}", this.tableName);
                if (this.ttl != 0 || this.timestamp != 0)
                {
                    toRet.Append(" USING ");
                    var and = false;
                    if (this.timestamp != 0)
                    {
                        toRet.AppendFormat("TIMESTAMP {0}", this.timestamp);
                        and = true;
                    }

                    if (and)
                    {
                        toRet.Append(" AND ");
                    }

                    if (this.ttl != 0)
                    {
                        toRet.AppendFormat("TTL {0}", this.ttl);
                    }
                }

                toRet.Append(" SET ");
                for (int i = 0; i < this.toSet.Count-1; i++)
                {
                    toRet.AppendFormat("{0}, ", this.toSet[i].AssignString);
                }
                toRet.AppendFormat("{0}", this.toSet.Last().AssignString);

                toRet.Append(" WHERE ");
                for (int i = 0; i < this.whereClauses.Count-1; i++)
                {
                    toRet.AppendFormat("{0} AND ", this.whereClauses[i].WhereString);
                }
                toRet.AppendFormat("{0}", this.whereClauses.Last().WhereString);

                if (this.ifClauses.Count > 0)
                {
                    toRet.Append(" IF ");
                    for (int i = 0; i < this.ifClauses.Count-1; i++)
                    {
                        toRet.AppendFormat("{0} AND ", this.ifClauses[i].WhereString);
                    }
                    toRet.AppendFormat("{0}", this.ifClauses.Last().WhereString);
                }

                toRet.Append(";");
                return toRet.ToString();
            }
        }
    }
}
