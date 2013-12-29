using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Manipulation
{

    public class CqlInsert : IManipulation
    {
        private readonly string tableName;
        private readonly Dictionary<string, string> toInsert = new Dictionary<string, string>();
        private bool isNotExists = false;
        private long timestamp = 0;
        private long ttl = 0;

        internal CqlInsert(string tableName)
        {
            this.tableName = tableName;
        }

        /// <summary>
        /// Specify a column that will be inserted
        /// </summary>
        /// <param name="columnName">The name of the column</param>
        /// <returns></returns>
        public CqlInsert With(string columnName)
        {
            if (!this.toInsert.ContainsKey(columnName))
                this.toInsert.Add(columnName, "?");
            else this.toInsert[columnName] = "?";
            return this;
        }

        /// <summary>
        /// Specify a column that will be inserted with a named parameter. E.g. insert into Test.BasicFlow (id,value) values(:id1,:value1); cmd.Parameters["id1"].Value = 0;
        /// </summary>
        /// <param name="columnName">The name of the column</param>
        /// /// <param name="parameterName">The name of the parameter</param>
        /// <returns></returns>
        public CqlInsert WithNamedParameter(string columnName, string parameterName)
        {
            if (!this.toInsert.ContainsKey(columnName))
                this.toInsert.Add(columnName, ":" + parameterName);
            else this.toInsert[columnName] = ":" + parameterName;
            return this;
        }

        /// <summary>
        /// Note that unlike in SQL, INSERT does not check the prior existence of the row by default: the row is created if none existed before, and updated otherwise. 
        /// Furthermore, there is no mean to know which of creation or update happened. It is however possible to use the IF NOT EXISTS condition to only insert if 
        /// the row does not exist prior to the insertion. But please note that using IF NOT EXISTS will incur a non negligible performance cost (internally, Paxos will be used)
        /// so this should be used sparingly.
        /// </summary>
        /// <param name="notExists">Set whether to insert only if it does not exist (no updates)</param>
        /// <returns></returns>
        public CqlInsert SetWhenNotExists(bool notExists)
        {
            this.isNotExists = notExists;
            return this;
        }

        /// <summary>
        /// If not specified, the current time of the insertion (in microseconds) is used. This is usually a suitable default.
        /// </summary>
        /// <param name="timestamp">The timestamp</param>
        /// <returns></returns>
        public CqlInsert SetTimestamp(long timestamp)
        {
            this.timestamp = timestamp;
            return this;
        }

        /// <summary>
        /// If set, the inserted values are automatically removed from the database after the specified time. Note that the TTL concerns the inserted values, not the column themselves.
        /// This means that any subsequent update of the column will also reset the TTL (to whatever TTL is specified in that update). By default, values never expire.
        /// </summary>
        /// <param name="timeToLive">The time to live</param>
        /// <returns></returns>
        public CqlInsert SetTimeToLive(long timeToLive)
        {
            this.ttl = timeToLive;
            return this;
        }

        public string BuildString
        {
            get
            {
                if (this.toInsert.Count < 2)
                    throw new InvalidOperationException("There must be at least one primary key column and one value column in order to create an insert.");

                var toRet = new StringBuilder();
                toRet.AppendFormat("INSERT INTO {0} (", this.tableName);

                var insertArray = toInsert.ToArray();
                for (int i = 0; i < insertArray.Length - 1; i++)
                {
                    toRet.AppendFormat("{0}, ", insertArray[i].Key);
                }
                toRet.AppendFormat("{0} )", insertArray.Last().Key);
                toRet.Append("VALUES (");
                for (int i = 0; i < insertArray.Length - 1; i++)
                {
                    toRet.Append(insertArray[i].Value + ", ");
                }
                toRet.Append(insertArray.Last().Value);
                toRet.Append(")"); //Close values

                if (this.isNotExists)
                    toRet.Append(" IF NOT EXISTS");

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

                toRet.Append(";");
                return toRet.ToString();
            }
        }
    }
}
