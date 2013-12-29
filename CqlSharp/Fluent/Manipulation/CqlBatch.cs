using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Manipulation
{
    public class CqlBatchDefiner
    {
        private readonly CqlBatch batch;
        internal CqlBatchDefiner()
        {
            this.batch = new CqlBatch();
        }

        /// <summary>
        /// Add a command (insert/update/delete) to the batch
        /// </summary>
        /// <param name="toAdd"></param>
        /// <returns></returns>
        public CqlBatchDefiner AddCommand(IManipulation toAdd)
        {
            this.batch.AddCommand(toAdd);
            return this;
        }

        /// <summary>
        /// Use after all commands have been add to the batch
        /// </summary>
        public CqlBatch BatchDefined
        {
            get { return this.batch; }
        }
    }

    public class CqlBatch : IFluentCommand
    {
        private readonly List<IManipulation> manipulations = new List<IManipulation>();
        private long timestamp = 0;

        internal CqlBatch()
        {

        }

        internal CqlBatch AddCommand(IManipulation manipulation)
        {
            this.manipulations.Add(manipulation);
            return this;
        }

        /// <summary>
        /// Set the timestamp. If not specified, the current time of the insertion (in microseconds) is used. This is usually a suitable default. 
        /// The timestamp applies to all the statement inside the batch. However, if used, TIMESTAMP must not be used in the statements within the batch.
        /// </summary>
        /// <param name="time">The timestamp</param>
        /// <returns></returns>
        public CqlBatch WithTimestamp(long time)
        {
            this.timestamp = time;
            return this;
        }


        public string BuildString
        {
            get
            {
                var toRet = new StringBuilder();
                toRet.Append("BEGIN BATCH ");

                if (this.timestamp != 0)
                {
                    toRet.AppendFormat("USING TIMESTAMP {0} ", this.timestamp);
                }

                foreach (var manipulation in this.manipulations)
                {
                    toRet.AppendFormat("{0} ", manipulation.BuildString);
                }
                toRet.Append("APPLY BATCH;");
                return toRet.ToString();
            }
        }
    }
}
