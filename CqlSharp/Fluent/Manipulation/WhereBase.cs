using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Manipulation
{
    
     #region Where

        public interface IHasWhere<T>
        {
            T AddWhere(IWhere where);
        }

        public interface IWhere 
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

    public class WhereBase<T, U> where T:WhereBase<T,U>
           where U: IHasWhere<U>
    {
        private readonly U update;

        internal WhereBase(U update)
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
        public T AddWhereRelation(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddWhere(new SimpleWhere(colName, para));
            return this as T;
        }

        /// <summary>
        /// The IN relation is only supported for the last column of the partition key, e.g. WHERE age IN ?
        /// </summary>
        /// <param name="colName">The name of the column (must be the last column of the partition key)</param>
        /// <param name="customParameterName">The optional custom parameter name. If not set, then reference the parameter by the column name when setting command parameters</param>
        /// <returns></returns>
        public T AddInWhere(string colName, string customParameterName = null)
        {
            var para = this.getPara(customParameterName);
            this.update.AddWhere(new InWhere(colName, para));
            return this as T;
        }

        /// <summary>
        /// Use this when all where clauses have been added. NOTE: All primary keys (partition + cluster) must have a where clause associated with them for the update to execute
        /// </summary>
        public U FinishedWhere
        {
            get
            {
                return this.update;
            }
        }
    }
}
