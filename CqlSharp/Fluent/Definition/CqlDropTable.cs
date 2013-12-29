using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    /// <summary>
    /// The DROP TABLE statement results in the immediate, irreversible removal of a table, including all data contained in it. 
    /// </summary>
    public class CqlDropTable : IFluentCommand
    {
        private readonly string tableName;
        private bool throwError = true;

        internal CqlDropTable(string name)
        {
            this.tableName = name;
        }

        /// <summary>
        /// If the table does not exists, the statement will return an error, unless throwOnError equals false, in which case the operation is a no-op.
        /// </summary>
        /// <param name="throwOnError"></param>
        /// <returns></returns>
        public CqlDropTable SetThrowOnError(bool throwOnError)
        {
            this.throwError = throwOnError;
            return this;
        }

        public string BuildString
        {
            get { return String.Format("DROP TABLE {0} {1};",
                !this.throwError ? "IF EXISTS" : "",
                this.tableName); }
        }
    }
}
