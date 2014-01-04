using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    public class CqlDropIndex : IFluentCommand
    {
        private readonly string indexName;
        private bool throwOnError = true;

        internal CqlDropIndex(string indexName)
        {
            this.indexName = indexName;
        }

        /// <summary>
        /// If the index does not exists, the statement will return an error, unless throwOnError is false, in which case the operation is a no-op.
        /// </summary>
        /// <param name="throwError">Whether to throw if the index does not exist</param>
        /// <returns></returns>
        public CqlDropIndex SetThrowOnError(bool throwError)
        {
            this.throwOnError = throwError;
            return this;
        }

        public string BuildString
        {
            get
            {
                return String.Format("DROP INDEX {0}{1};",
                    !this.throwOnError ? "IF EXISTS " : "",
                    this.indexName);
            }
        }
    }
}
