using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    public class CqlIndexNamed
    {
        private readonly string tableName;

        internal CqlIndexNamed(string tableName)
        {
            this.tableName = tableName;
        }

        /// <summary>
        /// Specify the column to index
        /// </summary>
        /// <param name="columnName">The name of the column</param>
        /// <returns></returns>
        public CqlIndex OnColumn(string columnName)
        {
            return new CqlIndex(this.tableName, columnName);
        }
    }

    public class CqlIndex : IFluentCommand
    {
        private readonly string tableName;
        private readonly string columnName;
        private string indexName;
        private string indexClass;
        private bool throwOnError = true;

        internal CqlIndex(string tableName, string columnName)
        {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        public string BuildString
        {
            get
            {
                return String.Format("CREATE {0}INDEX {1}{2}ON {3} ({4}){5};",
                    this.indexClass != null ? "CUSTOM " : String.Empty,
                    !this.throwOnError ? "IF NOT EXISTS " : String.Empty,
                    this.indexName != null ?  this.indexName + " " : String.Empty,
                    this.tableName,
                    this.columnName,
                    (this.indexClass != null ? " USING '" + this.indexClass + "'" : String.Empty)
                    );
            }
        }

        /// <summary>
        /// Give the index a name
        /// </summary>
        /// <param name="name">The index name</param>
        /// <returns></returns>
        public CqlIndex WithName(string name)
        {
            this.indexName = name;
            return this;
        }

        /// <summary>
        /// Specify a custom class to be used in indexing
        /// </summary>
        /// <param name="javaQualifiedClass">The fully qualified path of the class</param>
        /// <returns></returns>
        public CqlIndex WithCustomClass(string javaQualifiedClass)
        {
            this.indexClass = javaQualifiedClass;
            return this;
        }

        /// <summary>
        /// Attempting to create an already existing index will return an error unless throwOnError is false. If it is false, the statement will be a no-op if the index already exists.
        /// </summary>
        /// <param name="throwError">Whether to throw if the index already exists</param>
        /// <returns></returns>
        public CqlIndex SetThrowOnError(bool throwError)
        {
            this.throwOnError = throwError;
            return this;
        }
    }
}
