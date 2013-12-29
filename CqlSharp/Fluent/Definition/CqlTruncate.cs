using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    public class CqlTruncate : IFluentCommand
    {
        private readonly string tableName;

        internal CqlTruncate(string tableName)
        {
            this.tableName = tableName;
        }

        public string BuildString
        {
            get { return String.Format("TRUNCATE {0};", this.tableName); }
        }
    }
}
