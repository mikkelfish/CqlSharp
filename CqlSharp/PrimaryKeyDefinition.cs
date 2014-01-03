using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp
{
    public class ColumnDefinition
    {
        public string ColumnName { get; private set; }
        public Type Type { get; private set; }
        public string IndexName { get; private set; }

        public ColumnDefinition(string keyName, Type cqlType, string indexName = null)
        {
            this.ColumnName = keyName;
            this.Type = cqlType;
            this.IndexName = indexName;
        }
    }
}
