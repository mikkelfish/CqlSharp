using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Fluent.Manipulation;
using CqlSharp.Serialization;

namespace CqlSharp.Fluent
{
    public static class Utilities
    {
        public static string Table<T>()
        {
             var accessor = ObjectAccessor<T>.Instance;
             return accessor.KeySpaceTableAndName;
        }

        public static string[] PartitionKeys<T>()
        {
            var accessor = ObjectAccessor<T>.Instance;
            return accessor.PartitionKeys.Select(c => c.ColumnName).ToArray();
        }

        public static string[] ClusterKeys<T>()
        {
            var accessor = ObjectAccessor<T>.Instance;
            return accessor.ClusteringKeys.Select(c => c.ColumnName).ToArray();
        }

        public static string[] ValueColumns<T>()
        {
            var accessor = ObjectAccessor<T>.Instance;
            return accessor.NormalColumns.Select(c => c.ColumnName).ToArray();
        }
    }
}
