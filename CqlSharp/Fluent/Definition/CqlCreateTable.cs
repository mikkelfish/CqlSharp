using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace CqlSharp.Fluent.Definition
{
    public class CqlCreateTableNamed
    {
        private readonly CqlCreateTable table;
        internal CqlCreateTableNamed(string name)
        {
            this.table = new CqlCreateTable(name);
        }

        /// <summary>
        /// The first column of the key is called the partition key. It has the property that all the rows sharing the same partition key (even across table in fact) are stored on the same physical node. 
        /// Also, insertion/update/deletion on rows sharing the same partition key for a given table are performed atomically and in isolation. 
        /// Note that it is possible to have a composite partition key, i.e. a partition key formed of multiple columns.
        /// </summary>
        /// <param name="name">The name of the column used for the partition key</param>
        /// <param name="type">The type of the column</param>
        /// <returns></returns>
        public CqlCreateTableNamedAndPartitioned HasPartitionKey(string name, Type type)
        {
            this.table.HasPartitionKey(name, type);
            return new CqlCreateTableNamedAndPartitioned(this.table);
        }


        /// <summary>
        /// The first column of the key is called the partition key. It has the property that all the rows sharing the same partition key (even across table in fact) are stored on the same physical node. 
        /// Also, insertion/update/deletion on rows sharing the same partition key for a given table are performed atomically and in isolation. 
        /// Note that it is possible to have a composite partition key, i.e. a partition key formed of multiple columns.
        /// </summary>
        /// <param name="keys">The columns used for the composite partition key</param>
        /// <returns></returns>
        public CqlCreateTableNamedAndPartitioned HasPartitionKeys(IEnumerable<ColumnDefinition> keys)
        {
            foreach (var key in keys)
            {
                this.table.HasPartitionKey(key.ColumnName, key.Type);
            }
            return new CqlCreateTableNamedAndPartitioned(this.table);
        }
    }

    public class CqlCreateTableNamedAndPartitioned
    {
        private readonly CqlCreateTable table;
        internal CqlCreateTableNamedAndPartitioned(CqlCreateTable table)
        {
            this.table = table;
        }

        /// <summary>
        /// Adds a column used within the primary key (along with the partition key(s) previously defined.  On a given physical node, rows for a given partition key are stored in the order induced by
        /// the clustering columns, making the retrieval of rows in that clustering order particularly efficient.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        public CqlCreateTableNamedAndPartitioned AddClusteringKey(string name, Type type)
        {
            this.table.AddClusteringKey(name, type);
            return this;
        }

        /// <summary>
        /// Adds columns used within the primary key (along with the partition key(s) previously defined.  On a given physical node, rows for a given partition key are stored in the order induced by
        /// the clustering columns, making the retrieval of rows in that clustering order particularly efficient.
        /// </summary>
        /// <param name="keys">The keys to add</param>
        /// <returns></returns>
        public CqlCreateTableNamedAndPartitioned AddClusteringKeys(IEnumerable<ColumnDefinition> keys)
        {
            foreach (var key in keys)
            {
                this.table.AddClusteringKey(key.ColumnName, key.Type);
            }
            return this;
        }

        /// <summary>
        /// Call this after all clustering keys have been added
        /// </summary>
        public CqlCreateTableColumnDefiner FinishedClusteringKeys
        {
            get { return new CqlCreateTableColumnDefiner(this.table); }
        }
    }

    public class CqlCreateTableColumnDefiner
    {
        private readonly CqlCreateTable table;
        internal CqlCreateTableColumnDefiner(CqlCreateTable table)
        {
            this.table = table;
        }


        /// <summary>
        /// Adds a (non-primary key) column. When inserting a given row, not all columns needs to be defined (except for those part of the key), 
        /// and missing columns occupy no space on disk. Furthermore, adding new columns is a constant time operation. 
        /// There is thus no need to try to anticipate future usage (or to cry when you haven’t) when creating a table.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        public CqlCreateTableColumnDefiner AddColumn(string name, Type type)
        {
            this.table.AddColumn(name, type);
            return this;
        }

        /// <summary>
        /// Adds (non-primary key) columns. When inserting a given row, not all columns needs to be defined (except for those part of the key), 
        /// and missing columns occupy no space on disk. Furthermore, adding new columns is a constant time operation. 
        /// There is thus no need to try to anticipate future usage (or to cry when you haven’t) when creating a table.
        /// </summary>
        /// <param name="definitions">The columns to add</param>
        /// <returns></returns>
        public CqlCreateTableColumnDefiner AddColumns(IEnumerable<ColumnDefinition> definitions)
        {
            foreach (var definition in definitions)
            {
                this.table.AddColumn(definition.ColumnName, definition.Type);
            }
            return this;
        }

        /// <summary>
        /// Adds a (non-primary key) column with Java qualifed type
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="qualifiedName">Type of the column</param>
        /// <returns></returns>
        public CqlCreateTableColumnDefiner AddColumnCustomJavaType(string name, string qualifiedName)
        {
            this.table.AddColumnCustomJavaType(name, qualifiedName);
            return this;
        }

        /// <summary>
        /// Call this after all columns have been defined
        /// </summary>
        public CqlCreateTable FinishedDefiningColumns
        {
            get { return this.table; }
        }
    }


    public class CqlCreateTable : TableBase<CqlCreateTable>, IFluentCommand
    {
        private readonly string tableName;
        private bool throwError = true;

        private readonly Dictionary<string, string> columns = new Dictionary<string, string>();
        private readonly List<string> clusteringKeys = new List<string>();
        private readonly List<string> partitionKeys = new List<string>();
        

        internal CqlCreateTable(string tableName)
        {
            this.tableName = tableName;
        }

        internal CqlCreateTable HasPartitionKey(string name, Type type)
        {
            this.addColumn(name, type, true);
            if (!this.partitionKeys.Contains(name))
                this.partitionKeys.Add(name);
            return this;
        }
        
        private CqlCreateTable addColumn(string name, Type type, bool mustBeSimple)
        {
            if (!this.columns.ContainsKey(name))
            {
                this.columns.Add(name, type.ToCqlColumnType(mustBeSimple));
            }
            else throw new InvalidOperationException("The table already has a definition for column " + name);
            return this;
        }

        /// <summary>
        /// Adds a (non-primary key) column. When inserting a given row, not all columns needs to be defined (except for those part of the key), 
        /// and missing columns occupy no space on disk. Furthermore, adding new columns is a constant time operation. 
        /// There is thus no need to try to anticipate future usage (or to cry when you haven’t) when creating a table.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        internal CqlCreateTable AddColumn(string name, Type type)
        {
            return this.addColumn(name, type, false);
        }

        /// <summary>
        /// Adds a (non-primary key) column with Java qualifed type
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="qualifiedName">Type of the column</param>
        /// <returns></returns>
        internal CqlCreateTable AddColumnCustomJavaType(string name, string qualifiedName)
        {
            if (!this.columns.ContainsKey(name))
            {
                this.columns.Add(name, qualifiedName);
            }
            else throw new InvalidOperationException("The table already has a definition for column " + name);
            return this;
        }

        /// <summary>
        /// Adds a column used within the primary key (along with the partition key(s) previously defined.  On a given physical node, rows for a given partition key are stored in the order induced by
        /// the clustering columns, making the retrieval of rows in that clustering order particularly efficient.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        internal CqlCreateTable AddClusteringKey(string name, Type type)
        {
            this.addColumn(name, type, true);
            if (!this.clusteringKeys.Contains(name))
                this.clusteringKeys.Add(name);
            return this;
        }


        public string BuildString
        {
            get 
            {
                var toRet = new StringBuilder();
                toRet.AppendFormat("CREATE TABLE {0} {1} (",  !this.throwError ? "IF NOT EXISTS" : "", this.tableName);
                foreach (var col in this.columns)
                {
                    toRet.AppendFormat("{0} {1},", col.Key, col.Value);
                }

                //In CQL, the order in which columns are defined for the PRIMARY KEY matters. The first column of the key is called the partition key. 
                //It has the property that all the rows sharing the same partition key (even across table in fact) are stored on the same physical node. 
                //Also, insertion/update/deletion on rows sharing the same partition key for a given table are performed atomically and in isolation. 
                //Note that it is possible to have a composite partition key, i.e. a partition key formed of multiple columns, using an extra set of 
                //parentheses to define which columns forms the partition key.
                toRet.Append("PRIMARY KEY ( ");
                if (this.partitionKeys.Count == 1)
                    toRet.Append(this.partitionKeys.Single());
                else
                {
                    toRet.Append(" ( ");
                    for (int i = 0; i < this.partitionKeys.Count - 1; i++)
                    {
                        toRet.AppendFormat("{0}, ", this.partitionKeys[i]);
                    }

                    toRet.AppendFormat("{0} )", this.partitionKeys.Last());
                }

                foreach (var clustering in clusteringKeys)
                {
                    toRet.AppendFormat(", {0}", clustering);
                }
                toRet.Append(" )"); //close PRIMARY KEY

                toRet.Append(" )"); //close CREATE TABLE

                if (!this.clusteringOrder && !this.compactStorage && this.options.Count == 0)
                {
                    toRet.Append(";");
                    return toRet.ToString(); //We have no options so we're done
                }

                toRet.Append(this.getOptionString());
                return toRet.ToString();
            }
        }

        /// <summary>
        /// Attempting to create an already existing table will return an error unless the throwOnError is false. If it false, the statement will be a no-op if the table already exists.
        /// </summary>
        /// <param name="throwOnError">Whether to throw when the table already exists</param>
        /// <returns></returns>
        public CqlCreateTable SetThrowOnError(bool throwOnError)
        {
            this.throwError = throwOnError;
            return this;
        }

        /// <summary>
        /// This option is mainly targeted towards backward compatibility with some table definition created before CQL3. But it also provides a slightly more compact layout of data on disk, though at the price of flexibility and extensibility, and for that reason is not recommended unless for the backward compatibility reason. The restriction for table with COMPACT STORAGE is that they support one and only one column outside of the ones part of the PRIMARY KEY. It also follows that columns cannot be added nor removed after creation. A table with COMPACT STORAGE must also define at least one clustering key.
        /// </summary>
        /// <param name="useCompactStorage">Default: false</param>
        /// <returns></returns>
        public CqlCreateTable WithCompactStorage(bool useCompactStorage)
        {
            this.compactStorage = useCompactStorage;
            return this;
        }
    }
}
