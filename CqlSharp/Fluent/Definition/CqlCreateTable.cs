using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace CqlSharp.Fluent.Definition
{
    public class CqlTableNamed
    {
        private readonly CqlCreateTable table;
        internal CqlTableNamed(string name)
        {
            this.table = new CqlCreateTable(name);
        }

        public CqlCreateTable HasPartitionKey(string name, Type type)
        {
            this.table.HasPartitionKey(name, type);
            return this.table;
        }

        public CqlCreateTable HasPartitionKeys(Dictionary<string, Type> keys)
        {
            foreach (var kvp in keys)
            {
                this.table.HasPartitionKey(kvp.Key, kvp.Value);
            }
            return this.table;
        }
    }


    public class CqlCreateTable : TableBase<CqlCreateTable>, IBuiltCommand
    {
        private string TableName { get; set; }
        private bool ThrowError { get; set; }
      
        private readonly Dictionary<string, string> columns = new Dictionary<string, string>();
        private readonly List<string> clusteringKeys = new List<string>();
        private readonly List<string> partitionKeys = new List<string>();
        

        internal CqlCreateTable(string tableName)
        {
            this.TableName = tableName;
        }

        private bool implementsInterface(Type genericType, Type genericInterface)
        {
            if (genericType.IsInterface && genericType.IsGenericType &&
                genericType.GetGenericTypeDefinition() == genericInterface)
                return true;

            foreach (var i in genericType.GetInterfaces())
                if (i.IsGenericType && i.GetGenericTypeDefinition() == genericInterface)
                    return true;

            return false;
        }

        internal CqlCreateTable HasPartitionKey(string name, Type type)
        {
            this.addColumn(name, type);
            if (!this.partitionKeys.Contains(name))
                this.partitionKeys.Add(name);
            return this;
        }
        private CqlCreateTable addColumn(string name, Type type)
        {
            if (!this.columns.ContainsKey(name))
            {
                string cqlType;
                if (!type.IsGenericType)
                    cqlType = type.ToCqlType().ToString().ToLower();
                else if (this.implementsInterface(type, typeof(IList<>)))
                {
                    cqlType = "list<" + type.GetGenericArguments()[0] + ">";
                }
                else if (this.implementsInterface(type, typeof(ISet<>)))
                {
                    cqlType = "set<" + type.GetGenericArguments()[0] + ">";
                }
                else if (this.implementsInterface(type, typeof(IDictionary<,>)))
                {
                    cqlType = "map<" + type.GetGenericArguments()[0] + "," + type.GetGenericArguments()[1] + ">";
                }
                else throw new InvalidOperationException("Do not know how to convert " + type.FullName + " to column type");

                this.columns.Add(name, cqlType);
            }
            else throw new InvalidOperationException("The table already has a definition for column " + name);
            return this;
        }

        public CqlCreateTable AddColumn(string name, Type type)
        {
            return this.addColumn(name, type);
        }

        public CqlCreateTable AddColumnCustomJavaType(string name, string qualifiedName)
        {
            if (!this.columns.ContainsKey(name))
            {
                this.columns.Add(name, qualifiedName);
            }
            else throw new InvalidOperationException("The table already has a definition for column " + name);
            return this;
        }


        public CqlCreateTable AddClusteringKey(string name, Type type)
        {
            this.addColumn(name, type);
            if (!this.clusteringKeys.Contains(name))
                this.clusteringKeys.Add(name);
            return this;
        }


        public string BuildString
        {
            get 
            {
                var toRet = new StringBuilder();
                toRet.AppendFormat("CREATE TABLE {0} {1} (", this.TableName, this.ThrowError ? "" : "IF NOT EXISTS");
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

        public CqlCreateTable SetThrowOnError(bool throwOnError)
        {
            this.ThrowError = throwOnError;
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
