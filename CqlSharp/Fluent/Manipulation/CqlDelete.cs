using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Manipulation
{
    public class CqlDeleteNamed
    {
        private readonly CqlDelete delete;
        internal CqlDeleteNamed(string tableName)
        {
            this.delete = new CqlDelete(tableName);
        }

        private string getPara(string customParameterName)
        {
            return customParameterName == null ? "?" : ":" + customParameterName;
        }

        /// <summary>
        /// Delete a column from the row
        /// </summary>
        /// <param name="colName">The name of the column</param>
        /// <returns></returns>
        public CqlDeleteNamed DeleteColumn(string colName)
        {
            this.delete.AddToDelete(colName);
            return this;
        }

        /// <summary>
        /// Delete a value from a map, e.g. map.Remove(?)
        /// </summary>
        /// <param name="mapName">The name of the map</param>
        /// <param name="keyParameter">The parameter representing the key to remove</param>
        /// <returns></returns>
        public CqlDeleteNamed DeleteMapEntry(string mapName, string keyParameter = null)
        {
            keyParameter = this.getPara(keyParameter);
            this.delete.AddToDelete(String.Format("{0}[{1}]", mapName, keyParameter));
            return this;
        }

        /// <summary>
        /// Delete an item from a list by index, e.g. list.RemoveAt(?)
        /// </summary>
        /// <param name="listName">The name of the list</param>
        /// <param name="indexParameter">The parameter representing the index</param>
        /// <returns></returns>
        public CqlDeleteNamed DeleteListEntry(string listName, string indexParameter = null)
        {
            indexParameter = this.getPara(indexParameter);
            this.delete.AddToDelete(String.Format("{0}[{1}]", listName, indexParameter));
            return this;
        }

        /// <summary>
        /// Use after all delete statements have been added. If no delete statements have been added, then the entire row will be deleted
        /// </summary>
        public CqlDeleteNamedAndDefined DeletesDefined
        {
            get
            {
                return new CqlDeleteNamedAndDefined(this.delete);
            }
        }
    }

    public class CqlDeleteNamedAndDefined : WhereBase<CqlDeleteNamedAndDefined, CqlDelete>
    {
        internal CqlDeleteNamedAndDefined(CqlDelete delete) : base(delete)
        {
        }        
    }

    public class CqlDelete : IManipulation, IHasWhere<CqlDelete>
    {
        private readonly string tableName;
        private readonly List<string> toDelete = new List<string>();
        private readonly List<IWhere> whereStatements = new List<IWhere>();
        private string timestampParameter = null;

        private string getPara(string customParameterName)
        {
            return customParameterName == null ? "?" : ":" + customParameterName;
        }

        internal CqlDelete(string tableName)
        {
            this.tableName = tableName;
        }

        internal CqlDelete AddToDelete(string del)
        {
            this.toDelete.Add(del);
            return this;
        }

        CqlDelete IHasWhere<CqlDelete>.AddWhere(IWhere where)
        {
            this.whereStatements.Add(where);
            return this;
        }

        /// <summary>
        /// Set the timestamp. If not specified, the current time of the insertion (in microseconds) is used. This is usually a suitable default.
        /// </summary>
        /// <param name="parameter">The timestampParameter</param>
        /// <returns></returns>
        public CqlDelete WithTimestamp(string parameter = null)
        {
            this.timestampParameter = this.getPara(parameter);
            return this;
        }

        public string BuildString
        {
            get
            {
                var toRet = new StringBuilder();
                toRet.Append("DELETE ");
                if (this.toDelete.Count > 0)
                {
                    for (int i = 0; i < toDelete.Count - 1; i++)
                    {
                        toRet.AppendFormat("{0}, ", toDelete[0]);
                    }
                    toRet.AppendFormat("{0} ", toDelete.Last());
                }

                toRet.AppendFormat("FROM {0} ", this.tableName);

                if (this.timestampParameter != null)
                {
                    toRet.AppendFormat("USING TIMESTAMP {0} ", this.timestampParameter);
                }

                toRet.Append("WHERE ");
                for (int i = 0; i < this.whereStatements.Count - 1; i++)
                {
                    toRet.AppendFormat("{0} AND ", this.whereStatements[i].WhereString);
                }
                toRet.AppendFormat("{0}", this.whereStatements.Last().WhereString);

                toRet.Append(";");
                return toRet.ToString();
            }
        }
    }
}
