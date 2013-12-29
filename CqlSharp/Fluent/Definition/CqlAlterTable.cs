using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Fluent.Definition
{
    public class CqlAlterTableNamed
    {
        private readonly string tableName;

        internal CqlAlterTableNamed(string name)
        {
            this.tableName = name;
        }

        /// <summary>
        /// Update the type of a given defined column. Note that the type of the clustering columns cannot be modified as it induces the on-disk ordering of rows.
        /// Columns on which a secondary index is defined have the same restriction. Other columns are free from those restrictions (no validation of existing data is performed), 
        /// but it is usually a bad idea to change the type to a non-compatible one, unless no data have been inserted for that column yet, as this could confuse CQL drivers/tools.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">New column type</param>
        /// <returns></returns>
        public CqlAlterTableModifyColumn AlterColumn(string name, Type type)
        {
            return new CqlAlterTableModifyColumn(this.tableName, name, type.ToCqlColumnType(false));
        }

        /// <summary>
        /// Update the type of a given defined column. Note that the type of the clustering columns cannot be modified as it induces the on-disk ordering of rows.
        /// Columns on which a secondary index is defined have the same restriction. Other columns are free from those restrictions (no validation of existing data is performed), 
        /// but it is usually a bad idea to change the type to a non-compatible one, unless no data have been inserted for that column yet, as this could confuse CQL drivers/tools.
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="customJavaQualifiedType">New column type</param>
        /// <returns></returns>
        public CqlAlterTableModifyColumn AlterColumn(string name, string customJavaQualifiedType)
        {
            return new CqlAlterTableModifyColumn(this.tableName, name, customJavaQualifiedType);
        }

        /// <summary>
        /// Adds a new column to the table. The name for the new column must not conflict with an existing column. 
        /// Moreover, columns cannot be added to tables defined with the COMPACT STORAGE option
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="type">Type of the column</param>
        /// <returns></returns>
        public CqlAlterTableAddColumn AddColumn(string name, Type type)
        {
            return new CqlAlterTableAddColumn(this.tableName, name, type.ToCqlColumnType(false));
        }

        /// <summary>
        /// Adds a new column to the table. The name for the new column must not conflict with an existing column. 
        /// Moreover, columns cannot be added to tables defined with the COMPACT STORAGE option
        /// </summary>
        /// <param name="name">Name of the column</param>
        /// <param name="customJavaQualifiedType">Type of the column</param>
        /// <returns></returns>
        public CqlAlterTableAddColumn AddColumn(string name, string customJavaQualifiedType)
        {
            return new CqlAlterTableAddColumn(this.tableName, name, customJavaQualifiedType);
        }

        /// <summary>
        /// Change options on the table. Note that setting any compaction sub-options has the effect of erasing all 
        /// previous compaction options, so you need to re-specify all the sub-options if you want to keep them. The same note applies to the set of compression sub-options.
        /// </summary>
        /// <returns></returns>
        public CqlAlterTableOptions ChangeOptions()
        {
            return new CqlAlterTableOptions(this.tableName);
        }

        /// <summary>
        /// Removes a column from the table. Dropped columns will immediately become unavailable in the queries and will not be included in compacted 
        /// sstables in the future. If a column is readded, queries won’t return values written before the column was last dropped. 
        /// It is assumed that timestamps represent actual time, so if this is not your case, you should NOT readd previously dropped columns. 
        /// Columns can’t be dropped from tables defined with the COMPACT STORAGE option.
        /// </summary>
        /// <param name="name">Name of the column to drop</param>
        /// <returns></returns>
        public CqlAlterTableDropColumn DropColumn(string name)
        {
            return new CqlAlterTableDropColumn(this.tableName, name);
        }
    }

    public class CqlAlterTableModifyColumn : IFluentCommand
    {
        private readonly string tableName;
        private readonly string columnName;
        private readonly string coltype;

        internal CqlAlterTableModifyColumn(string tableName, string columnName, string coltype)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.coltype = coltype;
            
        }

        public string BuildString
        {
            get
            {
                return String.Format("ALTER TABLE {0} ALTER {1} TYPE {2};", this.tableName, this.columnName, this.coltype);
            }
        }
    }

    public class CqlAlterTableAddColumn : IFluentCommand
    {
        private readonly string tableName;
        private readonly string columnName;
        private readonly string coltype;

        internal CqlAlterTableAddColumn(string tableName, string columnName, string coltype)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.coltype = coltype;
            
        }

        public string BuildString
        {
            get
            {
                return String.Format("ALTER TABLE {0} ADD {1} {2};", this.tableName, this.columnName, this.coltype);
            }
        }
    }

    public class CqlAlterTableDropColumn : IFluentCommand
    {
        private readonly string tableName;
        private readonly string columnName;

        internal CqlAlterTableDropColumn(string tableName, string columnName)
        {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        public string BuildString
        {
            get
            {
                return String.Format("ALTER TABLE {0} DROP {1};", this.tableName, this.columnName);
            }
        }
    }

    public class CqlAlterTableOptions : TableBase<CqlAlterTableOptions>, IFluentCommand
    {
        private readonly string tableName;
        internal CqlAlterTableOptions(string tableName)
        {
            this.tableName = tableName;
        }

        public string BuildString
        {
            get { return String.Format("ALTER TABLE {0} {1};", this.tableName, this.getOptionString()); }
        }
    }
}
