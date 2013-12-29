using CqlSharp.Fluent.Definition;

namespace CqlSharp.Fluent
{
    public static class Define
    {
        /// <summary>
        /// The CREATE KEYSPACE statement creates a new top-level keyspace. A keyspace is a namespace that defines a replication strategy and some options for a set of tables. 
        /// Valid keyspaces names are identifiers composed exclusively of alphanumerical characters and whose length is lesser or equal to 32. 
        /// Note that as identifiers, keyspace names are case insensitive: use a quoted identifier for case sensitive keyspace names.
        /// </summary>
        /// <param name="name">Name for the keyspace</param>
        /// <returns></returns>
        public static CqlKeyspaceNamed CreateKeyspace(string name)
        {
            return new CqlKeyspaceNamed(new CqlKeyspaceDefinition.CreateKeyspace(name));
        }

        /// <summary>
        /// The ALTER KEYSPACE statement alter the properties of an existing keyspace. The supported properties are the same that for the CREATE TABLE statement.
        /// </summary>
        /// <param name="name">Name of the keyspace to alter</param>
        /// <returns></returns>
        public static CqlKeyspaceNamed AlterKeyspace(string name)
        {
            return new CqlKeyspaceNamed(new CqlKeyspaceDefinition.AlterKeyspace(name));
        }

        /// <summary>
        /// A DROP KEYSPACE statement results in the immediate, irreversible removal of an existing keyspace, including all column families in it, and all data contained in those column families.
        /// </summary>
        /// <param name="name">Name of the keyspace to drop</param>
        /// <returns></returns>
        public static CqlDropKeyspace DropKeyspace(string name)
        {
            return new CqlDropKeyspace(name);
        }

        /// <summary>
        /// The CREATE TABLE statement creates a new table. Each such table is a set of rows (usually representing related entities) for which it defines a number of properties. 
        /// A table is defined by a name, it defines the columns composing rows of the table and have a number of options. Valid table names are the same than valid keyspace names 
        /// (up to 32 characters long alphanumerical identifiers). If the table name is provided alone, the table is created within the current keyspace, 
        /// but if it is prefixed by an existing keyspace name, it is created in the specified keyspace (but does not change the current keyspace).
        /// </summary>
        /// <param name="name">Name of the table to create</param>
        /// <returns></returns>
        public static CqlTableNamed CreateTable(string name)
        {
            return new CqlTableNamed(name);
        }

        /// <summary>
        /// The ALTER statement is used to manipulate table definitions. It allows to add new columns, drop existing ones, change the type of existing columns, or update the table options.
        /// </summary>
        /// <param name="name">Name of the table to alter</param>
        /// <returns></returns>
        public static CqlAlterTableNamed AlterTable(string name)
        {
            return new CqlAlterTableNamed(name);
        }

        /// <summary>
        /// The DROP TABLE statement results in the immediate, irreversible removal of a table, including all data contained in it. 
        /// </summary>
        /// <param name="name">The name of the table to drop</param>
        /// <returns></returns>
        public static CqlDropTable DropTable(string name)
        {
            return new CqlDropTable(name);
        }

        /// <summary>
        /// The TRUNCATE statement permanently removes all data from a table.
        /// </summary>
        /// <param name="tableName">The name of the table to truncate</param>
        /// <returns></returns>
        public static CqlTruncate TruncateTable(string tableName)
        {
            return new CqlTruncate(tableName);
        }

        /// <summary>
        /// The CREATE INDEX statement is used to create a new (automatic) secondary index for a given (existing) column in a given table. 
        /// A name for the index itself can be specified before the ON keyword, if desired. If data already exists for the column, it will be indexed asynchronously. 
        /// After the index is created, new data for the column is indexed automatically at insertion time.
        /// </summary>
        /// <param name="tableName">The name of the table to index</param>
        /// <returns></returns>
        public static CqlIndexNamed CreateIndex(string tableName)
        {
            return new CqlIndexNamed(tableName);
        }

        /// <summary>
        /// The DROP INDEX statement is used to drop an existing secondary index.
        /// </summary>
        /// <param name="indexName">The name of the index to drop</param>
        /// <returns></returns>
        public static CqlDropIndex DropIndex(string indexName)
        {
            return new CqlDropIndex(indexName);
        }
    }
}
