using CqlSharp.Fluent.Definition;

namespace CqlSharp.Fluent
{
    public static class Build
    {
        public static CqlKeyspaceNamed CreateKeyspace(string name)
        {
            return new CqlKeyspaceNamed(new KeyspaceDefinition.CqlCreateKeyspace(name));
        }

        public static CqlKeyspaceNamed AlterKeyspace(string name)
        {
            return new CqlKeyspaceNamed(new KeyspaceDefinition.CqlAlterKeyspace(name));
        }

        public static CqlDropKeyspace DropKeyspace(string name)
        {
            return new CqlDropKeyspace(name);
        }

        public static CqlTableNamed CreateTable(string name)
        {
            return new CqlTableNamed(name);
        }
    }
}
