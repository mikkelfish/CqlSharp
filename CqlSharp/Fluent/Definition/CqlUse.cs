namespace CqlSharp.Fluent.Definition
{
    public class CqlUse : IBuiltCommand
    {
        public string Keyspace { get; private set; }
        internal CqlUse(string keyspace)
        {
            this.Keyspace = keyspace;
        }

        public string BuildString
        {
            get { return "USE " + this.Keyspace + ";"; }
        }
    }
}
