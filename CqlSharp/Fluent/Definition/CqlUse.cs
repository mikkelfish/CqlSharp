namespace CqlSharp.Fluent.Definition
{
    public class CqlUse : IFluentCommand
    {
        private readonly string keyspace;
        internal CqlUse(string keyspace)
        {
            this.keyspace = keyspace;
        }

        public string BuildString
        {
            get { return "USE " + this.keyspace + ";"; }
        }
    }
}
