namespace CqlSharp.Fluent.Definition
{
    public class CqlDropKeyspace: IBuiltCommand
    {
        private string Keyspace { get; set; }
        private bool ThrowError { get; set; }

        internal CqlDropKeyspace(string keyspace)
        {
            this.Keyspace = keyspace;
        }

        public CqlDropKeyspace SetThrowOnError(bool throwOnError)
        {
            this.ThrowError = throwOnError;
            return this;
        }

        public string BuildString
        {
            get { return "DROP KEYSPACE " + (this.ThrowError ? "IF EXISTS " : "") + this.Keyspace + ";"; }
        }
    }
}
