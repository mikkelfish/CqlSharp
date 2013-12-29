namespace CqlSharp.Fluent.Definition
{
    public class CqlDropKeyspace: IFluentCommand
    {
        private readonly string keyspace;
        private bool throwError;

        internal CqlDropKeyspace(string keyspace)
        {
            this.keyspace = keyspace;
        }

        /// <summary>
        /// If the keyspace does not exists, the statement will return an error, unless throwOnError is set to false, in which case the operation is a no-op.
        /// </summary>
        /// <param name="throwOnError">Whether to throw when the keyspace does not exist</param>
        /// <returns></returns>
        public CqlDropKeyspace SetThrowOnError(bool throwOnError)
        {
            this.throwError = throwOnError;
            return this;
        }

        public string BuildString
        {
            get { return "DROP KEYSPACE " + (!this.throwError ? "IF EXISTS " : "") + this.keyspace + ";"; }
        }
    }
}
