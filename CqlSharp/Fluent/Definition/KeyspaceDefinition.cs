using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Fluent.Definition
{
    public abstract class KeyspaceDefinition : IBuiltCommand
    {
        internal class CqlCreateKeyspace : KeyspaceDefinition
        {
            internal CqlCreateKeyspace(string keyspace)
                : base(keyspace)
            {

            }

            protected override string Command
            {
                get { return "CREATE"; }
            }

            protected override string ErrorOverwrite
            {
                get { return "IF NOT EXISTS "; }
            }
        }
        internal class CqlAlterKeyspace : KeyspaceDefinition
        {
            internal CqlAlterKeyspace(string keyspace)
                : base(keyspace)
            {

            }

            protected override string Command
            {
                get { return "ALTER"; }
            }

            protected override string ErrorOverwrite
            {
                get { return ""; }
            }
        }

        private string Keyspace { get; set; }
        private SimpleOption<bool> Durable { get; set; }
        private MapOption Replication { get; set; }
        private bool ThrowError { get;  set; }

        internal KeyspaceDefinition(string keyspace)
        {
            this.Keyspace = keyspace;
            this.Durable = new SimpleOption<bool>("durable_writes", true);
        }

        protected abstract string Command { get; }
        protected abstract string ErrorOverwrite { get; }

        public KeyspaceDefinition SetDurability(bool durability)
        {
            this.Durable = new SimpleOption<bool>("durable_writes", durability);
            return this;
        }

        public KeyspaceDefinition SetThrowOnError(bool throwOnError)
        {
            this.ThrowError = throwOnError;
            return this;
        }

        public string BuildString
        {
            get 
            {
                return String.Format("{0} KEYSPACE {1} {2} WITH {3} AND {4};", 
                    this.Command,
                    this.ThrowError ? ErrorOverwrite : "",
                    this.Keyspace, 
                    this.Replication.BuildString, 
                    this.Durable.BuildString);
            }
        }
    }

    public class CqlKeyspaceNamed
    {
        private KeyspaceDefinition definition { get; private set; }
        internal CqlKeyspaceNamed(KeyspaceDefinition definition)
        {
            this.definition = definition;
        }


        public KeyspaceDefinition WithSimpleStrategy(int replicationFactor)
        {
            this.definition.Replication = new MapOption("replication", 
                new SimpleOption<string>("class", "SimpleStrategy"),
                new SimpleOption<int>("replication_factor", replicationFactor, rf => rf > 0));
            return this.definition;
        }

        public KeyspaceDefinition WithNetworkTopologyStrategy(IDictionary<string, int> topology)
        {
            var transformed = topology
                .Select(kvp => new SimpleOption<int>(kvp.Key, kvp.Value, rf => rf > 0))
                .Cast<IOption>();

            this.definition.Replication = new MapOption("replication",
                transformed.Union(new[] { new SimpleOption<string>("class", "SimpleStrategy") }));
            return this.definition;
        }
    }
}
