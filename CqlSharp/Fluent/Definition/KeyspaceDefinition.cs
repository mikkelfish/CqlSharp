using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Fluent.Definition
{
    public abstract class CqlKeyspaceDefinition : IFluentCommand
    {
        internal class CreateKeyspace : CqlKeyspaceDefinition
        {
            internal CreateKeyspace(string keyspace)
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
        
        internal class AlterKeyspace : CqlKeyspaceDefinition
        {
            internal AlterKeyspace(string keyspace)
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

        private readonly string keyspace;
        private SimpleOption<bool> durable;
        internal MapOption replication;
        private bool throwError;

        internal CqlKeyspaceDefinition(string keyspace)
        {
            this.keyspace = keyspace;
            this.durable = new SimpleOption<bool>("durable_writes", true);
        }

        protected abstract string Command { get; }
        protected abstract string ErrorOverwrite { get; }

        /// <summary>
        /// Whether to use the commit log for updates on this keyspace (disable this option at your own risk!)
        /// </summary>
        /// <param name="durability">Durability setting, default true</param>
        /// <returns></returns>
        public CqlKeyspaceDefinition SetDurability(bool durability)
        {
            this.durable = new SimpleOption<bool>("durable_writes", durability);
            return this;
        }

        /// <summary>
        /// Attempting to create an already existing keyspace will return an error unless the throwOnError is false. 
        /// If it is false, the statement will be a no-op if the keyspace already exists.
        /// </summary>
        /// <param name="throwOnError">Whether to throw if the keyspace already exists</param>
        /// <returns></returns>
        public CqlKeyspaceDefinition SetThrowOnError(bool throwOnError)
        {
            this.throwError = throwOnError;
            return this;
        }

        public string BuildString
        {
            get 
            {
                return String.Format("{0} KEYSPACE {1} {2} WITH {3} AND {4};", 
                    this.Command,
                    !this.throwError ? ErrorOverwrite : "",
                    this.keyspace, 
                    this.replication.BuildString, 
                    this.durable.BuildString);
            }
        }
    }

    public class CqlKeyspaceNamed
    {
        private readonly CqlKeyspaceDefinition definition;
        internal CqlKeyspaceNamed(CqlKeyspaceDefinition definition)
        {
            this.definition = definition;
        }

        /// <summary>
        /// A simple strategy that defines a simple replication factor for the whole cluster. The only sub-options supported is 'replication_factor' to define that replication factor and is mandatory.
        /// </summary>
        /// <param name="replicationFactor">The replication factor to use</param>
        /// <returns></returns>
        public CqlKeyspaceDefinition WithSimpleStrategy(int replicationFactor)
        {
            this.definition.replication = new MapOption("replication", 
                new SimpleOption<string>("class", "SimpleStrategy"),
                new SimpleOption<int>("replication_factor", replicationFactor, rf => rf > 0));
            return this.definition;
        }

        /// <summary>
        /// A replication strategy that allows to set the replication factor independently for each data-center.
        /// The rest of the sub-options are key-value pairs where each time the key is the name of a datacenter and the value the replication factor for that data-center.
        /// </summary>
        /// <param name="topology">The topology to use in the strategy</param>
        /// <returns></returns>
        public CqlKeyspaceDefinition WithNetworkTopologyStrategy(IDictionary<string, int> topology)
        {
            var transformed = topology
                .Select(kvp => new SimpleOption<int>(kvp.Key, kvp.Value, rf => rf > 0))
                .Cast<IOption>();

            this.definition.replication = new MapOption("replication",
                transformed.Union(new[] { new SimpleOption<string>("class", "SimpleStrategy") }));
            return this.definition;
        }
    }
}
