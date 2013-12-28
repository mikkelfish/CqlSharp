using System;

namespace CqlSharp.Fluent.Definition
{
    internal class SimpleOption<T> : IOption
    {
        public string Name { get; private set; }
        public T Value { get; private set; }

        public SimpleOption(string name, T value, Func<T, bool> validate = null)
        {
            if (validate != null)
            {
                if (!validate(value))
                    throw new ArgumentException("The value " + value + " is not valid for option " + name);
            }

            this.Name = name;
            this.Value = value;
        }

        public CommandOption OptionType
        {
            get { return CommandOption.Simple; }
        }

        public string BuildString
        {
            get 
            {
                return String.Format("'{0}' : {1}", this.Name, this.Value);
            }
        }

        public override string ToString()
        {
            return this.BuildString;
        }

    }
}
