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
                var val = this.Value.ToString();
                if (this.Value is string)
                    val = "'" + val + "'";
                return String.Format("{0} = {1}", this.Name, val);
            }
        }

        public override string ToString()
        {
            return this.BuildString;
        }



        public string InnerBuildString
        {
            get 
            {
                var val = this.Value.ToString();
                if (this.Value is string)
                    val = "'" + val + "'";
                return String.Format("'{0}' : {1}", this.Name, val);
            }
        }
    }
}
