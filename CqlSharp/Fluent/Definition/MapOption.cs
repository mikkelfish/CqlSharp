using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace CqlSharp.Fluent.Definition
{
    internal class MapOption : IOption
    {
        private readonly List<IOption> backing = new List<IOption>();
        public IReadOnlyCollection<IOption> Options
        {
            get
            {
                return new ReadOnlyCollection<IOption>(this.backing);
            }
        }

        public string Name { get; private set; }

        public MapOption(string name, params IOption[] options)
        {
            this.Name = name;
            populate(options);
        }

        public MapOption(string name, IEnumerable<IOption> options)
        {
            this.Name = name;
            populate(options);
        }

        public void AddOptions(IEnumerable<IOption> options)
        {
            this.populate(options);
        }

        protected bool ValidateOptions(IEnumerable<IOption> options)
        {
            return true;
        }

        private void populate(IEnumerable<IOption> options)
        {
            var array = options.ToArray();
            if (!this.ValidateOptions(array))
                throw new ArgumentException("An option in the map is invalid");

            foreach (var option in array)
            {
                this.backing.Add(option);
            }
        }

        public CommandOption OptionType
        {
            get { return CommandOption.Map; }
        }

        public override string ToString()
        {
            return this.BuildString;
        }


        public string BuildString
        {
            get
            {
                

                var toRet = new StringBuilder(this.Name + " = {");
                for (int i = 0; i < this.backing.Count - 1; i++)
                {
                    toRet.Append(this.backing[i].InnerBuildString + ", ");
                }

                if (this.backing.Count > 0)
                    toRet.Append(this.backing.Last().InnerBuildString);

                toRet.Append("}");
                return toRet.ToString();
            }
        }


        public string InnerBuildString
        {
            get { throw new NotImplementedException("Maps cannot be inside each other"); }
        }
    }
}
