namespace CqlSharp.Fluent.Definition
{
    public enum CommandOption{Simple, Map};
    public interface IOption
    {
        CommandOption OptionType { get; }
        string BuildString { get; }
    }
}
