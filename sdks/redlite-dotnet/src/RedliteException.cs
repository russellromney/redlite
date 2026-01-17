namespace Redlite;

/// <summary>
/// Exception thrown by Redlite operations.
/// </summary>
public class RedliteException : Exception
{
    public RedliteException()
    {
    }

    public RedliteException(string message) : base(message)
    {
    }

    public RedliteException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
