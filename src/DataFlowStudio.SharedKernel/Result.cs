namespace DataFlowStudio.SharedKernel;

/// <summary>
/// A minimal, AOT-safe Result pattern (no reflection, no exceptions for control flow).
/// Application-layer methods return <see cref="Result"/> / <see cref="Result{T}"/> rather than
/// throwing for <i>expected</i> failures (MASTER-PLAN E25) — the caller must inspect the outcome,
/// which makes error handling explicit instead of hidden in a catch block.
/// </summary>
public readonly struct Result
{
    // Private ctor forces construction through the Success/Failure factories, so an instance can
    // never be in the contradictory "success + has error" state.
    private Result(bool isSuccess, Error error)
    {
        IsSuccess = isSuccess;
        Error = error;
    }

    /// <summary><c>true</c> when the operation succeeded.</summary>
    public bool IsSuccess { get; }

    /// <summary>Convenience inverse of <see cref="IsSuccess"/>.</summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>The failure detail; equals <see cref="Error.None"/> on success.</summary>
    public Error Error { get; }

    /// <summary>Creates a successful result.</summary>
    public static Result Success() => new(true, Error.None);

    /// <summary>Creates a failed result carrying <paramref name="error"/>.</summary>
    public static Result Failure(Error error) => new(false, error);

    /// <summary>Lets a method <c>return someError;</c> directly, without calling <see cref="Failure"/>.</summary>
    public static implicit operator Result(Error error) => Failure(error);
}

/// <summary>A <see cref="Result"/> that carries a value on success.</summary>
/// <typeparam name="T">The value type returned when the operation succeeds.</typeparam>
public readonly struct Result<T>
{
    // Held only when IsSuccess; nullable so a failed result carries default(T) without boxing.
    private readonly T? _value;

    private Result(bool isSuccess, T? value, Error error)
    {
        IsSuccess = isSuccess;
        _value = value;
        Error = error;
    }

    /// <summary><c>true</c> when the operation succeeded and <see cref="Value"/> is available.</summary>
    public bool IsSuccess { get; }

    /// <summary>Convenience inverse of <see cref="IsSuccess"/>.</summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>The failure detail; equals <see cref="Error.None"/> on success.</summary>
    public Error Error { get; }

    /// <summary>
    /// The success value. Accessing it on a failed result is a programming error, so it throws
    /// rather than silently returning <c>default</c> and masking the bug.
    /// </summary>
    /// <exception cref="InvalidOperationException">The result is a failure.</exception>
    public T Value => IsSuccess
        ? _value!
        : throw new InvalidOperationException("Cannot access the value of a failed Result.");

    /// <summary>Creates a successful result wrapping <paramref name="value"/>.</summary>
    public static Result<T> Success(T value) => new(true, value, Error.None);

    /// <summary>Creates a failed result carrying <paramref name="error"/>.</summary>
    public static Result<T> Failure(Error error) => new(false, default, error);

    /// <summary>Lets a method <c>return theValue;</c> directly on the success path.</summary>
    public static implicit operator Result<T>(T value) => Success(value);

    /// <summary>Lets a method <c>return someError;</c> directly on the failure path.</summary>
    public static implicit operator Result<T>(Error error) => Failure(error);
}
