namespace DataFlowStudio.SharedKernel;

/// <summary>
/// A structured, allocation-free error value used by the <see cref="Result"/> pattern
/// (MASTER-PLAN E25). Being a <c>readonly record struct</c>, an <see cref="Error"/> costs no heap
/// allocation and compares by value — cheap enough to return from any method.
/// The canonical Result/Error primitives migrate to <c>Nexus.Primitives</c> (nexus-shared) once a
/// second consumer needs them (E8); until then they live here in the SharedKernel.
/// </summary>
/// <param name="Code">Stable, machine-readable category (e.g. <c>validation</c>, <c>not_found</c>).</param>
/// <param name="Message">Human-readable detail for logs / Problem Details responses.</param>
public readonly record struct Error(string Code, string Message)
{
    /// <summary>The "no error" sentinel carried by every successful <see cref="Result"/>.</summary>
    public static readonly Error None = new(string.Empty, string.Empty);

    /// <summary>A caller-input / business-rule violation (maps to HTTP 400).</summary>
    public static Error Validation(string message) => new("validation", message);

    /// <summary>A requested entity does not exist (maps to HTTP 404).</summary>
    public static Error NotFound(string message) => new("not_found", message);

    /// <summary>A concurrency / state conflict, e.g. a stale <c>ROWVERSION</c> (maps to HTTP 409).</summary>
    public static Error Conflict(string message) => new("conflict", message);

    /// <summary>An unexpected failure that is not the caller's fault (maps to HTTP 500).</summary>
    public static Error Unexpected(string message) => new("unexpected", message);
}
