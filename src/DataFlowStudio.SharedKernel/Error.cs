namespace DataFlowStudio.SharedKernel;

/// <summary>
/// A structured, allocation-free error value used by the <see cref="Result"/> pattern
/// (MASTER-PLAN E25). The canonical Result/Error primitives will migrate to
/// <c>Nexus.Primitives</c> (nexus-shared) once a second consumer needs them (E8); until
/// then they live here in the SharedKernel.
/// </summary>
public readonly record struct Error(string Code, string Message)
{
    public static readonly Error None = new(string.Empty, string.Empty);

    public static Error Validation(string message) => new("validation", message);

    public static Error NotFound(string message) => new("not_found", message);

    public static Error Conflict(string message) => new("conflict", message);

    public static Error Unexpected(string message) => new("unexpected", message);
}
