namespace DataFlowStudio.SharedKernel;

/// <summary>
/// The six standard audit columns mandated by MASTER-PLAN enhancement E6 and present on
/// every business table in <c>schemas/dataflow-studio</c>:
/// <c>created_utc, created_by, modified_utc, modified_by, row_version, is_deleted</c>.
/// </summary>
public sealed record AuditColumns
{
    public required DateTime CreatedUtc { get; init; }

    public required string CreatedBy { get; init; }

    public required DateTime ModifiedUtc { get; init; }

    public required string ModifiedBy { get; init; }

    /// <summary>SQL Server <c>ROWVERSION</c> (8-byte, database-assigned) for optimistic concurrency.</summary>
    public required byte[] RowVersion { get; init; }

    public bool IsDeleted { get; init; }
}
