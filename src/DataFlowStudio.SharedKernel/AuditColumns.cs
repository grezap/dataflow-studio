namespace DataFlowStudio.SharedKernel;

/// <summary>
/// The six standard audit columns mandated by MASTER-PLAN enhancement E6 and present on every
/// business table in <c>schemas/dataflow-studio</c>:
/// <c>created_utc, created_by, modified_utc, modified_by, row_version, is_deleted</c>.
/// Modelled as a reusable value so every OLTP entity carries an identical audit footprint, and CDC
/// events can ship the audit trail downstream unchanged.
/// </summary>
public sealed record AuditColumns
{
    /// <summary>UTC timestamp the row was first inserted (<c>created_utc</c>).</summary>
    public required DateTime CreatedUtc { get; init; }

    /// <summary>Identity (user or service) that created the row (<c>created_by</c>).</summary>
    public required string CreatedBy { get; init; }

    /// <summary>UTC timestamp of the most recent update (<c>modified_utc</c>).</summary>
    public required DateTime ModifiedUtc { get; init; }

    /// <summary>Identity that last modified the row (<c>modified_by</c>).</summary>
    public required string ModifiedBy { get; init; }

    /// <summary>
    /// SQL Server <c>ROWVERSION</c> (8-byte, database-assigned, monotonically increasing) used for
    /// optimistic concurrency — an update matches on the value it read, so a concurrent change is
    /// detected as a 0-row update instead of a lost write.
    /// </summary>
    public required byte[] RowVersion { get; init; }

    /// <summary>Soft-delete flag (<c>is_deleted</c>); rows are tombstoned, not physically removed.</summary>
    public bool IsDeleted { get; init; }
}
