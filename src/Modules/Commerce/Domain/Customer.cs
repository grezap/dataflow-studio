using DataFlowStudio.SharedKernel;

namespace DataFlowStudio.Modules.Commerce.Domain;

/// <summary>
/// Domain projection of <c>dbo.Customers</c> (system-versioned temporal table). Mirrors the
/// authored DDL in <c>schemas/dataflow-studio/README.md</c>; persisted via Dapper.
/// </summary>
public sealed record Customer
{
    public long CustomerId { get; init; }

    public required string CustomerCode { get; init; }

    public required string DisplayName { get; init; }

    public required string Email { get; init; }

    public string? PhoneE164 { get; init; }

    public string PreferredLocale { get; init; } = "en-US";

    public byte Status { get; init; } = 1;

    public decimal LifetimeValueUsd { get; init; }

    public required AuditColumns Audit { get; init; }
}
