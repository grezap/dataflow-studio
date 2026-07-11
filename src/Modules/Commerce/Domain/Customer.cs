using Nexus.Primitives;

namespace DataFlowStudio.Modules.Commerce.Domain;

/// <summary>
/// Domain projection of <c>dbo.Customers</c> (the system-versioned temporal source table). Mirrors
/// the authored DDL in <c>schemas/dataflow-studio/README.md</c> and is persisted via Dapper (no EF
/// Core on any DataFlow Studio path). The temporal history behind this row feeds the SCD2
/// <c>dim_customer</c> load in the StarRocks warehouse.
/// </summary>
public sealed record Customer
{
    /// <summary>Surrogate identity (<c>CustomerId BIGINT IDENTITY</c>), the primary key.</summary>
    public long CustomerId { get; init; }

    /// <summary>Stable natural key used across the pipeline and in Kafka message keys.</summary>
    public required string CustomerCode { get; init; }

    /// <summary>Display name shown in UIs and reports.</summary>
    public required string DisplayName { get; init; }

    /// <summary>Contact email (unique per business rules).</summary>
    public required string Email { get; init; }

    /// <summary>Optional phone number in E.164 format.</summary>
    public string? PhoneE164 { get; init; }

    /// <summary>Preferred locale (BCP-47); defaults to <c>en-US</c>.</summary>
    public string PreferredLocale { get; init; } = "en-US";

    /// <summary>Lifecycle status code (1 = active); a <c>TINYINT</c> domain in the source.</summary>
    public byte Status { get; init; } = 1;

    /// <summary>Running lifetime value in USD; denormalized onto the customer for fast reads.</summary>
    public decimal LifetimeValueUsd { get; init; }

    /// <summary>The standard six audit columns (E6) carried by every business entity.</summary>
    public required AuditColumns Audit { get; init; }
}
