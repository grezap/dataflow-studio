using DataFlowStudio.SharedKernel;

namespace DataFlowStudio.Modules.Commerce.Contracts;

/// <summary>
/// A change-data-capture event for a <c>dbo.Customers</c> row. Emitted from the Commerce module
/// (via CDC) and consumed by the Ingestion worker, which maps it to an Avro record on the
/// <c>oltp.customers</c> Kafka topic. The wire shape is owned by the AsyncAPI spec (E14); this type
/// is the in-process representation before serialization.
/// </summary>
public sealed record CustomerChangedEvent : IntegrationEvent
{
    /// <summary>Surrogate key of the changed customer row.</summary>
    public required long CustomerId { get; init; }

    /// <summary>Natural key of the changed customer; used as the Kafka partition key.</summary>
    public required string CustomerCode { get; init; }

    /// <summary>Which kind of change this event represents (insert / update / delete).</summary>
    public required ChangeOperation Operation { get; init; }

    /// <inheritdoc />
    public override string Subject => "oltp.customers";
}

/// <summary>
/// CDC operation kind, mirroring the <c>audit.ChangeLog.Operation</c> domain (I / U / D). The enum
/// values are the literal character codes so they round-trip to the source column directly.
/// </summary>
public enum ChangeOperation
{
    /// <summary>Row was inserted (<c>I</c>).</summary>
    Insert = 'I',

    /// <summary>Row was updated (<c>U</c>).</summary>
    Update = 'U',

    /// <summary>Row was deleted (<c>D</c>).</summary>
    Delete = 'D',
}
