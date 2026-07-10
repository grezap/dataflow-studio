using DataFlowStudio.SharedKernel;

namespace DataFlowStudio.Modules.Commerce.Contracts;

/// <summary>
/// A change-data-capture event for a <c>dbo.Customers</c> row. Emitted from the Commerce
/// module (CDC) and consumed by the Ingestion worker, which maps it to an Avro record on the
/// <c>oltp.customers</c> Kafka topic. The wire shape is owned by the AsyncAPI spec (E14).
/// </summary>
public sealed record CustomerChangedEvent : IntegrationEvent
{
    public required long CustomerId { get; init; }

    public required string CustomerCode { get; init; }

    public required ChangeOperation Operation { get; init; }

    public override string Subject => "oltp.customers";
}

/// <summary>CDC operation kind, mirroring the <c>audit.ChangeLog.Operation</c> domain (I / U / D).</summary>
public enum ChangeOperation
{
    Insert = 'I',
    Update = 'U',
    Delete = 'D',
}
