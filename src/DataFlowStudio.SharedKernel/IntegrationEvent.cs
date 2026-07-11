namespace DataFlowStudio.SharedKernel;

/// <summary>
/// Base contract for cross-module / change-data-capture integration events. In DataFlow Studio
/// these carry CDC rows from the Commerce (OltpDb) module out toward the Ingestion worker, which
/// serializes them to Avro and publishes to Kafka via the Schema Registry. Because this abstraction
/// lives in the SharedKernel, the Ingestion worker can handle events from <i>any</i> module without
/// referencing that module — which is what keeps the modules independent (ADR-0001).
/// Serialized shapes are governed by the AsyncAPI spec in <c>docs/api/asyncapi.yaml</c> (E14).
/// </summary>
public abstract record IntegrationEvent
{
    /// <summary>Unique id for this event instance — the idempotency key for at-least-once delivery.</summary>
    public Guid EventId { get; init; } = Guid.NewGuid();

    /// <summary>When the underlying change occurred (UTC); becomes the Kafka record timestamp.</summary>
    public DateTimeOffset OccurredUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>The Kafka subject / topic segment this event maps to (e.g. <c>oltp.customers</c>).</summary>
    public abstract string Subject { get; }
}
