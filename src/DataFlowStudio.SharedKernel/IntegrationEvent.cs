namespace DataFlowStudio.SharedKernel;

/// <summary>
/// Base contract for cross-module / change-data-capture integration events. In DataFlow
/// Studio these carry CDC rows from the Commerce (OltpDb) module out toward the Ingestion
/// worker, which serializes them to Avro and publishes to Kafka via the Schema Registry.
/// Serialized shapes are governed by the AsyncAPI spec in <c>docs/api/asyncapi.yaml</c> (E14).
/// </summary>
public abstract record IntegrationEvent
{
    public Guid EventId { get; init; } = Guid.NewGuid();

    public DateTimeOffset OccurredUtc { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>The Kafka subject / topic segment this event maps to (e.g. <c>oltp.customers</c>).</summary>
    public abstract string Subject { get; }
}
