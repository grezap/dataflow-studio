using Confluent.Kafka;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>How a <see cref="FakeProducer{TKey,TValue}"/> should behave on produce.</summary>
internal enum ProduceBehavior
{
    /// <summary>Delivery succeeds.</summary>
    Ok,

    /// <summary>The delivery report carries an error (async delivery failure) — drives the fire-and-forget error handler.</summary>
    DeliveryError,

    /// <summary>Produce throws <see cref="ProduceException{TKey,TValue}"/> (broker unreachable — can't even enqueue).</summary>
    ThrowProduceException,
}

/// <summary>
/// A recording <see cref="IProducer{TKey,TValue}"/> for the curation + telemetry sink tests. It captures
/// every produced message and can simulate a delivery-report error or a synchronous produce exception,
/// so the dual-path error handling (ADR-0008) is testable without a broker. Only the members the code
/// under test uses are implemented; the rest throw.
/// </summary>
internal sealed class FakeProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    public List<(string Topic, TKey Key, TValue Value)> Produced { get; } = [];

    public ProduceBehavior Behavior { get; set; } = ProduceBehavior.Ok;

    public int Flushes { get; private set; }

    public bool Disposed { get; private set; }

    public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null)
    {
        if (Behavior == ProduceBehavior.ThrowProduceException)
        {
            throw new ProduceException<TKey, TValue>(
                new Error(ErrorCode.Local_Transport, "fake: broker unreachable"),
                new DeliveryResult<TKey, TValue> { Topic = topic, Message = message });
        }

        Produced.Add((topic, message.Key, message.Value));
        deliveryHandler?.Invoke(new DeliveryReport<TKey, TValue>
        {
            Topic = topic,
            Message = message,
            Error = Behavior == ProduceBehavior.DeliveryError
                ? new Error(ErrorCode.Local_MsgTimedOut, "fake: delivery timed out")
                : new Error(ErrorCode.NoError),
        });
    }

    public void Produce(TopicPartition topicPartition, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null) =>
        Produce(topicPartition.Topic, message, deliveryHandler);

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(string topic, Message<TKey, TValue> message, CancellationToken cancellationToken = default)
    {
        if (Behavior == ProduceBehavior.ThrowProduceException)
        {
            throw new ProduceException<TKey, TValue>(
                new Error(ErrorCode.Local_Transport, "fake: broker unreachable"),
                new DeliveryResult<TKey, TValue> { Topic = topic, Message = message });
        }

        Produced.Add((topic, message.Key, message.Value));
        return Task.FromResult(new DeliveryResult<TKey, TValue>
        {
            Topic = topic,
            Message = message,
            Status = PersistenceStatus.Persisted,
        });
    }

    public Task<DeliveryResult<TKey, TValue>> ProduceAsync(TopicPartition topicPartition, Message<TKey, TValue> message, CancellationToken cancellationToken = default) =>
        ProduceAsync(topicPartition.Topic, message, cancellationToken);

    public int Flush(TimeSpan timeout)
    {
        Flushes++;
        return 0;
    }

    public void Flush(CancellationToken cancellationToken = default) => Flushes++;

    public void Dispose() => Disposed = true;

    // ---- Unused IProducer surface ----
    public Handle Handle => throw new NotSupportedException();

    public string Name => "fake-producer";

    public int AddBrokers(string brokers) => throw new NotSupportedException();

    public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

    public int Poll(TimeSpan timeout) => throw new NotSupportedException();

    public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

    public void BeginTransaction() => throw new NotSupportedException();

    public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void CommitTransaction() => throw new NotSupportedException();

    public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void AbortTransaction() => throw new NotSupportedException();

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) =>
        throw new NotSupportedException();
}
