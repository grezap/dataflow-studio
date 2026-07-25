using DataFlowStudio.SharedKernel.Telemetry;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>
/// A recording <see cref="IPipelineTelemetrySink"/> for engine tests: it captures every stage event,
/// CDC-lag sample, and error so a test can assert what was emitted (and in what order) without a live
/// Kafka/ClickHouse sink.
/// </summary>
internal sealed class RecordingTelemetrySink : IPipelineTelemetrySink
{
    public List<PipelineStageEvent> Stages { get; } = [];

    public List<CdcLagSample> Lags { get; } = [];

    public List<PipelineError> Errors { get; } = [];

    public int Flushes { get; private set; }

    public void RecordStage(PipelineStageEvent stageEvent) => Stages.Add(stageEvent);

    public void RecordCdcLag(CdcLagSample sample) => Lags.Add(sample);

    public void RecordError(PipelineError error) => Errors.Add(error);

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flushes++;
        return Task.CompletedTask;
    }
}
