using DataFlowStudio.Modules.Telemetry;
using DataFlowStudio.SharedKernel.Telemetry;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>A recording <see cref="IErrorFallbackSink"/> so the sink's fallback path is observable.</summary>
internal sealed class RecordingErrorFallbackSink : IErrorFallbackSink
{
    public List<PipelineError> Inserted { get; } = [];

    public Task InsertAsync(PipelineError error, CancellationToken cancellationToken = default)
    {
        Inserted.Add(error);
        return Task.CompletedTask;
    }
}
