namespace DataFlowStudio.SharedKernel.Lineage;

/// <summary>
/// The pipeline's data-lineage contract (E16). A stage (curation, warehouse-sink) reports its job run
/// as a START then a COMPLETE (or FAIL) so an OpenLineage backend — Marquez — can render the dataset
/// graph that answers "if I change this source, what breaks downstream?". Defined in the SharedKernel
/// so a module never references the concrete emitter (module isolation, ADR-0001), exactly like
/// <see cref="Telemetry.IPipelineTelemetrySink"/>.
/// <para>
/// Datasets are passed as names (e.g. <c>oltp.OltpDb.dbo.Customers</c>, <c>dfs.customers.changed.v1</c>,
/// <c>dwh.dim_customer</c>); the emitter places them all in its configured OpenLineage namespace, so the
/// raw → curated → DWH graph is one connected lineage. The <c>runId</c> is the run's OpenTelemetry trace
/// id (as a UUID), so a single run is one correlated entity across three planes — a Tempo trace,
/// ClickHouse <c>pipeline_events</c>, and a Marquez run. Emission is best-effort: an unreachable Marquez
/// must never fail the pipeline.
/// </para>
/// </summary>
public interface ILineageEmitter
{
    /// <summary>Emits an OpenLineage START run event for a job.</summary>
    /// <param name="jobName">The job (e.g. <c>curation</c>, <c>warehouse-sink</c>).</param>
    /// <param name="runId">The run id (the OTel trace id as a UUID) tying this to the trace + telemetry.</param>
    /// <param name="inputs">The dataset names the job reads.</param>
    /// <param name="cancellationToken">Cancels the emit.</param>
    Task StartAsync(string jobName, string runId, IReadOnlyList<string> inputs, CancellationToken cancellationToken = default);

    /// <summary>Emits an OpenLineage COMPLETE run event, recording the outputs the run produced.</summary>
    /// <param name="jobName">The job.</param>
    /// <param name="runId">The run id from <see cref="StartAsync"/>.</param>
    /// <param name="inputs">The dataset names the job read.</param>
    /// <param name="outputs">The dataset names the job wrote.</param>
    /// <param name="cancellationToken">Cancels the emit.</param>
    Task CompleteAsync(string jobName, string runId, IReadOnlyList<string> inputs, IReadOnlyList<string> outputs, CancellationToken cancellationToken = default);

    /// <summary>Emits an OpenLineage FAIL run event when a run aborts.</summary>
    /// <param name="jobName">The job.</param>
    /// <param name="runId">The run id from <see cref="StartAsync"/>.</param>
    /// <param name="inputs">The dataset names the job read.</param>
    /// <param name="errorMessage">The failure detail.</param>
    /// <param name="cancellationToken">Cancels the emit.</param>
    Task FailAsync(string jobName, string runId, IReadOnlyList<string> inputs, string errorMessage, CancellationToken cancellationToken = default);
}

/// <summary>
/// The no-op lineage emitter. Stages default to this so a run that is not wired for lineage (no Marquez
/// endpoint configured) still executes cleanly — every emit call simply does nothing.
/// </summary>
public sealed class NullLineageEmitter : ILineageEmitter
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly NullLineageEmitter Instance = new();

    private NullLineageEmitter()
    {
    }

    /// <inheritdoc />
    public Task StartAsync(string jobName, string runId, IReadOnlyList<string> inputs, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task CompleteAsync(string jobName, string runId, IReadOnlyList<string> inputs, IReadOnlyList<string> outputs, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task FailAsync(string jobName, string runId, IReadOnlyList<string> inputs, string errorMessage, CancellationToken cancellationToken = default) => Task.CompletedTask;
}
