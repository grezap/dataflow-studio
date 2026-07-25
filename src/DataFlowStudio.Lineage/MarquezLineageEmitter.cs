using System.Globalization;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using DataFlowStudio.SharedKernel.Lineage;
using Microsoft.Extensions.Logging;

namespace DataFlowStudio.Lineage;

/// <summary>
/// Emits OpenLineage run events (START / COMPLETE / FAIL) to Marquez at <c>POST /api/v1/lineage</c>
/// (E16, ADR-0011). It POSTs from the build host straight to the front door by IP, validating the
/// private-CA leaf against the supplied lab root (the same custom-root trust the OTLP exporter uses).
/// Emission is <b>best-effort</b>: an unreachable or erroring Marquez is logged and swallowed so the
/// pipeline run is never disrupted by its own lineage side-channel.
/// </summary>
public sealed partial class MarquezLineageEmitter : ILineageEmitter, IDisposable
{
    private readonly LineageEmitterOptions _options;
    private readonly HttpClient _http;
    private readonly ILogger<MarquezLineageEmitter> _logger;

    /// <summary>Creates the emitter, pinning the Marquez front-door certificate to the lab root when supplied.</summary>
    /// <param name="options">Endpoint, namespace, producer, and private-CA trust.</param>
    /// <param name="logger">Diagnostics log for best-effort emit failures.</param>
    public MarquezLineageEmitter(LineageEmitterOptions options, ILogger<MarquezLineageEmitter> logger)
        : this(options, logger, BuildHandler(options))
    {
    }

    // Seam ctor: the tests inject a stub HttpMessageHandler so the OpenLineage event shape + best-effort
    // error handling are covered without a live Marquez. The public ctor supplies the real private-CA
    // pinning handler.
    internal MarquezLineageEmitter(LineageEmitterOptions options, ILogger<MarquezLineageEmitter> logger, HttpMessageHandler handler)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _logger = logger;
        _http = new HttpClient(handler) { BaseAddress = options.Endpoint, Timeout = TimeSpan.FromSeconds(20) };
    }

    private static HttpClientHandler BuildHandler(LineageEmitterOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var handler = new HttpClientHandler();
        if (options.ServerCaCertificates is { Count: > 0 } roots)
        {
            handler.ServerCertificateCustomValidationCallback = (_, cert, chain, errors) =>
                ValidateServerCertificate(cert, chain, errors, roots);
        }

        return handler;
    }

    /// <inheritdoc />
    public Task StartAsync(string jobName, string runId, IReadOnlyList<string> inputs, CancellationToken cancellationToken = default) =>
        EmitAsync("START", jobName, runId, inputs, [], cancellationToken);

    /// <inheritdoc />
    public Task CompleteAsync(string jobName, string runId, IReadOnlyList<string> inputs, IReadOnlyList<string> outputs, CancellationToken cancellationToken = default) =>
        EmitAsync("COMPLETE", jobName, runId, inputs, outputs, cancellationToken);

    /// <inheritdoc />
    public Task FailAsync(string jobName, string runId, IReadOnlyList<string> inputs, string errorMessage, CancellationToken cancellationToken = default)
    {
        LogFail(_logger, jobName, errorMessage);
        return EmitAsync("FAIL", jobName, runId, inputs, [], cancellationToken);
    }

    private async Task EmitAsync(
        string eventType,
        string jobName,
        string runId,
        IReadOnlyList<string> inputs,
        IReadOnlyList<string> outputs,
        CancellationToken cancellationToken)
    {
        try
        {
            var runEvent = new RunEvent(
                eventType,
                DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture),
                _options.Producer.ToString(),
                new RunRef(NormalizeRunId(runId)),
                new JobRef(_options.Namespace, jobName),
                [.. inputs.Select(name => new DatasetRef(_options.Namespace, name))],
                [.. outputs.Select(name => new DatasetRef(_options.Namespace, name))]);

            var json = JsonSerializer.Serialize(runEvent, LineageJsonContext.Default.RunEvent);
            using var content = new StringContent(json, Encoding.UTF8, "application/json");
            using var response = await _http.PostAsync(new Uri("/api/v1/lineage", UriKind.Relative), content, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                LogNon2xx(_logger, eventType, jobName, (int)response.StatusCode);
            }
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException)
        {
            LogEmitFailed(_logger, eventType, jobName, ex.Message);   // best-effort: never break the run
        }
    }

    // OpenLineage runIds are UUIDs. The pipeline passes its OTel trace id (32 hex, no dashes), so format
    // it as a UUID — the lineage run then shares its id with the Tempo trace + ClickHouse pipeline_events.
    private static string NormalizeRunId(string runId) =>
        Guid.TryParse(runId, out var g) ? g.ToString("D") : runId;

    // Accept the front door's leaf iff it chains to one of the supplied private roots and the endpoint's
    // host still matches a SAN. Overrides only the "untrusted root" verdict (the lab CA isn't in the OS
    // store); a name mismatch or a missing certificate is still fatal. Mirrors Nexus.Observability.
    // Only ever invoked during a live TLS handshake → exercised against the lab Marquez, not unit tests (ADR-0012).
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static bool ValidateServerCertificate(
        X509Certificate2? certificate,
        X509Chain? presentedChain,
        SslPolicyErrors errors,
        X509Certificate2Collection roots)
    {
        if (certificate is null || errors.HasFlag(SslPolicyErrors.RemoteCertificateNotAvailable)
            || errors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch))
        {
            return false;
        }

        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.CustomTrustStore.AddRange(roots);

        if (presentedChain is not null)
        {
            foreach (var element in presentedChain.ChainElements)
            {
                chain.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        return chain.Build(certificate);
    }

    /// <summary>Disposes the underlying HttpClient.</summary>
    public void Dispose() => _http.Dispose();

    [LoggerMessage(Level = LogLevel.Warning, Message = "OpenLineage {EventType} for job '{Job}' returned HTTP {Status}.")]
    private static partial void LogNon2xx(ILogger logger, string eventType, string job, int status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "OpenLineage {EventType} for job '{Job}' failed (best-effort, ignored): {Error}")]
    private static partial void LogEmitFailed(ILogger logger, string eventType, string job, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Pipeline job '{Job}' failed: {Error}")]
    private static partial void LogFail(ILogger logger, string job, string error);
}
