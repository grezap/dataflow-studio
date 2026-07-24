using System.Security.Cryptography.X509Certificates;

namespace DataFlowStudio.Lineage;

/// <summary>
/// Settings for <see cref="MarquezLineageEmitter"/>. The endpoint is the Marquez base URL (the nginx
/// TLS front door); the emitter appends <c>/api/v1/lineage</c>. Because the front door presents a
/// private-CA leaf, supply the lab PKI root via <see cref="ServerCaCertificates"/> — the front door
/// serves its own intermediate, so the root alone completes the chain (and its leaf carries an IP SAN,
/// so a WORKGROUP host can reach it by IP).
/// </summary>
public sealed record LineageEmitterOptions
{
    /// <summary>The Marquez base URL, e.g. <c>https://192.168.70.127</c>.</summary>
    public required Uri Endpoint { get; init; }

    /// <summary>The OpenLineage namespace stamped on every job + dataset. Default <c>dataflow-studio</c>.</summary>
    public string Namespace { get; init; } = "dataflow-studio";

    /// <summary>The OpenLineage <c>producer</c> URI (who emitted the event).</summary>
    public Uri Producer { get; init; } = new("https://github.com/grezap/dataflow-studio");

    /// <summary>Private-CA root(s) to trust for the Marquez front-door server certificate; null uses OS trust.</summary>
    public X509Certificate2Collection? ServerCaCertificates { get; init; }
}
