using System.Net;

namespace DataFlowStudio.UnitTests.Fakes;

/// <summary>
/// A stub <see cref="HttpMessageHandler"/> for the OpenLineage emitter tests: it records each request
/// (method, URI, body) and returns a configurable status code, or throws a configured exception so the
/// emitter's best-effort swallow can be exercised — no live Marquez.
/// </summary>
internal sealed class StubHttpMessageHandler : HttpMessageHandler
{
    public List<(HttpMethod Method, Uri? Uri, string Body)> Requests { get; } = [];

    public HttpStatusCode ResponseStatus { get; set; } = HttpStatusCode.OK;

    public Exception? ThrowOnSend { get; set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        Requests.Add((request.Method, request.RequestUri, body));

        if (ThrowOnSend is { } ex)
        {
            throw ex;
        }

        return new HttpResponseMessage(ResponseStatus);
    }
}
