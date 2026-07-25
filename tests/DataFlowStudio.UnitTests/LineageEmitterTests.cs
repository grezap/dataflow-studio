using System.Net;
using DataFlowStudio.Lineage;
using DataFlowStudio.UnitTests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// The Marquez OpenLineage emitter (E16, ADR-0011) against a stub handler: it POSTs a well-formed
/// START / COMPLETE / FAIL run event to <c>/api/v1/lineage</c>, normalizes the OTel trace id into a UUID
/// runId, and is best-effort — a non-2xx or a transport exception is swallowed, never rethrown.
/// </summary>
public sealed class LineageEmitterTests
{
    private static readonly LineageEmitterOptions Options = new() { Endpoint = new Uri("https://marquez.test"), Namespace = "dataflow-studio" };

    private static MarquezLineageEmitter Emitter(StubHttpMessageHandler handler) =>
        new(Options, NullLogger<MarquezLineageEmitter>.Instance, handler);

    [Fact]
    public async Task Start_posts_an_openlineage_start_event_to_the_lineage_endpoint()
    {
        var handler = new StubHttpMessageHandler();
        using var emitter = Emitter(handler);

        await emitter.StartAsync("curation", "run-1", ["oltp.raw.customers"]);

        var (method, uri, body) = handler.Requests.ShouldHaveSingleItem();
        method.ShouldBe(HttpMethod.Post);
        uri.ShouldBe(new Uri("https://marquez.test/api/v1/lineage"));
        body.ShouldContain("\"eventType\":\"START\"");
        body.ShouldContain("\"namespace\":\"dataflow-studio\"");
        body.ShouldContain("\"name\":\"curation\"");
        body.ShouldContain("oltp.raw.customers");
    }

    [Fact]
    public async Task Complete_carries_both_inputs_and_outputs()
    {
        var handler = new StubHttpMessageHandler();
        using var emitter = Emitter(handler);

        await emitter.CompleteAsync("warehouse-sink", "run-2", ["dfs.curated.orders"], ["dwh.fact_order"]);

        var body = handler.Requests.ShouldHaveSingleItem().Body;
        body.ShouldContain("\"eventType\":\"COMPLETE\"");
        body.ShouldContain("dfs.curated.orders");
        body.ShouldContain("dwh.fact_order");
    }

    [Fact]
    public async Task Fail_emits_a_fail_event()
    {
        var handler = new StubHttpMessageHandler();
        using var emitter = Emitter(handler);

        await emitter.FailAsync("curation", "run-3", ["oltp.raw.orders"], "boom");

        handler.Requests.ShouldHaveSingleItem().Body.ShouldContain("\"eventType\":\"FAIL\"");
    }

    [Fact]
    public async Task RunId_that_is_a_32_hex_trace_id_is_normalized_to_a_uuid()
    {
        var handler = new StubHttpMessageHandler();
        using var emitter = Emitter(handler);

        // An OTel trace id (32 hex, no dashes) must serialize as a dashed UUID so the Marquez run shares
        // its id with the Tempo trace + ClickHouse pipeline_events.
        await emitter.StartAsync("curation", "0af7651916cd43dd8448eb211c80319c", []);

        handler.Requests.ShouldHaveSingleItem().Body
            .ShouldContain("\"runId\":\"0af76519-16cd-43dd-8448-eb211c80319c\"");
    }

    [Fact]
    public async Task A_non_2xx_response_is_swallowed()
    {
        var handler = new StubHttpMessageHandler { ResponseStatus = HttpStatusCode.InternalServerError };
        using var emitter = Emitter(handler);

        await Should.NotThrowAsync(() => emitter.StartAsync("curation", "run-4", []));
        handler.Requests.ShouldHaveSingleItem();   // it still tried
    }

    [Fact]
    public async Task A_transport_exception_is_swallowed_best_effort()
    {
        var handler = new StubHttpMessageHandler { ThrowOnSend = new HttpRequestException("no route to host") };
        using var emitter = Emitter(handler);

        await Should.NotThrowAsync(() => emitter.CompleteAsync("curation", "run-5", ["in"], ["out"]));
    }
}
