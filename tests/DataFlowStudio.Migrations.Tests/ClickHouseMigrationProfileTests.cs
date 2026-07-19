using DataFlowStudio.Migrations.Clickhouse;
using Shouldly;
using Xunit;

namespace DataFlowStudio.Migrations.Tests;

/// <summary>
/// Topology-selection unit tests for the ClickHouse migration profiles (no container). The lab profile
/// applies every script — including the native Kafka-engine ingestion (Script0005) — and carries the
/// broker/group substitution variables; the single-node profile excludes Script0005 so the E1
/// idempotency gate stays green on a broker-less container (ADR-0008).
/// </summary>
public sealed class ClickHouseMigrationProfileTests
{
    [Fact]
    public void Lab_profile_applies_every_script_and_carries_the_kafka_variables()
    {
        var lab = ClickHouseMigrationProfile.Lab();

        lab.ExcludedScripts.ShouldBeEmpty("the replicated lab runs the native Kafka-engine ingestion");
        lab.Variables.ShouldContainKey("kafka_brokers");
        lab.Variables.ShouldContainKey("kafka_group_pipeline_events");
        lab.Variables.ShouldContainKey("kafka_group_cdc_lag");
        lab.Variables.ShouldContainKey("kafka_group_error_events");
    }

    [Fact]
    public void SingleNode_profile_excludes_the_kafka_ingestion_script()
    {
        var single = ClickHouseMigrationProfile.SingleNode();

        single.ExcludedScripts.ShouldContain(
            "Script0005_kafka_ingestion.sql",
            "a lone CI container has no Kafka broker to consume from");
    }
}
