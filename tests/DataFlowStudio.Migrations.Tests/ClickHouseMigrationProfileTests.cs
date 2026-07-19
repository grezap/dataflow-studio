using DataFlowStudio.Migrations.Clickhouse;
using FluentAssertions;
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

        lab.ExcludedScripts.Should().BeEmpty("the replicated lab runs the native Kafka-engine ingestion");
        lab.Variables.Should().ContainKey("kafka_brokers");
        lab.Variables.Should().ContainKey("kafka_group_pipeline_events");
        lab.Variables.Should().ContainKey("kafka_group_cdc_lag");
        lab.Variables.Should().ContainKey("kafka_group_error_events");
    }

    [Fact]
    public void SingleNode_profile_excludes_the_kafka_ingestion_script()
    {
        var single = ClickHouseMigrationProfile.SingleNode();

        single.ExcludedScripts.Should().Contain(
            "Script0005_kafka_ingestion.sql",
            "a lone CI container has no Kafka broker to consume from");
    }
}
