using Avro;
using DataFlowStudio.Modules.Ingestion.Curation;
using FluentAssertions;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// Unit tests for the pure curation logic: every catalog spec produces a valid Avro schema, and the
/// projector reshapes a Debezium <c>after</c> image into the curated record (types, nulls, key,
/// envelope) without any Kafka or lab dependency.
/// </summary>
public sealed class CurationTests
{
    private static EntityCurationSpec Spec(string entity) =>
        CurationCatalog.All.Single(s => s.Entity == entity);

    private static string Envelope(string op, long tsMs, string afterJson) =>
        "{\"payload\":{\"op\":\"" + op + "\",\"source\":{\"ts_ms\":" + tsMs + "},\"after\":" + afterJson + "}}";

    [Fact]
    public void Every_catalog_spec_builds_a_valid_schema_with_envelope_fields()
    {
        CurationCatalog.All.Should().NotBeEmpty();

        foreach (var spec in CurationCatalog.All)
        {
            var schema = spec.Schema;   // throws if the generated JSON is not valid Avro
            schema.Name.Should().Be(spec.RecordName);

            var fieldNames = schema.Fields.Select(f => f.Name).ToList();
            fieldNames.Should().Contain(["operation", "sourceTsMs", "curatedAtUtc"]);
            foreach (var field in spec.Fields)
            {
                fieldNames.Should().Contain(field.Name);
            }

            // The key field must be one of the projected fields.
            fieldNames.Should().Contain(spec.KeyField);
        }
    }

    [Fact]
    public void Curated_topics_and_raw_topics_are_unique_and_paired()
    {
        CurationCatalog.All.Select(s => s.RawTopic).Should().OnlyHaveUniqueItems();
        CurationCatalog.All.Select(s => s.CuratedTopic).Should().OnlyHaveUniqueItems();
        CurationCatalog.RawTopics.Should().HaveCount(CurationCatalog.All.Count);
    }

    [Fact]
    public void Projects_a_customer_change_to_the_curated_record_and_key()
    {
        var spec = Spec("customers");
        var raw = Envelope("c", 1_700_000_000_000, """
            {"CustomerId":42,"CustomerCode":"SEED-C001","DisplayName":"Ada Lovelace",
             "Email":"ada@example.com","PreferredLocale":"en-US","Status":1,"LifetimeValueUsd":"318.18"}
            """);

        var change = DebeziumChange.Parse(raw);
        change.Operation.Should().Be("insert");
        change.HasAfter.Should().BeTrue();

        var (record, key) = CuratedRecordProjector.Project(spec, change);

        key.Should().Be("SEED-C001");
        record["customerId"].Should().Be(42L);
        record["customerCode"].Should().Be("SEED-C001");
        record["preferredLocale"].Should().Be("en-US");
        record["status"].Should().Be(1);
        record["lifetimeValueUsd"].Should().Be("318.18");   // decimals carried as strings
        record["operation"].Should().Be("insert");
        record["sourceTsMs"].Should().Be(1_700_000_000_000L);
    }

    [Fact]
    public void Snapshot_reads_map_to_the_snapshot_operation()
    {
        var change = DebeziumChange.Parse(Envelope("r", 1, """{"CustomerId":1,"CustomerCode":"X","DisplayName":"n","Email":"e","PreferredLocale":"en","Status":1,"LifetimeValueUsd":"0"}"""));
        change.Operation.Should().Be("snapshot");
    }

    [Fact]
    public void Projects_nullable_field_as_null()
    {
        var spec = Spec("product-categories");
        var raw = Envelope("c", 1, """
            {"CategoryId":10,"ParentId":null,"Name":"Electronics","Slug":"electronics","DisplayOrder":1}
            """);

        var (record, _) = CuratedRecordProjector.Project(spec, DebeziumChange.Parse(raw));

        record["parentId"].Should().BeNull();
        record["categoryId"].Should().Be(10);
    }

    [Fact]
    public void Projects_timestamp_millis_as_long()
    {
        var spec = Spec("orders");
        var raw = Envelope("c", 5, """
            {"OrderId":1,"OrderNumber":"SEED-ORD-0001","CustomerId":42,"BillingAddressId":7,
             "ShippingAddressId":7,"PlacedAtUtc":1719830400000,"Status":4,"SubtotalUsd":"289.98",
             "TaxUsd":"23.20","ShippingUsd":"5.00","TotalUsd":"318.18","Currency":"USD"}
            """);

        var (record, key) = CuratedRecordProjector.Project(spec, DebeziumChange.Parse(raw));

        key.Should().Be("SEED-ORD-0001");
        record["placedAtUtc"].Should().Be(1719830400000L);
        record["totalUsd"].Should().Be("318.18");
    }

    [Fact]
    public void Missing_non_nullable_source_column_throws()
    {
        var spec = Spec("warehouses");
        var raw = Envelope("c", 1, """{"WarehouseId":1,"Code":"WH-EAST"}""");   // missing Name/Region/...

        var act = () => CuratedRecordProjector.Project(spec, DebeziumChange.Parse(raw));

        act.Should().Throw<InvalidOperationException>().WithMessage("*not nullable*");
    }

    [Fact]
    public void Delete_without_after_is_flagged_as_no_after()
    {
        var change = DebeziumChange.Parse("""{"payload":{"op":"d","source":{"ts_ms":1},"after":null,"before":{"CustomerCode":"X"}}}""");
        change.Operation.Should().Be("delete");
        change.HasAfter.Should().BeFalse();
    }
}
