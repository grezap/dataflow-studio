using DataFlowStudio.Modules.Ingestion.Curation;
using Shouldly;
using Xunit;

namespace DataFlowStudio.UnitTests;

/// <summary>
/// Branch-level coverage of the pure curation core: the projector's per-field-kind conversion matrix
/// (including the tolerance branches for raw-number decimals, int-as-boolean, and string timestamps),
/// nullable handling, and the Debezium envelope parser's operation mapping + root/payload shapes.
/// </summary>
public sealed class CurationBranchTests
{
    private static EntityCurationSpec Spec(string keyField = "id") =>
        new(
            "widgets", "oltp.OltpDb.dbo.Widgets", "dfs.widgets.changed.v1", "WidgetChanged", keyField,
            [
                new CuratedField("id", "Id", CuratedFieldKind.Bigint),
                new CuratedField("count", "Count", CuratedFieldKind.Integer),
                new CuratedField("name", "Name", CuratedFieldKind.Text, Nullable: true),
                new CuratedField("price", "Price", CuratedFieldKind.DecimalString),
                new CuratedField("active", "Active", CuratedFieldKind.Boolean),
                new CuratedField("ts", "Ts", CuratedFieldKind.TimestampMillis),
            ]);

    private static string Envelope(string op, long tsMs, string afterJson) =>
        "{\"payload\":{\"op\":\"" + op + "\",\"source\":{\"ts_ms\":" + tsMs + "},\"after\":" + afterJson + "}}";

    [Fact]
    public void Projects_each_field_kind_from_native_json_types()
    {
        var change = DebeziumChange.Parse(Envelope("c", 5, """
            {"Id":42,"Count":7,"Name":"Ada","Price":"9.99","Active":true,"Ts":1719830400000}
            """));

        var (record, key) = CuratedRecordProjector.Project(Spec(), change);

        key.ShouldBe("42");
        record["id"].ShouldBe(42L);
        record["count"].ShouldBe(7);
        record["name"].ShouldBe("Ada");
        record["price"].ShouldBe("9.99");
        record["active"].ShouldBe(true);
        record["ts"].ShouldBe(1719830400000L);
        record["operation"].ShouldBe("insert");
    }

    [Fact]
    public void Projects_the_tolerance_branches_raw_decimal_int_boolean_string_timestamp_and_null()
    {
        // Name omitted → nullable → null; Price a raw number; Active as int 1; Ts as a numeric string.
        var change = DebeziumChange.Parse(Envelope("u", 9, """
            {"Id":1,"Count":2,"Price":100,"Active":1,"Ts":"1719830400000"}
            """));

        var (record, _) = CuratedRecordProjector.Project(Spec(), change);

        record["name"].ShouldBeNull();
        record["price"].ShouldBe("100");        // raw number tolerated via GetRawText
        record["active"].ShouldBe(true);        // int 1 → true
        record["ts"].ShouldBe(1719830400000L);  // numeric string → parsed long
        record["operation"].ShouldBe("update");
    }

    [Fact]
    public void Projects_boolean_false_from_zero()
    {
        var change = DebeziumChange.Parse(Envelope("c", 1, """
            {"Id":1,"Count":0,"Price":"0","Active":0,"Ts":0}
            """));

        var (record, _) = CuratedRecordProjector.Project(Spec(), change);
        record["active"].ShouldBe(false);
    }

    [Fact]
    public void Key_is_empty_when_the_key_field_is_absent()
    {
        // Key field points at the nullable 'name', which is omitted → the message key is empty.
        var change = DebeziumChange.Parse(Envelope("c", 1, """
            {"Id":1,"Count":0,"Price":"0","Active":false,"Ts":0}
            """));

        var (_, key) = CuratedRecordProjector.Project(Spec(keyField: "name"), change);
        key.ShouldBe(string.Empty);
    }

    [Fact]
    public void Projecting_a_change_with_no_after_throws()
    {
        var change = DebeziumChange.Parse("""{"payload":{"op":"d","source":{"ts_ms":1},"after":null}}""");
        Should.Throw<InvalidOperationException>(() => CuratedRecordProjector.Project(Spec(), change))
              .Message.ShouldContain("no 'after'");
    }

    [Theory]
    [InlineData("r", "snapshot")]
    [InlineData("c", "insert")]
    [InlineData("u", "update")]
    [InlineData("d", "delete")]
    [InlineData("x", "x")]   // unknown op passes through verbatim
    public void Parses_every_operation_code(string op, string expected)
    {
        DebeziumChange.Parse(Envelope(op, 1, """{"Id":1}""")).Operation.ShouldBe(expected);
    }

    [Fact]
    public void Parses_a_root_level_payload_without_the_payload_wrapper()
    {
        // schemas.enable=false can place the fields at the root (no "payload" envelope).
        var change = DebeziumChange.Parse("""{"op":"c","source":{"ts_ms":42},"after":{"Id":1}}""");
        change.Operation.ShouldBe("insert");
        change.SourceTsMs.ShouldBe(42);
        change.HasAfter.ShouldBeTrue();
    }

    [Fact]
    public void Missing_op_and_source_default_gracefully()
    {
        var change = DebeziumChange.Parse("""{"after":{"Id":1}}""");
        change.Operation.ShouldBe("?");
        change.SourceTsMs.ShouldBe(0);
    }
}
