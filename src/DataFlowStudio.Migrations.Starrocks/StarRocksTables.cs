namespace DataFlowStudio.Migrations.Starrocks;

/// <summary>
/// The canonical list of tables the StarRocks migration scripts create, used by the idempotency
/// test to assert the full <c>dwh</c> star schema is present after <c>MigrateUp</c>. Mirrors the
/// authored DDL in <c>schemas/dataflow-studio/README.md</c> (with the StarRocks-validity fixes
/// recorded in ADR-0005).
/// </summary>
public static class StarRocksTables
{
    /// <summary>The five Kimball dimensions (database, name).</summary>
    public static readonly IReadOnlyList<(string Database, string Name)> Dimensions =
    [
        ("dwh", "dim_date"),
        ("dwh", "dim_customer"),
        ("dwh", "dim_product"),
        ("dwh", "dim_warehouse"),
        ("dwh", "dim_carrier"),
    ];

    /// <summary>The four fact tables plus the customer-segment bridge.</summary>
    public static readonly IReadOnlyList<(string Database, string Name)> Facts =
    [
        ("dwh", "fact_order"),
        ("dwh", "fact_order_line"),
        ("dwh", "fact_transaction"),
        ("dwh", "fact_inventory_snap"),
        ("dwh", "bridge_customer_seg"),
    ];

    /// <summary>Every table the scripts create, across both owned databases.</summary>
    public static readonly IReadOnlyList<(string Database, string Name)> All =
        [.. Dimensions, .. Facts];
}
