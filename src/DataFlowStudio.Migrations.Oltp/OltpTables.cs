namespace DataFlowStudio.Migrations.Oltp;

/// <summary>
/// The canonical list of the 11 OltpDb business tables authored in
/// <c>schemas/dataflow-studio/README.md</c>, plus the two temporal history tables that
/// <c>SYSTEM_VERSIONING = ON</c> auto-creates. Used by the E1 migration test to assert the
/// schema is fully present after each <c>MigrateUp</c>.
/// </summary>
public static class OltpTables
{
    /// <summary>The 11 business tables (schema, name), in dependency order.</summary>
    public static readonly IReadOnlyList<(string Schema, string Name)> Business =
    [
        ("dbo", "Customers"),
        ("dbo", "CustomerAddresses"),
        ("dbo", "ProductCategories"),
        ("dbo", "Products"),
        ("dbo", "Warehouses"),
        ("dbo", "ProductInventory"),
        ("dbo", "Orders"),
        ("dbo", "OrderLines"),
        ("dbo", "Transactions"),
        ("dbo", "Shipments"),
        ("audit", "ChangeLog"),
    ];

    /// <summary>History tables created implicitly by the two system-versioned temporal tables.</summary>
    public static readonly IReadOnlyList<(string Schema, string Name)> TemporalHistory =
    [
        ("dbo", "CustomersHistory"),
        ("dbo", "ProductsHistory"),
    ];
}
