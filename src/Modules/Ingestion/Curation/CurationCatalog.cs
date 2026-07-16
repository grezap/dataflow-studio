using K = DataFlowStudio.Modules.Ingestion.Curation.CuratedFieldKind;

namespace DataFlowStudio.Modules.Ingestion.Curation;

/// <summary>
/// The full set of order-flow entities the curation worker reshapes from Debezium raw CDC into
/// curated Avro. Each <see cref="EntityCurationSpec"/> maps an OltpDb table's raw topic to a curated
/// <c>dfs.&lt;entity&gt;.changed.v1</c> topic, projecting only the fields the downstream StarRocks
/// dimensions/facts need. Adding an entity is a new list entry — no new worker code (ADR-0007).
/// </summary>
public static class CurationCatalog
{
    private const string RawPrefix = "oltp.OltpDb.dbo.";

    /// <summary>Every curated entity spec, in dependency order (dimensions before facts).</summary>
    public static readonly IReadOnlyList<EntityCurationSpec> All =
    [
        // dim_customer (SCD2)
        new(
            entity: "customers",
            rawTopic: RawPrefix + "Customers",
            curatedTopic: "dfs.customers.changed.v1",
            recordName: "CustomerChanged",
            keyField: "customerCode",
            fields:
            [
                new("customerId", "CustomerId", K.Bigint),
                new("customerCode", "CustomerCode", K.Text),
                new("displayName", "DisplayName", K.Text),
                new("email", "Email", K.Text),
                new("preferredLocale", "PreferredLocale", K.Text),
                new("status", "Status", K.Integer),
                new("lifetimeValueUsd", "LifetimeValueUsd", K.DecimalString),
            ]),

        // dim_product category names (joined at load)
        new(
            entity: "product-categories",
            rawTopic: RawPrefix + "ProductCategories",
            curatedTopic: "dfs.product-categories.changed.v1",
            recordName: "ProductCategoryChanged",
            keyField: "slug",
            fields:
            [
                new("categoryId", "CategoryId", K.Integer),
                new("parentId", "ParentId", K.Integer, Nullable: true),
                new("name", "Name", K.Text),
                new("slug", "Slug", K.Text),
                new("displayOrder", "DisplayOrder", K.Integer),
            ]),

        // dim_product (SCD2)
        new(
            entity: "products",
            rawTopic: RawPrefix + "Products",
            curatedTopic: "dfs.products.changed.v1",
            recordName: "ProductChanged",
            keyField: "sku",
            fields:
            [
                new("productId", "ProductId", K.Bigint),
                new("sku", "Sku", K.Text),
                new("categoryId", "CategoryId", K.Integer),
                new("displayName", "DisplayName", K.Text),
                new("listPriceUsd", "ListPriceUsd", K.DecimalString),
                new("status", "Status", K.Integer),
            ]),

        // dim_warehouse
        new(
            entity: "warehouses",
            rawTopic: RawPrefix + "Warehouses",
            curatedTopic: "dfs.warehouses.changed.v1",
            recordName: "WarehouseChanged",
            keyField: "code",
            fields:
            [
                new("warehouseId", "WarehouseId", K.Integer),
                new("code", "Code", K.Text),
                new("name", "Name", K.Text),
                new("region", "Region", K.Text),
                new("countryIso2", "CountryIso2", K.Text),
                new("timezoneIana", "TimezoneIana", K.Text),
            ]),

        // fact_order billing/shipping address enrichment
        new(
            entity: "customer-addresses",
            rawTopic: RawPrefix + "CustomerAddresses",
            curatedTopic: "dfs.customer-addresses.changed.v1",
            recordName: "CustomerAddressChanged",
            keyField: "addressId",
            fields:
            [
                new("addressId", "AddressId", K.Bigint),
                new("customerId", "CustomerId", K.Bigint),
                new("addressType", "AddressType", K.Integer),
                new("city", "City", K.Text),
                new("region", "Region", K.Text, Nullable: true),
                new("postalCode", "PostalCode", K.Text),
                new("countryIso2", "CountryIso2", K.Text),
                new("isDefault", "IsDefault", K.Boolean),
            ]),

        // fact_order
        new(
            entity: "orders",
            rawTopic: RawPrefix + "Orders",
            curatedTopic: "dfs.orders.changed.v1",
            recordName: "OrderChanged",
            keyField: "orderNumber",
            fields:
            [
                new("orderId", "OrderId", K.Bigint),
                new("orderNumber", "OrderNumber", K.Text),
                new("customerId", "CustomerId", K.Bigint),
                new("billingAddressId", "BillingAddressId", K.Bigint),
                new("shippingAddressId", "ShippingAddressId", K.Bigint),
                new("placedAtUtc", "PlacedAtUtc", K.TimestampMillis),
                new("status", "Status", K.Integer),
                new("subtotalUsd", "SubtotalUsd", K.DecimalString),
                new("taxUsd", "TaxUsd", K.DecimalString),
                new("shippingUsd", "ShippingUsd", K.DecimalString),
                new("totalUsd", "TotalUsd", K.DecimalString),
                new("currency", "Currency", K.Text),
            ]),

        // fact_order_line
        new(
            entity: "order-lines",
            rawTopic: RawPrefix + "OrderLines",
            curatedTopic: "dfs.order-lines.changed.v1",
            recordName: "OrderLineChanged",
            keyField: "orderLineId",
            fields:
            [
                new("orderLineId", "OrderLineId", K.Bigint),
                new("orderId", "OrderId", K.Bigint),
                new("productId", "ProductId", K.Bigint),
                new("warehouseId", "WarehouseId", K.Integer),
                new("quantity", "Quantity", K.Integer),
                new("unitPriceUsd", "UnitPriceUsd", K.DecimalString),
                new("discountUsd", "DiscountUsd", K.DecimalString),
                // lineTotalUsd is a computed PERSISTED column — SQL Server CDC stores computed
                // columns as NULL in the change table, so it is not carried; the DWH loader
                // recomputes line_total_usd = quantity * unit_price - discount.
            ]),

        // fact_transaction
        new(
            entity: "transactions",
            rawTopic: RawPrefix + "Transactions",
            curatedTopic: "dfs.transactions.changed.v1",
            recordName: "TransactionChanged",
            keyField: "transactionId",
            fields:
            [
                new("transactionId", "TransactionId", K.Bigint),
                new("orderId", "OrderId", K.Bigint),
                new("provider", "Provider", K.Text),
                new("providerRef", "ProviderRef", K.Text),
                new("kind", "Kind", K.Integer),
                new("amountUsd", "AmountUsd", K.DecimalString),
                new("occurredAtUtc", "OccurredAtUtc", K.TimestampMillis),
                new("status", "Status", K.Integer),
            ]),

        // dim_carrier + shipment facts
        new(
            entity: "shipments",
            rawTopic: RawPrefix + "Shipments",
            curatedTopic: "dfs.shipments.changed.v1",
            recordName: "ShipmentChanged",
            keyField: "trackingNumber",
            fields:
            [
                new("shipmentId", "ShipmentId", K.Bigint),
                new("orderId", "OrderId", K.Bigint),
                new("carrier", "Carrier", K.Text),
                new("trackingNumber", "TrackingNumber", K.Text),
                new("shippedAtUtc", "ShippedAtUtc", K.TimestampMillis, Nullable: true),
                new("deliveredAtUtc", "DeliveredAtUtc", K.TimestampMillis, Nullable: true),
                new("status", "Status", K.Integer),
            ]),

        // fact_inventory_snap
        new(
            entity: "product-inventory",
            rawTopic: RawPrefix + "ProductInventory",
            curatedTopic: "dfs.product-inventory.changed.v1",
            recordName: "ProductInventoryChanged",
            keyField: "productId",
            fields:
            [
                new("productId", "ProductId", K.Bigint),
                new("warehouseId", "WarehouseId", K.Integer),
                new("onHand", "OnHand", K.Integer),
                new("reserved", "Reserved", K.Integer),
                new("reorderPoint", "ReorderPoint", K.Integer),
                new("safetyStock", "SafetyStock", K.Integer),
            ]),
    ];

    /// <summary>The raw Debezium topics the worker subscribes to (one per entity).</summary>
    public static IReadOnlyList<string> RawTopics => [.. All.Select(s => s.RawTopic)];

    /// <summary>Looks up the spec for a raw topic, or null if the topic is not in the catalog.</summary>
    public static EntityCurationSpec? ForRawTopic(string rawTopic) =>
        All.FirstOrDefault(s => s.RawTopic == rawTopic);
}
