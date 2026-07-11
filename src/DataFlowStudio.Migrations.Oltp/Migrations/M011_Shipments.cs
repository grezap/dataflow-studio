using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary><c>dbo.Shipments</c> — fulfilment/tracking records for an order; FK → <c>Orders</c>.</summary>
[Migration(20260711011L)]
public sealed class M011_Shipments : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Shipments (
            ShipmentId     BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            OrderId        BIGINT NOT NULL REFERENCES dbo.Orders(OrderId),
            Carrier        VARCHAR(32) NOT NULL,
            TrackingNumber VARCHAR(64) NOT NULL,
            ShippedAtUtc   DATETIME2(3) NULL,
            DeliveredAtUtc DATETIME2(3) NULL,
            Status         TINYINT NOT NULL,
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0
        );
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql("DROP TABLE dbo.Shipments;");
}
