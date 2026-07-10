using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.OrderLines — FKs -> Orders, Products, Warehouses; PERSISTED computed LineTotalUsd.
[Migration(20260711009L)]
public sealed class M009_OrderLines : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.OrderLines (
            OrderLineId   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            OrderId       BIGINT NOT NULL REFERENCES dbo.Orders(OrderId),
            ProductId     BIGINT NOT NULL REFERENCES dbo.Products(ProductId),
            WarehouseId   INT    NOT NULL REFERENCES dbo.Warehouses(WarehouseId),
            Quantity      INT NOT NULL,
            UnitPriceUsd  DECIMAL(18,4) NOT NULL,
            DiscountUsd   DECIMAL(18,4) NOT NULL DEFAULT 0,
            LineTotalUsd  AS (CAST(Quantity * UnitPriceUsd - DiscountUsd AS DECIMAL(18,2))) PERSISTED,
            created_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by    NVARCHAR(100) NOT NULL,
            modified_utc  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by   NVARCHAR(100) NOT NULL,
            row_version   ROWVERSION NOT NULL,
            is_deleted    BIT NOT NULL DEFAULT 0
        );
        """);

    public override void Down() => Execute.Sql("DROP TABLE dbo.OrderLines;");
}
