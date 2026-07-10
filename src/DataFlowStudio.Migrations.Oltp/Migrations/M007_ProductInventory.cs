using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.ProductInventory — composite PK (ProductId, WarehouseId); FKs -> Products, Warehouses.
[Migration(20260711007L)]
public sealed class M007_ProductInventory : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.ProductInventory (
            ProductId      BIGINT NOT NULL REFERENCES dbo.Products(ProductId),
            WarehouseId    INT    NOT NULL REFERENCES dbo.Warehouses(WarehouseId),
            OnHand         INT NOT NULL DEFAULT 0,
            Reserved       INT NOT NULL DEFAULT 0,
            ReorderPoint   INT NOT NULL DEFAULT 0,
            SafetyStock    INT NOT NULL DEFAULT 0,
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0,
            PRIMARY KEY (ProductId, WarehouseId)
        );
        """);

    public override void Down() => Execute.Sql("DROP TABLE dbo.ProductInventory;");
}
