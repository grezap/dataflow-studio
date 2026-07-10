using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.Products — system-versioned temporal table; FK -> dbo.ProductCategories.
[Migration(20260711005L)]
public sealed class M005_Products : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Products (
            ProductId      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            Sku            VARCHAR(64) NOT NULL UNIQUE,
            CategoryId     INT NOT NULL REFERENCES dbo.ProductCategories(CategoryId),
            DisplayName    NVARCHAR(300) NOT NULL,
            Description    NVARCHAR(MAX) NULL,
            ListPriceUsd   DECIMAL(18,4) NOT NULL,
            Weight_g       INT NULL,
            Status         TINYINT NOT NULL DEFAULT 1,
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0,
            ValidFrom      DATETIME2(3) GENERATED ALWAYS AS ROW START NOT NULL,
            ValidTo        DATETIME2(3) GENERATED ALWAYS AS ROW END   NOT NULL,
            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.ProductsHistory));
        """);

    public override void Down() => Execute.Sql(
        """
        ALTER TABLE dbo.Products SET (SYSTEM_VERSIONING = OFF);
        DROP TABLE dbo.Products;
        DROP TABLE dbo.ProductsHistory;
        """);
}
