using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary>
/// <c>dbo.Orders</c> — order headers. FKs → <c>dbo.Customers</c> and (twice) to
/// <c>dbo.CustomerAddresses</c> for billing + shipping.
/// </summary>
[Migration(20260711008L)]
public sealed class M008_Orders : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Orders (
            OrderId        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            OrderNumber    VARCHAR(32) NOT NULL UNIQUE,
            CustomerId     BIGINT NOT NULL REFERENCES dbo.Customers(CustomerId),
            BillingAddressId  BIGINT NOT NULL REFERENCES dbo.CustomerAddresses(AddressId),
            ShippingAddressId BIGINT NOT NULL REFERENCES dbo.CustomerAddresses(AddressId),
            PlacedAtUtc    DATETIME2(3) NOT NULL,
            Status         TINYINT NOT NULL,     -- 1 new, 2 paid, 3 shipped, 4 delivered, 5 cancelled
            SubtotalUsd    DECIMAL(18,2) NOT NULL,
            TaxUsd         DECIMAL(18,2) NOT NULL,
            ShippingUsd    DECIMAL(18,2) NOT NULL,
            TotalUsd       DECIMAL(18,2) NOT NULL,
            Currency       CHAR(3) NOT NULL DEFAULT 'USD',
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0
        );
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql("DROP TABLE dbo.Orders;");
}
