using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.Transactions — payment events for an order; FK -> Orders.
[Migration(20260711010L)]
public sealed class M010_Transactions : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Transactions (
            TransactionId   BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            OrderId         BIGINT NOT NULL REFERENCES dbo.Orders(OrderId),
            Provider        VARCHAR(32) NOT NULL,     -- stripe, paypal, ach
            ProviderRef     VARCHAR(64) NOT NULL,
            Kind            TINYINT NOT NULL,         -- 1 auth, 2 capture, 3 refund, 4 chargeback
            AmountUsd       DECIMAL(18,2) NOT NULL,
            OccurredAtUtc   DATETIME2(3) NOT NULL,
            Status          TINYINT NOT NULL,         -- 1 pending, 2 settled, 3 failed
            created_utc     DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by      NVARCHAR(100) NOT NULL,
            modified_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by     NVARCHAR(100) NOT NULL,
            row_version     ROWVERSION NOT NULL,
            is_deleted      BIT NOT NULL DEFAULT 0
        );
        """);

    public override void Down() => Execute.Sql("DROP TABLE dbo.Transactions;");
}
