using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.Customers — system-versioned temporal table with the standard six audit columns (E6).
[Migration(20260711002L)]
public sealed class M002_Customers : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Customers (
            CustomerId       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            CustomerCode     VARCHAR(32) NOT NULL UNIQUE,
            DisplayName      NVARCHAR(200) NOT NULL,
            Email            NVARCHAR(256) NOT NULL,
            PhoneE164        VARCHAR(20) NULL,
            PreferredLocale  VARCHAR(10) NOT NULL DEFAULT 'en-US',
            Status           TINYINT NOT NULL DEFAULT 1,
            LifetimeValueUsd DECIMAL(18,2) NOT NULL DEFAULT 0,
            created_utc      DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by       NVARCHAR(100) NOT NULL,
            modified_utc     DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by      NVARCHAR(100) NOT NULL,
            row_version      ROWVERSION NOT NULL,
            is_deleted       BIT NOT NULL DEFAULT 0,
            ValidFrom        DATETIME2(3) GENERATED ALWAYS AS ROW START NOT NULL,
            ValidTo          DATETIME2(3) GENERATED ALWAYS AS ROW END   NOT NULL,
            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
        ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.CustomersHistory));
        """);

    public override void Down() => Execute.Sql(
        """
        ALTER TABLE dbo.Customers SET (SYSTEM_VERSIONING = OFF);
        DROP TABLE dbo.Customers;
        DROP TABLE dbo.CustomersHistory;
        """);
}
