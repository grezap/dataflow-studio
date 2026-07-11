using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary>
/// <c>dbo.Customers</c> — a system-versioned temporal table with the standard six audit columns
/// (E6). SQL Server auto-creates the <c>CustomersHistory</c> table to retain every prior version.
/// </summary>
[Migration(20260711002L)]
public sealed class M002_Customers : Migration
{
    /// <inheritdoc />
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

    /// <inheritdoc />
    public override void Down() => Execute.Sql(
        // System-versioning must be turned OFF before either table can be dropped, else SQL Server
        // rejects the DROP. This is exactly the case the E1 up->down->up gate exercises.
        """
        ALTER TABLE dbo.Customers SET (SYSTEM_VERSIONING = OFF);
        DROP TABLE dbo.Customers;
        DROP TABLE dbo.CustomersHistory;
        """);
}
