using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary><c>dbo.Warehouses</c> — reference table for inventory and order fulfilment.</summary>
[Migration(20260711006L)]
public sealed class M006_Warehouses : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.Warehouses (
            WarehouseId   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            Code          VARCHAR(16) NOT NULL UNIQUE,
            Name          NVARCHAR(200) NOT NULL,
            Region        NVARCHAR(100) NOT NULL,
            CountryIso2   CHAR(2) NOT NULL,
            TimezoneIana  VARCHAR(64) NOT NULL,
            created_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by    NVARCHAR(100) NOT NULL,
            modified_utc  DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by   NVARCHAR(100) NOT NULL,
            row_version   ROWVERSION NOT NULL,
            is_deleted    BIT NOT NULL DEFAULT 0
        );
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql("DROP TABLE dbo.Warehouses;");
}
