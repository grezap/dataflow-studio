using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// dbo.CustomerAddresses — FK -> dbo.Customers.
[Migration(20260711003L)]
public sealed class M003_CustomerAddresses : Migration
{
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.CustomerAddresses (
            AddressId      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            CustomerId     BIGINT NOT NULL REFERENCES dbo.Customers(CustomerId),
            AddressType    TINYINT NOT NULL,    -- 1=billing 2=shipping
            Line1          NVARCHAR(200) NOT NULL,
            Line2          NVARCHAR(200) NULL,
            City           NVARCHAR(100) NOT NULL,
            Region         NVARCHAR(100) NULL,
            PostalCode     VARCHAR(20) NOT NULL,
            CountryIso2    CHAR(2) NOT NULL,
            IsDefault      BIT NOT NULL DEFAULT 0,
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0
        );
        """);

    public override void Down() => Execute.Sql("DROP TABLE dbo.CustomerAddresses;");
}
