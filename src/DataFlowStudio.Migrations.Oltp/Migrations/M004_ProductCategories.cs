using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary><c>dbo.ProductCategories</c> — self-referencing category hierarchy (ParentId → CategoryId).</summary>
[Migration(20260711004L)]
public sealed class M004_ProductCategories : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE dbo.ProductCategories (
            CategoryId     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            ParentId       INT NULL REFERENCES dbo.ProductCategories(CategoryId),
            Name           NVARCHAR(200) NOT NULL,
            Slug           VARCHAR(200) NOT NULL UNIQUE,
            DisplayOrder   INT NOT NULL DEFAULT 0,
            created_utc    DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            created_by     NVARCHAR(100) NOT NULL,
            modified_utc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
            modified_by    NVARCHAR(100) NOT NULL,
            row_version    ROWVERSION NOT NULL,
            is_deleted     BIT NOT NULL DEFAULT 0
        );
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql("DROP TABLE dbo.ProductCategories;");
}
