using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary>
/// <c>audit.ChangeLog</c> — an application-level change journal capturing before/after JSON per row
/// change. Complements SQL Server CDC with a human-readable, queryable audit trail.
/// </summary>
[Migration(20260711012L)]
public sealed class M012_AuditChangeLog : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        CREATE TABLE audit.ChangeLog (
            ChangeId     BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            TableName    SYSNAME NOT NULL,
            PrimaryKey   NVARCHAR(256) NOT NULL,
            Operation    CHAR(1) NOT NULL,    -- I / U / D
            BeforeJson   NVARCHAR(MAX) NULL,
            AfterJson    NVARCHAR(MAX) NULL,
            ChangedBy    NVARCHAR(100) NOT NULL,
            ChangedUtc   DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
        );
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql("DROP TABLE audit.ChangeLog;");
}
