using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

/// <summary>
/// Creates the non-default schemas. <c>dbo</c> already exists; <c>audit</c> holds the ChangeLog
/// table. Runs first so later migrations can place objects into <c>audit</c>.
/// </summary>
[Migration(20260711001L)]
public sealed class M001_CreateSchemas : Migration
{
    /// <inheritdoc />
    public override void Up() => Execute.Sql(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
            EXEC('CREATE SCHEMA audit');
        """);

    /// <inheritdoc />
    public override void Down() => Execute.Sql(
        """
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
            EXEC('DROP SCHEMA audit');
        """);
}
