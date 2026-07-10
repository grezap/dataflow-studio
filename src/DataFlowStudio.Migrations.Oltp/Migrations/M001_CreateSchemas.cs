using FluentMigrator;

namespace DataFlowStudio.Migrations.Oltp.Migrations;

// Creates the non-default schemas. dbo already exists; audit holds the ChangeLog table.
[Migration(20260711001L)]
public sealed class M001_CreateSchemas : Migration
{
    public override void Up() => Execute.Sql(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
            EXEC('CREATE SCHEMA audit');
        """);

    public override void Down() => Execute.Sql(
        """
        IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
            EXEC('DROP SCHEMA audit');
        """);
}
