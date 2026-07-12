using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.MySql;

namespace DataFlowStudio.Migrations.Starrocks;

/// <summary>
/// A DbUp journal for StarRocks. StarRocks speaks the MySQL wire protocol, so
/// <see cref="MySqlTableJournal"/>'s <c>information_schema</c> lookups and INSERT/SELECT journal
/// SQL work as-is — but its <c>CREATE TABLE</c> DDL does not: DbUp's default journal table uses an
/// <c>INT AUTO_INCREMENT</c> surrogate key, and StarRocks only supports <c>AUTO_INCREMENT</c> on
/// PRIMARY KEY tables and always requires an explicit key model + <c>DISTRIBUTED BY</c> clause.
/// This subclass overrides only the table-creation SQL to emit a StarRocks PRIMARY KEY table keyed
/// on the script name, replicated across the three backends.
/// </summary>
public sealed class StarRocksTableJournal : MySqlTableJournal
{
    private readonly int _replicationNum;

    /// <summary>
    /// Creates the journal bound to the given DbUp connection manager and logger, tracking applied
    /// scripts in <paramref name="schema"/>.<paramref name="table"/> (e.g. <c>dwh.schemaversions</c>).
    /// </summary>
    /// <param name="connectionManager">Yields the DbUp connection manager for the target StarRocks FE.</param>
    /// <param name="logger">Yields the DbUp log sink.</param>
    /// <param name="schema">The StarRocks database that holds the journal table (e.g. <c>dwh</c>).</param>
    /// <param name="table">The journal table name (e.g. <c>schemaversions</c>).</param>
    /// <param name="replicationNum">
    /// The StarRocks replica count for the journal table — 3 for the three-backend lab, 1 for a
    /// single-backend test container (a table cannot request more replicas than there are backends).
    /// </param>
    public StarRocksTableJournal(
        Func<IConnectionManager> connectionManager,
        Func<IUpgradeLog> logger,
        string schema,
        string table,
        int replicationNum)
        : base(connectionManager, logger, schema, table)
    {
        _replicationNum = replicationNum;
    }

    /// <summary>
    /// Emits the StarRocks-flavoured journal DDL: a PRIMARY KEY table on <c>scriptname</c> (the same
    /// column names the inherited INSERT/SELECT journal SQL expects), hash-distributed and replicated
    /// to match the backend count. The <paramref name="quotedPrimaryKeyName"/> the base class computes
    /// is unused — StarRocks declares the key via the <c>PRIMARY KEY(...)</c> model clause, not a
    /// named table constraint.
    /// </summary>
    protected override string CreateSchemaTableSql(string quotedPrimaryKeyName) =>
        $"""
        CREATE TABLE {FqSchemaTableName} (
            `scriptname` VARCHAR(255) NOT NULL,
            `applied`    DATETIME     NOT NULL
        )
        PRIMARY KEY(`scriptname`)
        DISTRIBUTED BY HASH(`scriptname`) BUCKETS 1
        PROPERTIES ("replication_num" = "{_replicationNum}")
        """;
}
