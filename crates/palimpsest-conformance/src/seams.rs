//! Verification seams for accelerated temporal conformance (spec 018).
//!
//! These seams operate on scratch databases that the test suite owns. Each
//! rewind disables one guard trigger for exactly one statement and then
//! re-enables the trigger.

use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

/// Runs one SQL statement while a guard trigger is disabled.
///
/// The trigger is re-enabled before this function reports the statement
/// result. The table name and the trigger name are fixed vocabulary from the
/// checked-in migrations. The caller owns the statement text. These strings
/// are audited safe because they never contain request content.
pub async fn rewind_under_disabled_trigger(
    pool: &PgPool,
    table: &str,
    trigger: &str,
    statement: &str,
) -> Result<u64> {
    let disable = format!("ALTER TABLE {table} DISABLE TRIGGER {trigger}");
    let enable = format!("ALTER TABLE {table} ENABLE TRIGGER {trigger}");
    sqlx::query(sqlx::AssertSqlSafe(disable))
        .execute(pool)
        .await
        .with_context(|| format!("disable trigger {trigger}"))?;
    let outcome = sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await;
    sqlx::query(sqlx::AssertSqlSafe(enable))
        .execute(pool)
        .await
        .with_context(|| format!("enable trigger {trigger}"))?;
    let rows = outcome
        .with_context(|| format!("rewind statement under trigger {trigger}"))?
        .rows_affected();
    Ok(rows)
}

/// Rewinds a stored expiry column into the past by one second while a guard
/// trigger is disabled, and proves that exactly one deadline moved.
///
/// The one second margin satisfies the spec 018 safety margin of at least
/// 100 milliseconds. The expiry column and the where clause are fixed
/// vocabulary from the checked-in migrations and the caller-owned fixture
/// identifiers; they never contain request content.
pub async fn rewind_expiry_under_disabled_trigger(
    pool: &PgPool,
    table: &str,
    trigger: &str,
    expiry_column: &str,
    where_clause: &str,
    context_message: &str,
) -> Result<()> {
    let statement = format!(
        "UPDATE {table} SET {expiry_column} = clock_timestamp() - interval '1 second' \
         WHERE {where_clause}"
    );
    let rewound = rewind_under_disabled_trigger(pool, table, trigger, &statement)
        .await
        .with_context(|| format!("{context_message}: rewind the expiry deadline"))?;
    ensure!(
        rewound >= 1,
        "{context_message}: the expiry rewind missed its target row"
    );
    Ok(())
}
