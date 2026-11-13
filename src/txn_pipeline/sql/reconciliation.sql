-- Reconciliation checks. Each query returns the NUMBER OF VIOLATIONS; 0 = pass.
-- Run by `python -m txn_pipeline check`, or paste any query into the DuckDB CLI.
-- Format: "-- name: <check_name> | <description>" followed by one query ending in ";".

-- name: unbalanced_curated_transactions | every curated transaction has sum(debits) = sum(credits)
SELECT COUNT(*) FROM (
    SELECT transaction_id
    FROM curated_entries
    GROUP BY transaction_id
    HAVING SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END)
        <> SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END)
);

-- name: trial_balance_not_zero | per currency, all signed movements net to zero (double-entry holds globally)
SELECT COUNT(*) FROM (
    SELECT currency FROM curated_entries GROUP BY currency HAVING SUM(signed_amount) <> 0
);

-- name: single_leg_curated_transactions | no curated transaction has fewer than 2 legs
SELECT COUNT(*) FROM (
    SELECT transaction_id FROM curated_entries GROUP BY transaction_id HAVING COUNT(*) < 2
);

-- name: orphan_entries | every curated leg references a known account (referential integrity)
SELECT COUNT(*) FROM curated_entries e
WHERE NOT EXISTS (SELECT 1 FROM ref_accounts a WHERE a.account_id = e.account_id);

-- name: currency_mismatch | every curated leg uses its account's currency
SELECT COUNT(*) FROM curated_entries e
JOIN ref_accounts a ON a.account_id = e.account_id
WHERE a.currency <> e.currency;

-- name: header_detail_mismatch | transaction headers agree with their legs (leg count and total)
SELECT COUNT(*) FROM curated_transactions t
JOIN (
    SELECT transaction_id, COUNT(*) AS legs,
           SUM(CASE WHEN direction = 'DEBIT' THEN amount ELSE 0 END) AS debit_total
    FROM curated_entries GROUP BY transaction_id
) e USING (transaction_id)
WHERE t.leg_count <> e.legs OR t.total_amount <> e.debit_total;

-- name: entries_without_header | every curated leg has a transaction header
SELECT COUNT(*) FROM curated_entries e
WHERE NOT EXISTS (SELECT 1 FROM curated_transactions t WHERE t.transaction_id = e.transaction_id);

-- name: file_row_accounting | for each loaded file: rows_read = curated + quarantined + already_loaded
SELECT COUNT(*) FROM file_loads
WHERE status = 'SUCCESS'
  AND rows_read <> rows_curated + rows_quarantined + rows_already_loaded;

-- name: curated_rows_match_file_loads | curated row count equals the sum reported by successful file loads
SELECT CASE WHEN
    (SELECT COUNT(*) FROM curated_entries)
  = (SELECT COALESCE(SUM(rows_curated), 0) FROM file_loads WHERE status = 'SUCCESS')
THEN 0 ELSE 1 END;

-- name: non_positive_amounts | no curated leg has amount <= 0
SELECT COUNT(*) FROM curated_entries WHERE amount <= 0;

-- name: quarantine_without_reason | every quarantined row has a reason and a source location
SELECT COUNT(*) FROM quarantine
WHERE reason_code IS NULL OR source_file IS NULL OR file_checksum IS NULL;
