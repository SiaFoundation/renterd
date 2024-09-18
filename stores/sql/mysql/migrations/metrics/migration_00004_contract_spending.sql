ALTER TABLE contracts DROP COLUMN download_spending_lo;
ALTER TABLE contracts DROP COLUMN download_spending_hi;
ALTER TABLE contracts RENAME COLUMN list_spending_lo TO sector_roots_spending_lo;
ALTER TABLE contracts RENAME COLUMN list_spending_hi TO sector_roots_spending_hi;