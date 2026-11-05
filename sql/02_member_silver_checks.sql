-- EXERCISE 02: Run AFTER you've implemented initial Silver Member.
SELECT COUNT(*) AS silver_member_count
FROM payer_02_dev.silver.member;

SELECT member_id, COUNT(*) AS duplicate_count
FROM payer_02_dev.silver.member
GROUP BY member_id
HAVING COUNT(*) > 1;

SELECT COUNT(*) AS null_member_ids
FROM payer_02_dev.silver.member
WHERE member_id IS NULL;

-- Review status values; do not assume they are valid without checking the source contract.
SELECT member_status, COUNT(*) AS members
FROM payer_02_dev.silver.member
GROUP BY member_status ORDER BY members DESC;
