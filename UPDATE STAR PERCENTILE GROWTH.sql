UPDATE K12INTEL_USERDATA.XTBL_TEST_SCORES
SET TEST_GROWTH_PERCENTILE = NULL

;
UPDATE K12INTEL_USERDATA.XTBL_TEST_SCORES
SET TEST_GROWTH_PERCENTILE = TEST_GROWTH_TARGET_1
WHERE TEST_GROWTH_PERCENTILE IS NULL AND TEST_GROWTH_TARGET_1 IS NOT NULL
;
COMMIT;

SELECT * FROM K12INTEL_USERDATA.XTBL_TEST_SCORES
WHERE TEST_GROWTH_PERCENTILE IS NOT NULL
;
UPDATE K12INTEL_DW.FTBL_TEST_SCORES
SET TEST_GROWTH_PERCENTILE = TEST_GROWTH_TARGET_1, SYS_UPDATED = SYSDATE
WHERE TEST_GROWTH_PERCENTILE IS NULL AND TEST_GROWTH_TARGET_1 IS NOT NULL
;
commit;