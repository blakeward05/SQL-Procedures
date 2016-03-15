DROP MATERIALIZED VIEW K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES;
CREATE MATERIALIZED VIEW K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES (TEST_SCORES_KEY,TESTS_KEY,SCHOOL_KEY,SCHOOL_ANNUAL_ATTRIBS_KEY,STUDENT_KEY,STUDENT_ANNUAL_ATTRIBS_KEY,CALENDAR_DATE_KEY,SCHOOL_DATES_KEY,TEST_RECORD_TYPE,TEST_TYPE,TEST_ADMIN_PERIOD,ROLLING_ADMIN_NBR,DATE_VALUE,TEST_PRIMARY_RESULT_CODE,TEST_PRIMARY_RESULT,TEST_SECONDARY_RESULT_CODE,TEST_SECONDARY_RESULT,TEST_ITEMS_ATTEMPTED,TEST_SCORE_VALUE,TEST_RAW_SCORE,TEST_SCALED_SCORE,TEST_LOWER_BOUND,TEST_UPPER_BOUND,TEST_NCE_SCORE,TEST_PERCENTILE_SCORE,TEST_GRADE_EQUIVALENT,TEST_READING_LEVEL,TEST_STANDARD_ERROR,TEST_QUARTILE_SCORE,TEST_DECILE_SCORE,TEST_GROWTH_TARGET_1,TEST_GROWTH_TARGET_2,TEST_SCORE_TEXT,TEST_STUDENT_GRADE,SUBJECT,SEASON,SCHOOL_YEAR,GRADE,TARGET_PERCENTILE,CALENDAR_TYPE,WIN_START_DATE,WIN_END_DATE,IN_WINDOW,ATTEMPT,SEASON_ATTEMPT,CREATED)
TABLESPACE K12INTEL_DW_DATA
PCTUSED    0
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOCACHE
LOGGING
NOCOMPRESS
NOPARALLEL
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
WITH PRIMARY KEY
AS 
/* Formatted on 2/17/2016 10:04:16 AM (QP5 v5.269.14213.34746) */
SELECT tsc.test_scores_key,
       tst.tests_key,
       sch.school_key,
       schaa.school_annual_attribs_key,
       st.student_key,
       staa.student_annual_attribs_key,
       cd.calendar_date_key,
       sd.school_dates_key,
       tsc.test_record_type,
       tst.test_type,
       tsc.test_admin_period,
       adm.rolling_admin_nbr,
       cd.date_value,
       trg.test_benchmark_code AS test_primary_result_code,
       trg.test_benchmark_name AS test_primary_result,
       tsc.test_secondary_result_code,
       tsc.test_secondary_result,
       tsc.test_items_attempted,
       tsc.test_score_value,
       tsc.test_raw_score,
       tsc.test_scaled_score,
       tsc.test_lower_bound,
       tsc.test_upper_bound,
       tsc.test_nce_score,
       tsc.test_percentile_score,
       tsc.test_grade_equivalent,
       tsc.test_reading_level,
       tsc.test_standard_error,
       tsc.test_quartile_score,
       test_decile_score,
       test_growth_target_1,
       test_growth_target_2,
       test_score_text,
       tsc.test_student_grade,
       tst.test_subject,
       tsc.test_admin_period AS season,
       sd.local_school_year AS school_year,
       CASE
          WHEN tsc.test_student_grade = 'KG' THEN 'K5'
          ELSE tsc.test_student_grade
       END
          AS grade,
       trg2.min_value AS target_percentile,
       win.calendar_type,
       win.start_date,
       win.end_date,
       CASE
          WHEN cd.date_value BETWEEN win.start_date AND win.end_date
          THEN
             'Yes'
          ELSE
             'No'
       END
          AS in_window,
       ROW_NUMBER ()
       OVER (
          PARTITION BY st.student_key,
                       tsc.test_admin_period,
                       tst.test_subject,
                       CASE
                          WHEN cd.date_value BETWEEN win.start_date
                                                 AND win.end_date
                          THEN
                             'Yes'
                          ELSE
                             'No'
                       END
          ORDER BY cd.date_value)
          AS attempt,
       ROW_NUMBER ()
       OVER (
          PARTITION BY st.student_key,
                       tsc.test_admin_period,
                       tst.test_subject
          ORDER BY cd.date_value)
          AS season_attempt,
       SYSDATE
  FROM K12INTEL_DW.FTBL_TEST_SCORES tsc
       INNER JOIN K12INTEL_DW.DTBL_TESTS tst ON tst.tests_key = tsc.tests_key
       INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES cd
          ON cd.calendar_date_key = tsc.calendar_date_key
       INNER JOIN K12INTEL_DW.DTBL_SCHOOL_DATES sd
          ON sd.school_dates_key = tsc.school_dates_key
       INNER JOIN K12INTEL_DW.DTBL_SCHOOLS sch
          ON sch.school_key = tsc.school_key
       INNER JOIN K12INTEL_STAGING_MPSENT.ENT_ENTITY_MASTER_VIEW ent
          ON     ent.esis_id = TO_NUMBER (sch.school_code)
             AND TO_CHAR (ent.school_year_fall) =
                    SUBSTR (sd.local_school_year, 1, 4)
       LEFT JOIN K12INTEL_DW.MPSD_STAR_WINDOWS win
          ON     win.calendar_type = ent.calendar
             AND win.season = tsc.test_admin_period
             AND win.school_year = sd.local_school_year
       INNER JOIN K12INTEL_DW.DTBL_SCHOOL_ANNUAL_ATTRIBS schaa
          ON tsc.school_annual_attribs_key = schaa.school_annual_attribs_key
       INNER JOIN K12INTEL_DW.DTBL_STUDENTS st
          ON tsc.student_key = st.student_key
       INNER JOIN K12INTEL_DW.DTBL_STUDENT_ANNUAL_ATTRIBS staa
          ON staa.student_annual_attribs_key = tsc.student_annual_attribs_key
       INNER JOIN K12INTEL_DW.MPSD_TEST_ADMIN_NUMBER adm
          ON     adm.test_type = SUBSTR (test_name, 1, 4)
             AND tsc.test_admin_period = adm.season
       LEFT JOIN K12INTEL_DW.DTBL_TEST_BENCHMARKS trg
          ON     trg.tests_key = tst.tests_key
             AND tsc.test_percentile_score BETWEEN trg.min_value
                                               AND trg.max_value
             AND trg.effective_start_date <= cd.date_value
             AND trg.effective_end_date >= cd.date_value
       INNER JOIN K12INTEL_DW.DTBL_TEST_BENCHMARKS trg2
          ON     trg2.tests_key = tst.tests_key
             AND trg2.test_benchmark_code = '2'
 WHERE     tst.test_name LIKE 'STAR%'
       AND tst.test_class = 'Component'
       AND tsc.test_percentile_score IS NOT NULL;


COMMENT ON MATERIALIZED VIEW K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES IS 'snapshot table for snapshot K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES';

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_SCHAAK ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(SCHOOL_ANNUAL_ATTRIBS_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_SCHAAK
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_SCHOOL_KEY ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(SCHOOL_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_SCHOOL_KEY
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_SCHOOL_YEAR ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(SCHOOL_YEAR)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_SCHOOL_YEAR
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_SDK ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(SCHOOL_DATES_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_SDK
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_SEASON ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(SEASON)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_SEASON
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_STUAAK ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(STUDENT_ANNUAL_ATTRIBS_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_STUAAK
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_STUDENT_KEY ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(STUDENT_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_STUDENT_KEY
  MONITORING USAGE;

CREATE INDEX K12INTEL_DW.MPSIDX_STAR_TEST_SCORES_KEY ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES
(TEST_SCORES_KEY)
LOGGING
TABLESPACE K12INTEL_DW_INDEX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
            FLASH_CACHE      DEFAULT
            CELL_FLASH_CACHE DEFAULT
           )
NOPARALLEL;

ALTER INDEX K12INTEL_DW.MPSIDX_STAR_TEST_SCORES_KEY
  MONITORING USAGE;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO BOREPORTS;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO DW_READ_ONLY;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO HYPSUITE_ACCESS;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO SIP_SYSTEM;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO VFX_K12_REPORTING;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO WAREHS_DUHVELOPER;

GRANT SELECT ON K12INTEL_DW.MPS_MV_STAR_COMPONENT_SCORES TO WAREHS_READ_ONLY;
