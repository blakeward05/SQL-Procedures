DROP PROCEDURE K12INTEL_METADATA.BLD_F_SAR_ACAD;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_F_SAR_ACAD"
(
	p_PARAM_BUILD_ID			IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
	p_PARAM_PACKAGE_ID			IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
	p_PARAM_TASK_ID				IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
	p_PARAM_USE_FULL_REFRESH	IN NUMBER,
	p_PARAM_STAGE_SOURCE		IN VARCHAR2,
	p_PARAM_MISC_PARAMS			IN VARCHAR2,
	p_PARAM_EXECUTION_STATUS	OUT NUMBER
) IS
  PRAGMA AUTONOMOUS_TRANSACTION;

	v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_F_SAR_ACAD';
	v_WAREHOUSE_KEY NUMBER(10,0) := 0;
	v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
	v_AUDIT_NATURAL_KEY VARCHAR2(512) := '';
	v_BASE_NATURALKEY_TXT VARCHAR(512) := '';
	v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
	v_STAT_ROWS_INSERTED NUMBER(10,0) := 0;
	v_STAT_ROWS_UPDATED NUMBER(10,0) := 0;
	v_STAT_ROWS_DELETED NUMBER(10,0) := 0;
	v_STAT_ROWS_EVOLVED NUMBER(10,0) := 0;
	v_STAT_ROWS_AUDITED NUMBER(10,0) := 0;
	v_STAT_DBERR_COUNT NUMBER(10,0) := 0;

/*
 <!--
     Versifit Structured Comment Block
 -->
 <ETL_COMMENTS>
     <AUTHOR>Versifit</AUTHOR>
     <COPYRIGHT>Copyright (c) 2014 VersiFit Technologies LLC. All rights reserved.</COPYRIGHT>
     <DESCRIPTION>
     </DESCRIPTION>
     <LANGUAGE>PLSQL</LANGUAGE>
     <TYPE>BLD</TYPE>
     <CONNECTOR>EDVANTAGE</CONNECTOR>
     <QUALIFIER></QUALIFIER>
     <TARGETS>
         <TARGET SCHEMA="K12INTEL_DW" NAME="FTBL_STUDENTS_AT_RISK"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="12/30/2009" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
         <CHANGE DATE="12/01/2010" USER="Josh Meyer" VERSION="10.6.0"  DESC="Changes to DTBL_SCHOOL_DATES/DTBL_SCHOOLS for school_code from number to varchar"/>
         <CHANGE DATE="12/01/2011" USER="Jason Hildebrandt" VERSION="10.6.0"  DESC="Changed SCHOOL_CODE filters to convert to number and compare < 9000 so it did not drop schols 97,99 etc..."/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_DW.FTBL_STUDENTS_AT_RISK WHERE SYS_ETL_SOURCE = 'BLD_F_SAR_ACAD'
     DELETE K12INTEL_DW.FTBL_STUDENTS_AT_RISK WHERE SYS_ETL_SOURCE = 'BLD_F_SAR_ACAD'
*/

    v_start_time                CONSTANT DATE := sysdate;
    v_buffer varchar(30);
    v_const_school_year         NUMBER(10) := k12intel_metadata.get_school_year(SYSDATE);
    v_datadate DATE             := get_lastdata_date;

-- Auditing related variables
  v_table_id NUMBER(10);
  v_rowcnt   NUMBER;
BEGIN
  v_table_id := -999;


  /*
    Local variables and cursors
  */
  DECLARE
    v_EXISTING_STUDENT_AT_RISK_KEY NUMBER(10);
    v_begin_grade VARCHAR(2);
    v_end_grade VARCHAR(2);

    TYPE curAtRisk_t IS REF CURSOR;
    cur curAtRisk_t;
    stmt_str VARCHAR2(8000);

    v_ftbl_students_at_risk_record K12INTEL_DW.FTBL_STUDENTS_AT_RISK%ROWTYPE;

    v_student_at_risk_status VARCHAR2(20);
    v_dtbl_students_record K12INTEL_DW.DTBL_STUDENTS%ROWTYPE;
    v_dtbl_students_evolved_record K12INTEL_DW.DTBL_STUDENTS_EVOLVED%ROWTYPE;
    v_ftbl_stu_at_risk_rec K12INTEL_DW.FTBL_STUDENTS_AT_RISK%ROWTYPE;
    v_ftbl_stu_at_risk_rec_inact K12INTEL_DW.FTBL_STUDENTS_AT_RISK%ROWTYPE;

  BEGIN
    DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'pre-cursor');

    -- Obtain the next SCHOOL_KEY when inserting a new record
    --SELECT NVL(MAX(SCHOOL_KEY), 0) + 1 INTO v_SCHOOL_KEY FROM K12INTEL_DW.DTBL_RISK_FACTORS;

   v_ftbl_students_at_risk_record.SYS_ETL_SOURCE   := v_SYS_ETL_SOURCE;
   v_ftbl_students_at_risk_record.SYS_CREATED        := sysdate;
   v_ftbl_students_at_risk_record.SYS_UPDATED        := sysdate;
   v_ftbl_students_at_risk_record.SYS_AUDIT_IND    := 'N';


    -- Loop through all records in the xtbl_schools table then transform and store values into
    -- a DTBL_RISK_FACTORS rowtype variable for later processing.
    FOR v_students_at_risk_data IN (
        SELECT
            drsk.RISK_FACTOR_KEY,
            drsk.RISK_FACTOR_STATUS,
            drsk.RISK_FACTOR_EFFECTIVE_END,
            drsk.RISK_FACTOR_SEVERITY,
            drsk.RISK_FACTOR_DEFAULT_DURATION,
            drsk.RISK_FACTOR_DEFAULT_ACTION,
            CASE
                WHEN xrsk.RISK_FACTOR_SOURCE_FILTER = '@@@' OR xrsk.RISK_FACTOR_SOURCE_FILTER IS NULL OR (LENGTH(RTRIM(LTRIM(xrsk.RISK_FACTOR_SOURCE_FILTER)))=0) THEN '1=1'
                ELSE xrsk.RISK_FACTOR_SOURCE_FILTER
            END RISK_FACTOR_SOURCE_FILTER,
            CASE
                WHEN xrsk.RISK_FACTOR_CONDITION_CODE = '@@@' OR xrsk.RISK_FACTOR_CONDITION_CODE IS NULL OR (LENGTH(RTRIM(LTRIM(xrsk.RISK_FACTOR_CONDITION_CODE)))=0) THEN '1=1'
                ELSE xrsk.RISK_FACTOR_CONDITION_CODE
            END RISK_FACTOR_CONDITION_CODE,
            CASE
                WHEN xrsk.RISK_FACTOR_CONDITION_LOGIC = '@@@' OR xrsk.RISK_FACTOR_CONDITION_LOGIC IS NULL OR (LENGTH(RTRIM(LTRIM(xrsk.RISK_FACTOR_CONDITION_LOGIC)))=0) THEN '1=1'
                ELSE xrsk.RISK_FACTOR_CONDITION_LOGIC
            END RISK_FACTOR_CONDITION_LOGIC,
            CASE
                WHEN RISK_MEASURE_VALUE_1_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_MEASURE_VALUE_1_LOGIC
            END RISK_MEASURE_VALUE_1_LOGIC,
            CASE
                WHEN RISK_MEASURE_VALUE_2_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_MEASURE_VALUE_2_LOGIC
            END RISK_MEASURE_VALUE_2_LOGIC,
            CASE
                WHEN RISK_MEASURE_VALUE_3_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_MEASURE_VALUE_3_LOGIC
            END RISK_MEASURE_VALUE_3_LOGIC,
            CASE
                WHEN RISK_MEASURE_VALUE_4_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_MEASURE_VALUE_4_LOGIC
            END RISK_MEASURE_VALUE_4_LOGIC,
            CASE
                WHEN RISK_REPORT_TEXT_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_REPORT_TEXT_LOGIC
            END RISK_REPORT_TEXT_1_LOGIC,
            CASE
                WHEN RISK_REPORT_TEXT_2_LOGIC IS NULL THEN 'NULL'
                ELSE RISK_REPORT_TEXT_2_LOGIC
            END RISK_REPORT_TEXT_2_LOGIC
        FROM K12INTEL_DW.DTBL_RISK_FACTORS drsk
            INNER JOIN K12INTEL_USERDATA.XTBL_RISK_FACTORS xrsk
                ON xrsk.RISK_FACTOR_SCOPE = 'FTBL_SAR_ACAD'
                AND xrsk.RISK_FACTOR_ID = drsk.RISK_FACTOR_ID
        WHERE drsk.RISK_FACTOR_STATUS = 'Active'
            AND sysdate BETWEEN drsk.RISK_FACTOR_EFFECTIVE_START AND drsk.RISK_FACTOR_EFFECTIVE_END
    ) LOOP
    BEGIN
        stmt_str := 'SELECT '
            || '    case '
            || '        when active_rows.student_key is null then ''Inactive'' '
            || '        else ''Active'' '
            || '    end STUDENT_AT_RISK_STATUS '
            || '    ,active_rows.* '
            || '    ,inactive_rows.* '
            || 'FROM ( '
            || '        SELECT '
            || '         dtbl_students.STUDENT_KEY '
            || '         ,dtbl_students.STUDENT_CURRENT_SCHOOL_CODE '
            || '         ,dtbl_students.STUDENT_CURRENT_GRADE_CODE '
            || '         ,dtbl_students_evolved.STUDENT_EVOLVE_KEY '
            || '         ,'|| '''--''' ||' STUDENT_RISK_FACTOR_MET_IND '
            || '         ,'|| '0' ||' STUDENT_RISK_FACTOR_MET_VALUE '
            || '         ,'|| v_students_at_risk_data.RISK_MEASURE_VALUE_1_LOGIC ||' RISK_FACTOR_MEASURE_VALUE_1 '
            || '         ,'|| v_students_at_risk_data.RISK_MEASURE_VALUE_2_LOGIC ||' RISK_FACTOR_MEASURE_VALUE_2 '
            || '         ,'|| v_students_at_risk_data.RISK_MEASURE_VALUE_3_LOGIC ||' RISK_FACTOR_MEASURE_VALUE_3 '
            || '         ,'|| v_students_at_risk_data.RISK_MEASURE_VALUE_4_LOGIC ||' RISK_FACTOR_MEASURE_VALUE_4 '
            || '         ,'|| v_students_at_risk_data.RISK_REPORT_TEXT_1_LOGIC ||' RISK_FACTOR_REPORT_TEXT_1 '
            || '         ,'|| v_students_at_risk_data.RISK_REPORT_TEXT_2_LOGIC ||' RISK_FACTOR_REPORT_TEXT_2 '
            || '        FROM '
            || '         K12INTEL_DW.DTBL_STUDENTS dtbl_students '
            || '         INNER JOIN K12INTEL_DW.DTBL_STUDENTS_EXTENSION dtbl_students_extension '
            || '         ON dtbl_students.student_key = dtbl_students_extension.student_key '
            || '         INNER JOIN K12INTEL_DW.DTBL_STUDENT_DETAILS dtbl_student_details '
            || '         ON dtbl_students.student_key = dtbl_student_details.student_key '
            || '         INNER JOIN K12INTEL_DW.DTBL_SCHOOLS dtbl_schools '
            || '         ON dtbl_students.STUDENT_CURRENT_SCHOOL_CODE = dtbl_schools.school_code '
            || '         LEFT JOIN K12INTEL_DW.DTBL_STUDENTS_EVOLVED dtbl_students_evolved '
            || '         ON dtbl_students_evolved.STUDENT_ID = dtbl_students.STUDENT_ID AND sysdate BETWEEN dtbl_students_evolved.SYS_BEGIN_DATE AND dtbl_students_evolved.SYS_END_DATE '
            || '        left join ( '
            || '            select student_key, '
            || '                sum(case when dtbl_scales.SCALE_ABBREVIATION IN (''D'', ''F'') or dtbl_scales.SCALE_PASS_FAIL_INDICATOR = ''Fail'' then 1 else 0 end) FAILING_MARKS, '
            || '                sum(case when dtbl_scales.SCALE_ABBREVIATION = ''A'' then 1 else 0 end) A_MARKS, '
            || '                sum(case when dtbl_scales.SCALE_ABBREVIATION = ''B'' then 1 else 0 end) B_MARKS, '
            || '                sum(case when dtbl_scales.SCALE_ABBREVIATION = ''C'' then 1 else 0 end) C_MARKS, '
            || '                count(*) TOTAL_MARKS '
            || '            from k12intel_dw.FTBL_FINAL_MARKS '
            || '                inner join k12intel_dw.dtbl_school_dates '
            || '                    on FTBL_FINAL_MARKS.SCHOOL_DATES_KEY = dtbl_school_dates.SCHOOL_DATES_KEY '
            || '                inner join k12intel_dw.dtbl_scales '
            || '                    on FTBL_FINAL_MARKS.SCALE_KEY = dtbl_scales.SCALE_KEY '
            || '                inner join k12intel_dw.dtbl_courses '
            || '                    on FTBL_FINAL_MARKS.COURSE_KEY = dtbl_courses.COURSE_KEY '
            || '            where '
            || '                dtbl_school_dates.ROLLING_LOCAL_SCHOOL_YR_NUMBER = 0 '
            || '            group by '
            || '                student_key '
            || '            ) marks '
            || '                on dtbl_students.student_key = marks.student_key '
            || '        WHERE ( '
            || v_students_at_risk_data.RISK_FACTOR_SOURCE_FILTER
            || '         ) '
            || '         AND '
            || '         ( '
            || v_students_at_risk_data.RISK_FACTOR_CONDITION_LOGIC
            || '         ) '
            || '         AND '
            || '         ( '
            || '             DTBL_SCHOOLS.SYS_DUMMY_IND  =  ''N'' '
            || '             AND ( '
            || '                 DTBL_SCHOOLS.SCHOOL_TYPE  NOT IN  (''@ERR'', ''ADMINISTRATIVE'', ''CHAPTER 220'', ''HEAD START NON-MPS'', ''OPEN ENROLLMENT'', ''PRIVATE'', ''RESIDENTIAL CARE CENTER'', ''Traditional'', ''UNKNOWN'') '
            || '                 OR '
            || '                 DTBL_SCHOOLS.SCHOOL_TYPE  =  ''Traditional'' '
            || '                 AND DTBL_SCHOOLS.STATE_DISTRICT_CODE  =  ''3619'' '
            || '                 AND DTBL_SCHOOLS.STATE_SCHOOL_AGENCY  =  ''PUBLIC SCHOOL'' '
            || '                 ) '
            || '            ) '
            || '            AND CASE WHEN nvl(length(trim(translate(trim(DTBL_SCHOOLS.school_code),''0123456789'',''          ''))),0) = 0 THEN CASE WHEN DTBL_SCHOOLS.school_code < 9000 THEN 1 ELSE 0 END ELSE 0 END = 1 '
            || ') active_rows FULL OUTER JOIN ( '
            || '    SELECT '
            || '     a.STUDENT_AT_RISK_KEY ,a.RISK_FACTOR_KEY ,a.STUDENT_KEY ,a.STUDENT_EVOLVE_KEY ,a.SCHOOL_KEY ,a.SCHOOL_ANNUAL_ATTRIBS_KEY '
            || '     ,a.CALENDAR_DATE_KEY ,a.SCHOOL_DATES_KEY ,a.STUDENT_RISK_IDENTIFIED_DATE ,a.STUDENT_RISK_EXPIRE_DATE ,a.STUDENT_RISK_STATUS '
            || '     ,a.STUDENT_RISK_OUTCOME ,a.STUDENT_RISK_SEVERITY_SCORE ,a.STUDENT_RISK_DURATION ,a.STUDENT_RISK_FACTOR_MET_IND '
            || '     ,a.STUDENT_RISK_FACTOR_MET_VALUE ,a.STUDENT_RISK_MEASURE_VALUE ,a.STUDENT_RISK_MEASURE_VALUE_2 ,a.STUDENT_RISK_REPORT_TEXT '
            || '     ,a.STUDENT_RISK_NOTES ,a.DISTRICT_CODE ,a.SYS_ETL_SOURCE ,a.SYS_AUDIT_IND '
            || '    FROM '
            || '     K12INTEL_DW.FTBL_STUDENTS_AT_RISK a '
            || '    WHERE risk_factor_key = :1 '
            || ') inactive_rows '
            || '    ON active_rows.student_key = inactive_rows.student_key '
--            || 'WHERE ROWNUM <= 10000 '
    ;

      OPEN cur FOR stmt_str
      USING v_students_at_risk_data.RISK_FACTOR_KEY;

    LOOP
      FETCH cur
      INTO
        v_student_at_risk_status,
        v_dtbl_students_record.STUDENT_KEY,
        v_dtbl_students_record.STUDENT_CURRENT_SCHOOL_CODE,
        v_dtbl_students_record.STUDENT_CURRENT_GRADE_CODE,
        v_dtbl_students_evolved_record.STUDENT_EVOLVE_KEY,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_FACTOR_MET_IND,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_FACTOR_MET_VALUE,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE_2,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE_3,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE_4,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_REPORT_TEXT,
        v_ftbl_stu_at_risk_rec.STUDENT_RISK_REPORT_TEXT_2,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_AT_RISK_KEY,
        v_ftbl_stu_at_risk_rec_inact.RISK_FACTOR_KEY,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_KEY,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_EVOLVE_KEY,
        v_ftbl_stu_at_risk_rec_inact.SCHOOL_KEY,
        v_ftbl_stu_at_risk_rec_inact.SCHOOL_ANNUAL_ATTRIBS_KEY,
        v_ftbl_stu_at_risk_rec_inact.CALENDAR_DATE_KEY,
        v_ftbl_stu_at_risk_rec_inact.SCHOOL_DATES_KEY,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_IDENTIFIED_DATE,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_EXPIRE_DATE,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_STATUS,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_OUTCOME,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_SEVERITY_SCORE,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_DURATION,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_FACTOR_MET_IND,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_FACTOR_MET_VALUE,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_MEASURE_VALUE,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_MEASURE_VALUE_2,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_REPORT_TEXT,
        v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_NOTES,
        v_ftbl_stu_at_risk_rec_inact.DISTRICT_CODE,
        v_ftbl_stu_at_risk_rec_inact.SYS_ETL_SOURCE,
        v_ftbl_stu_at_risk_rec_inact.SYS_AUDIT_IND
        ;
      EXIT WHEN cur%NOTFOUND;

    BEGIN
        IF v_student_at_risk_status = 'Active' THEN
            -- SYS_AUDIT_IND
            v_ftbl_students_at_risk_record.SYS_AUDIT_IND   := 'N';
            v_ftbl_students_at_risk_record.SYS_PARTITION_VALUE   := 0;
            v_BASE_NATURALKEY_TXT := '@TODO';--'RISK_FACTOR_ID=' || v_risk_factors_data.RISK_FACTOR_ID;

            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'facilities_key');
             -- Obtain the FACILITIES_KEY from the dtbl_facilities table


            ---------------------------------------------------------------
            -- STUDENT_AT_RISK_KEY
            ---------------------------------------------------------------
            BEGIN
                SELECT nvl(max(STUDENT_AT_RISK_KEY), 1000) + 1
                INTO v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY
                FROM k12intel_dw.FTBL_STUDENTS_AT_RISK;

                --v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY := v_students_at_risk_data.RISK_FACTOR_KEY;
                K12INTEL_METADATA.GEN_STUDENT_AT_RISK_KEY
                (
                  v_dtbl_students_record.STUDENT_KEY,
                  v_students_at_risk_data.RISK_FACTOR_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY
                );

                --v_WAREHOUSE_KEY := v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY;
            EXCEPTION
            WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY := -1;
                v_WAREHOUSE_KEY := NULL;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- RISK_FACTOR_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'RISK_FACTOR_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := v_students_at_risk_data.RISK_FACTOR_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_KEY := v_dtbl_students_record.STUDENT_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;



            ---------------------------------------------------------------
            -- STUDENT_EVOLVE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_EVOLVE_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY := v_dtbl_students_evolved_record.STUDENT_EVOLVE_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_EVOLVE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_ANNUAL_ATTRIBS_KEY
            ---------------------------------------------------------------
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ANNUAL_ATTRIBS_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- SCHOOL_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_KEY');
            BEGIN
              SELECT school_key, FACILITIES_KEY
              INTO v_ftbl_students_at_risk_record.SCHOOL_KEY, v_ftbl_students_at_risk_record.FACILITIES_KEY
              FROM k12intel_dw.dtbl_schools
              WHERE school_code = v_dtbl_students_record.STUDENT_CURRENT_SCHOOL_CODE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_KEY := 0;
                v_ftbl_students_at_risk_record.FACILITIES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'FACILITIES_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_ANNUAL_ATTRIBS_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_ANNUAL_ATTRIBS_KEY');
            BEGIN
              --SELECT school_key
              --INTO v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY
              --FROM k12intel_dw.dtbl_schools
              --WHERE school_key = v_ftbl_students_at_risk_record.SCHOOL_KEY
              --  AND school_year = v_students_at_risk_data.MARK_LOCAL_SCHOOL_YEAR;
              v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_ANNUAL_ATTRIBS_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- CALENDAR_DATE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'CALENDAR_DATE_KEY');
            BEGIN
              SELECT /*date_value, */calendar_date_key
              into v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY
              FROM k12intel_dw.dtbl_calendar_dates
              WHERE date_value = v_datadate;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'CALENDAR_DATE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_DATES_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_DATES_KEY');
            BEGIN
              SELECT school_dates_key
              into v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY
              FROM k12intel_dw.dtbl_school_dates
              WHERE date_value = v_datadate
                AND school_code = v_dtbl_students_record.STUDENT_CURRENT_SCHOOL_CODE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_IDENTIFIED_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_IDENTIFIED_DATE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := v_datadate;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_IDENTIFIED_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_EXPIRE_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_EXPIRE_DATE');
            DECLARE
              v_end_of_school_year_date K12INTEL_DW.DTBL_SCHOOL_DATES.DATE_VALUE%TYPE;
            BEGIN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := NULL;
                SELECT DATE_VALUE as END_OF_SCHOOL_YR_DATE
                INTO v_end_of_school_year_date
                FROM
                (
                 SELECT
                  LOCAL_ENROLL_DAY_IN_SCHOOL_YR
                  ,DATE_VALUE
                  ,ROW_NUMBER() OVER (PARTITION BY SCHOOL_CODE ORDER BY LOCAL_ENROLL_DAY_IN_SCHOOL_YR DESC, DATE_VALUE DESC) ROW_NUM
                 FROM
                  K12INTEL_DW.DTBL_SCHOOL_DATES
                 WHERE
                  SCHOOL_CODE = v_dtbl_students_record.STUDENT_CURRENT_SCHOOL_CODE
                  AND ROLLING_LOCAL_SCHOOL_YR_NUMBER = 0
                ) a
                WHERE ROW_NUM = 1;

                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE + v_students_at_risk_data.RISK_FACTOR_DEFAULT_DURATION;

                IF (v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE > v_students_at_risk_data.RISK_FACTOR_EFFECTIVE_END AND v_students_at_risk_data.RISK_FACTOR_EFFECTIVE_END < v_end_of_school_year_date) THEN
                    v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_students_at_risk_data.RISK_FACTOR_EFFECTIVE_END;
                ELSIF (v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE > v_end_of_school_year_date) THEN
                    v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_end_of_school_year_date;
                END IF;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_EXPIRE_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_STATUS
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_STATUS');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS := 'Active';
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_OUTCOME
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_OUTCOME');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME := v_students_at_risk_data.RISK_FACTOR_DEFAULT_ACTION;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_OUTCOME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_SEVERITY_SCORE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_SEVERITY_SCORE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE := v_students_at_risk_data.RISK_FACTOR_SEVERITY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_SEVERITY_SCORE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_DURATION
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_DURATION');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_DURATION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_FACTOR_MET_IND
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_FACTOR_MET_IND');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND := '@ERR';
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_FACTOR_MET_IND',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_FACTOR_MET_VALUE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_FACTOR_MET_VALUE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_FACTOR_MET_VALUE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_MEASURE_VALUE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_MEASURE_VALUE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE := v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_MEASURE_VALUE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_MEASURE_VALUE_2
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_MEASURE_VALUE_2');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 := v_ftbl_stu_at_risk_rec.STUDENT_RISK_MEASURE_VALUE_2;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_MEASURE_VALUE_2',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_REPORT_TEXT
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_REPORT_TEXT');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT := v_ftbl_stu_at_risk_rec.STUDENT_RISK_REPORT_TEXT;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_REPORT_TEXT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_NOTES
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_NOTES');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES := NULL;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_NOTES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- DISTRICT_CODE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'DISTRICT_CODE');
            BEGIN
              v_ftbl_students_at_risk_record.DISTRICT_CODE := '@ERR';
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.DISTRICT_CODE := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;
        ELSE
            -- SYS_AUDIT_IND
            v_ftbl_students_at_risk_record.SYS_AUDIT_IND   := 'N';
            v_ftbl_students_at_risk_record.SYS_PARTITION_VALUE   := 0;
            v_BASE_NATURALKEY_TXT := '@TODO';--'RISK_FACTOR_ID=' || v_risk_factors_data.RISK_FACTOR_ID;

            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'facilities_key');
             -- Obtain the FACILITIES_KEY from the dtbl_facilities table


            ---------------------------------------------------------------
            -- STUDENT_AT_RISK_KEY
            ---------------------------------------------------------------
            BEGIN
                v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY := v_ftbl_stu_at_risk_rec_inact.STUDENT_AT_RISK_KEY;

                v_WAREHOUSE_KEY := v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY;
            EXCEPTION
            WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY := -1;
                v_WAREHOUSE_KEY := NULL;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- RISK_FACTOR_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'RISK_FACTOR_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := v_ftbl_stu_at_risk_rec_inact.RISK_FACTOR_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.RISK_FACTOR_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_KEY := v_ftbl_stu_at_risk_rec_inact.STUDENT_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;



            ---------------------------------------------------------------
            -- STUDENT_EVOLVE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_EVOLVE_KEY');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY := v_ftbl_stu_at_risk_rec_inact.STUDENT_EVOLVE_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_EVOLVE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_ANNUAL_ATTRIBS_KEY
            ---------------------------------------------------------------
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ANNUAL_ATTRIBS_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- SCHOOL_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_KEY');
            BEGIN
              SELECT dsch.school_key, dsch.FACILITIES_KEY
              INTO v_ftbl_students_at_risk_record.SCHOOL_KEY, v_ftbl_students_at_risk_record.FACILITIES_KEY
              FROM k12intel_dw.dtbl_schools dsch
                INNER JOIN k12intel_dw.dtbl_students dstu
                  ON dsch.school_code = dstu.student_current_school_code
              WHERE dstu.student_key = v_ftbl_stu_at_risk_rec_inact.STUDENT_KEY;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_KEY := 0;
                v_ftbl_students_at_risk_record.FACILITIES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'FACILITIES_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_ANNUAL_ATTRIBS_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_ANNUAL_ATTRIBS_KEY');
            BEGIN
              --SELECT school_key
              --INTO v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY
              --FROM k12intel_dw.dtbl_schools
              --WHERE school_key = v_ftbl_students_at_risk_record.SCHOOL_KEY
              --  AND school_year = v_students_at_risk_data.MARK_LOCAL_SCHOOL_YEAR;
              v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'SCHOOL_ANNUAL_ATTRIBS_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- CALENDAR_DATE_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'CALENDAR_DATE_KEY');
            BEGIN
              SELECT /*date_value, */calendar_date_key
              INTO v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY
              FROM k12intel_dw.dtbl_calendar_dates
              WHERE date_value = v_datadate;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'CALENDAR_DATE_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- SCHOOL_DATES_KEY
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'SCHOOL_DATES_KEY');
            BEGIN
              --SELECT school_dates_key
              --into v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY
              --FROM k12intel_dw.dtbl_school_dates
              --WHERE date_value = v_last_datadate
              --  AND school_code = v_students_at_risk_data.STUDENT_CURRENT_SCHOOL_CODE;
              v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY := 0;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'RISK_FACTOR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_IDENTIFIED_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_IDENTIFIED_DATE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := v_datadate;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_IDENTIFIED_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_EXPIRE_DATE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_EXPIRE_DATE');
            DECLARE
              v_end_of_school_year_date K12INTEL_DW.DTBL_SCHOOL_DATES.DATE_VALUE%TYPE;
            BEGIN
              IF v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_STATUS = 'Active' THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_datadate - 1;
              ELSE
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_EXPIRE_DATE;
              END IF;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_EXPIRE_DATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_STATUS
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_STATUS');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS := 'Inactive';
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_OUTCOME
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_OUTCOME');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME := NVL(v_students_at_risk_data.RISK_FACTOR_DEFAULT_ACTION, v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_OUTCOME);
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_OUTCOME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_SEVERITY_SCORE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_SEVERITY_SCORE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE := NVL(v_students_at_risk_data.RISK_FACTOR_SEVERITY, v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_SEVERITY_SCORE);
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_SEVERITY_SCORE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_DURATION
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_DURATION');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := 0;
              IF v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_STATUS = 'Active' THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := v_datadate - v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_IDENTIFIED_DATE;
              ELSE
                v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_DURATION;
              END IF;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_DURATION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_FACTOR_MET_IND
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_FACTOR_MET_IND');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_FACTOR_MET_IND;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_FACTOR_MET_IND',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_FACTOR_MET_VALUE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_FACTOR_MET_VALUE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_FACTOR_MET_VALUE;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE := 0;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_FACTOR_MET_VALUE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;

            ---------------------------------------------------------------
            -- STUDENT_RISK_MEASURE_VALUE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_MEASURE_VALUE');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE := NULL;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_MEASURE_VALUE;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_MEASURE_VALUE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_MEASURE_VALUE_2
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_MEASURE_VALUE_2');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_MEASURE_VALUE_2;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_MEASURE_VALUE_2',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_REPORT_TEXT
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_REPORT_TEXT');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT := v_ftbl_stu_at_risk_rec_inact.STUDENT_RISK_REPORT_TEXT;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_REPORT_TEXT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- STUDENT_RISK_NOTES
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'STUDENT_RISK_NOTES');
            BEGIN
              v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES := NULL;
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES := NULL;
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RISK_NOTES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;


            ---------------------------------------------------------------
            -- DISTRICT_CODE
            ---------------------------------------------------------------
            DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'DISTRICT_CODE');
            BEGIN
              v_ftbl_students_at_risk_record.DISTRICT_CODE := '@ERR';
            EXCEPTION
              WHEN OTHERS THEN
                v_ftbl_students_at_risk_record.DISTRICT_CODE := '@ERR';
                v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                        p_PARAM_BUILD_ID,
                        p_PARAM_PACKAGE_ID,
                        p_PARAM_TASK_ID,
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped error.',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                );
            END;
        END IF;

        ---------------------------------------------------------------
        -- SYS_CURRENT_IND
        ---------------------------------------------------------------
        BEGIN
          v_ftbl_students_at_risk_record.SYS_CURRENT_IND := 'N';
        EXCEPTION
          WHEN OTHERS THEN
            v_ftbl_students_at_risk_record.SYS_CURRENT_IND := 'N';
            v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

            K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'SYS_CURRENT_IND',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
            );
        END;

        ---------------------------------------------------------------
        -- SYS_PRIMARY_ANNUAL_IND
        ---------------------------------------------------------------
        BEGIN
          v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND := 'N';
        EXCEPTION
          WHEN OTHERS THEN
            v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND := 'N';
            v_ftbl_students_at_risk_record.SYS_AUDIT_IND := 'Y';
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

            K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'SYS_PRIMARY_ANNUAL_IND',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
            );
        END;

        DBMS_APPLICATION_INFO.set_module(module_name => p_PARAM_BUILD_ID||':'||p_PARAM_PACKAGE_ID||':'||p_PARAM_TASK_ID, action_name => 'insert/update');
        /*************************************
              Insert or Update database
         *************************************/
        -- CHECK FOR EXISITING STUDENT AT RISK RECORD AND UPDATE IF EXISTS, ELSE INSERT A NEW STUDENT AT RISK RECORD
        BEGIN
          SELECT STUDENT_AT_RISK_KEY INTO v_EXISTING_STUDENT_AT_RISK_KEY
          FROM K12INTEL_DW.FTBL_STUDENTS_AT_RISK
          WHERE STUDENT_AT_RISK_KEY = v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY;
          --WHERE STUDENT_KEY = v_ftbl_students_at_risk_record.STUDENT_KEY
          --  AND RISK_FACTOR_KEY = v_ftbl_students_at_risk_record.RISK_FACTOR_KEY;

          /*
            Dimension record exists so update it if it needs it.
          */
          BEGIN
            v_rowcnt := 0;
            UPDATE K12INTEL_DW.FTBL_STUDENTS_AT_RISK
            SET
               RISK_FACTOR_KEY              = v_ftbl_students_at_risk_record.RISK_FACTOR_KEY,
               STUDENT_KEY                  = v_ftbl_students_at_risk_record.STUDENT_KEY,
               STUDENT_EVOLVE_KEY           = v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY,
               STUDENT_ANNUAL_ATTRIBS_KEY   = v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY,
               SCHOOL_KEY                   = v_ftbl_students_at_risk_record.SCHOOL_KEY,
               SCHOOL_ANNUAL_ATTRIBS_KEY    = v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY,
               FACILITIES_KEY               = v_ftbl_students_at_risk_record.FACILITIES_KEY,
               CALENDAR_DATE_KEY            = v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY,
               SCHOOL_DATES_KEY             = v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY,
               STUDENT_RISK_IDENTIFIED_DATE = v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE,
               STUDENT_RISK_EXPIRE_DATE     = v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE,
               STUDENT_RISK_STATUS          = v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS,
               STUDENT_RISK_OUTCOME         = v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME,
               STUDENT_RISK_SEVERITY_SCORE  = v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE,
               STUDENT_RISK_DURATION        = v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION,
               STUDENT_RISK_FACTOR_MET_IND  = v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND,
               STUDENT_RISK_FACTOR_MET_VALUE= v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE,
               STUDENT_RISK_MEASURE_VALUE   = v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE,
               STUDENT_RISK_MEASURE_VALUE_2 = v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2,
               STUDENT_RISK_MEASURE_VALUE_3 = v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_3,
               STUDENT_RISK_MEASURE_VALUE_4 = v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_4,
               STUDENT_RISK_REPORT_TEXT     = v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT,
               STUDENT_RISK_REPORT_TEXT_2     = v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT_2,
               STUDENT_RISK_NOTES           = v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES,
               DISTRICT_CODE                = v_ftbl_students_at_risk_record.DISTRICT_CODE,
               SYS_CURRENT_IND              = v_ftbl_students_at_risk_record.SYS_CURRENT_IND,
               SYS_PRIMARY_ANNUAL_IND       = v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND,
               SYS_UPDATED                  = v_ftbl_students_at_risk_record.SYS_UPDATED,
               SYS_AUDIT_IND                = v_ftbl_students_at_risk_record.SYS_AUDIT_IND
          WHERE STUDENT_AT_RISK_KEY = v_EXISTING_STUDENT_AT_RISK_KEY
                  AND (
                      (
                        (RISK_FACTOR_KEY != v_ftbl_students_at_risk_record.RISK_FACTOR_KEY) OR
                        (RISK_FACTOR_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.RISK_FACTOR_KEY IS NULL) OR
                        (RISK_FACTOR_KEY IS NULL AND v_ftbl_students_at_risk_record.RISK_FACTOR_KEY IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_KEY != v_ftbl_students_at_risk_record.STUDENT_KEY) OR
                        (STUDENT_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_KEY IS NULL) OR
                        (STUDENT_KEY IS NULL AND v_ftbl_students_at_risk_record.STUDENT_KEY IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_EVOLVE_KEY != v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY) OR
                        (STUDENT_EVOLVE_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY IS NULL) OR
                        (STUDENT_EVOLVE_KEY IS NULL AND v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_ANNUAL_ATTRIBS_KEY != v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY) OR
                        (STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NULL) OR
                        (STUDENT_ANNUAL_ATTRIBS_KEY IS NULL AND v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL)
                      ) OR

                      (
                        (SCHOOL_KEY != v_ftbl_students_at_risk_record.SCHOOL_KEY) OR
                        (SCHOOL_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.SCHOOL_KEY IS NULL) OR
                        (SCHOOL_KEY IS NULL AND v_ftbl_students_at_risk_record.SCHOOL_KEY IS NOT NULL)
                      ) OR

                      (
                        (SCHOOL_ANNUAL_ATTRIBS_KEY != v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY) OR
                        (SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL) OR
                        (SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL AND v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL)
                      ) OR

                      (
                        (FACILITIES_KEY != v_ftbl_students_at_risk_record.FACILITIES_KEY) OR
                        (FACILITIES_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.FACILITIES_KEY IS NULL) OR
                        (FACILITIES_KEY IS NULL AND v_ftbl_students_at_risk_record.FACILITIES_KEY IS NOT NULL)
                      ) OR

                      (
                        (CALENDAR_DATE_KEY != v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY) OR
                        (CALENDAR_DATE_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY IS NULL) OR
                        (CALENDAR_DATE_KEY IS NULL AND v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY IS NOT NULL)
                      ) OR

                      (
                        (SCHOOL_DATES_KEY != v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY) OR
                        (SCHOOL_DATES_KEY IS NOT NULL AND v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY IS NULL) OR
                        (SCHOOL_DATES_KEY IS NULL AND v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_IDENTIFIED_DATE != v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE) OR
                        (STUDENT_RISK_IDENTIFIED_DATE IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE IS NULL) OR
                        (STUDENT_RISK_IDENTIFIED_DATE IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_EXPIRE_DATE != v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE) OR
                        (STUDENT_RISK_EXPIRE_DATE IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE IS NULL) OR
                        (STUDENT_RISK_EXPIRE_DATE IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_STATUS != v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS) OR
                        (STUDENT_RISK_STATUS IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS IS NULL) OR
                        (STUDENT_RISK_STATUS IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_OUTCOME != v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME) OR
                        (STUDENT_RISK_OUTCOME IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME IS NULL) OR
                        (STUDENT_RISK_OUTCOME IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_SEVERITY_SCORE != v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE) OR
                        (STUDENT_RISK_SEVERITY_SCORE IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE IS NULL) OR
                        (STUDENT_RISK_SEVERITY_SCORE IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_DURATION != v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION) OR
                        (STUDENT_RISK_DURATION IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION IS NULL) OR
                        (STUDENT_RISK_DURATION IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_FACTOR_MET_IND != v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND) OR
                        (STUDENT_RISK_FACTOR_MET_IND IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND IS NULL) OR
                        (STUDENT_RISK_FACTOR_MET_IND IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_FACTOR_MET_VALUE != v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE) OR
                        (STUDENT_RISK_FACTOR_MET_VALUE IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE IS NULL) OR
                        (STUDENT_RISK_FACTOR_MET_VALUE IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_MEASURE_VALUE != v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE) OR
                        (STUDENT_RISK_MEASURE_VALUE IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE IS NULL) OR
                        (STUDENT_RISK_MEASURE_VALUE IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_MEASURE_VALUE_2 != v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2) OR
                        (STUDENT_RISK_MEASURE_VALUE_2 IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 IS NULL) OR
                        (STUDENT_RISK_MEASURE_VALUE_2 IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2 IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_MEASURE_VALUE_3 != v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_3) OR
                        (STUDENT_RISK_MEASURE_VALUE_3 IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_3 IS NULL) OR
                        (STUDENT_RISK_MEASURE_VALUE_3 IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_3 IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_MEASURE_VALUE_4 != v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_4) OR
                        (STUDENT_RISK_MEASURE_VALUE_4 IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_4 IS NULL) OR
                        (STUDENT_RISK_MEASURE_VALUE_4 IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_4 IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_REPORT_TEXT != v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT) OR
                        (STUDENT_RISK_REPORT_TEXT IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT IS NULL) OR
                        (STUDENT_RISK_REPORT_TEXT IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_REPORT_TEXT_2 != v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT_2) OR
                        (STUDENT_RISK_REPORT_TEXT_2 IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT_2 IS NULL) OR
                        (STUDENT_RISK_REPORT_TEXT_2 IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT_2 IS NOT NULL)
                      ) OR

                      (
                        (STUDENT_RISK_NOTES != v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES) OR
                        (STUDENT_RISK_NOTES IS NOT NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES IS NULL) OR
                        (STUDENT_RISK_NOTES IS NULL AND v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES IS NOT NULL)
                      ) OR

                      (
                        (DISTRICT_CODE != v_ftbl_students_at_risk_record.DISTRICT_CODE) OR
                        (DISTRICT_CODE IS NOT NULL AND v_ftbl_students_at_risk_record.DISTRICT_CODE IS NULL) OR
                        (DISTRICT_CODE IS NULL AND v_ftbl_students_at_risk_record.DISTRICT_CODE IS NOT NULL)
                      ) OR

                      (
                        (SYS_CURRENT_IND != v_ftbl_students_at_risk_record.SYS_CURRENT_IND) OR
                        (SYS_CURRENT_IND IS NOT NULL AND v_ftbl_students_at_risk_record.SYS_CURRENT_IND IS NULL) OR
                        (SYS_CURRENT_IND IS NULL AND v_ftbl_students_at_risk_record.SYS_CURRENT_IND IS NOT NULL)
                      ) OR

                      (
                        (SYS_PRIMARY_ANNUAL_IND != v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND) OR
                        (SYS_PRIMARY_ANNUAL_IND IS NOT NULL AND v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND IS NULL) OR
                        (SYS_PRIMARY_ANNUAL_IND IS NULL AND v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND IS NOT NULL)
                      ) OR

                      (
                        (SYS_AUDIT_IND != v_ftbl_students_at_risk_record.SYS_AUDIT_IND) OR
                        (SYS_AUDIT_IND IS NOT NULL AND v_ftbl_students_at_risk_record.SYS_AUDIT_IND IS NULL) OR
                        (SYS_AUDIT_IND IS NULL AND v_ftbl_students_at_risk_record.SYS_AUDIT_IND IS NOT NULL)
                      )
				  )
            ;

          v_rowcnt := v_rowcnt + SQL%ROWCOUNT;
            COMMIT;
          IF v_rowcnt > 0 THEN
                v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(v_rowcnt);
          END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            RAISE;
          WHEN OTHERS THEN
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

            K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'UPDATE/COMMIT',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
            );
        END;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            /*
              Dimension record doesn't so insert it.
            */
          BEGIN
            INSERT INTO K12INTEL_DW.FTBL_STUDENTS_AT_RISK
            VALUES (
                  v_ftbl_students_at_risk_record.STUDENT_AT_RISK_KEY,
                  v_ftbl_students_at_risk_record.RISK_FACTOR_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_EVOLVE_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_ANNUAL_ATTRIBS_KEY,
                  v_ftbl_students_at_risk_record.SCHOOL_KEY,
                  v_ftbl_students_at_risk_record.SCHOOL_ANNUAL_ATTRIBS_KEY,
                  v_ftbl_students_at_risk_record.FACILITIES_KEY,
                  v_ftbl_students_at_risk_record.CALENDAR_DATE_KEY,
                  v_ftbl_students_at_risk_record.SCHOOL_DATES_KEY,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_IDENTIFIED_DATE,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_EXPIRE_DATE,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_STATUS,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_OUTCOME,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_SEVERITY_SCORE,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_DURATION,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_IND,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_FACTOR_MET_VALUE,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_2,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_3,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_MEASURE_VALUE_4,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_REPORT_TEXT_2,
                  v_ftbl_students_at_risk_record.STUDENT_RISK_NOTES,
                  v_ftbl_students_at_risk_record.DISTRICT_CODE,
                  v_ftbl_students_at_risk_record.SYS_ETL_SOURCE,
                  v_ftbl_students_at_risk_record.SYS_CURRENT_IND,
                  v_ftbl_students_at_risk_record.SYS_PRIMARY_ANNUAL_IND,
                  v_ftbl_students_at_risk_record.SYS_CREATED,
                  v_ftbl_students_at_risk_record.SYS_UPDATED,
                  v_ftbl_students_at_risk_record.SYS_AUDIT_IND,
                  v_ftbl_students_at_risk_record.SYS_PARTITION_VALUE
            );

              --v_SCHOOL_KEY := v_SCHOOL_KEY + 1;
              COMMIT;
           	v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED(v_rowcnt);
          EXCEPTION
          WHEN OTHERS THEN
              v_WAREHOUSE_KEY := 0;
              v_AUDIT_BASE_SEVERITY := 0;
              v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
              v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

              K12INTEL_METADATA.WRITE_AUDIT(
                      p_PARAM_BUILD_ID,
                      p_PARAM_PACKAGE_ID,
                      p_PARAM_TASK_ID,
                      v_SYS_ETL_SOURCE,
                      'INSERT/COMMIT',
                      v_WAREHOUSE_KEY,
                      v_AUDIT_NATURAL_KEY,
                      'Untrapped error.',
                      sqlerrm,
                      'Y',
                      v_AUDIT_BASE_SEVERITY
              );
          END;
        END; -- END Insert/Update Processing

    EXCEPTION
      WHEN OTHERS THEN
        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

        K12INTEL_METADATA.WRITE_AUDIT(
                p_PARAM_BUILD_ID,
                p_PARAM_PACKAGE_ID,
                p_PARAM_TASK_ID,
                v_SYS_ETL_SOURCE,
                'INNER CURSOR LOOP',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped error.',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
        );

    END;
    v_STAT_ROWS_PROCESSED := v_STAT_ROWS_PROCESSED + 1;

      -- Update the stats every 1000 records
      IF MOD(v_STAT_ROWS_PROCESSED, 1000) = 0 THEN
        -- Write task stats
        BEGIN
			K12INTEL_METADATA.WORKFLOW_WRITE_TASK_STATS(
				p_PARAM_BUILD_ID,
				p_PARAM_PACKAGE_ID,
				p_PARAM_TASK_ID,
				v_STAT_ROWS_PROCESSED,
				v_STAT_ROWS_INSERTED,
				v_STAT_ROWS_UPDATED,
				v_STAT_ROWS_DELETED,
				v_STAT_ROWS_EVOLVED,
				v_STAT_ROWS_AUDITED,
				v_STAT_DBERR_COUNT
			);
        EXCEPTION
          WHEN OTHERS THEN
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
            v_AUDIT_NATURAL_KEY := NULL;

            K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'WRITE TASK STATS',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
            );
        END;
      END IF;

      END LOOP;

      CLOSE cur;

      -- Update the stats every outer cursor record
      BEGIN
			K12INTEL_METADATA.WORKFLOW_WRITE_TASK_STATS(
				p_PARAM_BUILD_ID,
				p_PARAM_PACKAGE_ID,
				p_PARAM_TASK_ID,
				v_STAT_ROWS_PROCESSED,
				v_STAT_ROWS_INSERTED,
				v_STAT_ROWS_UPDATED,
				v_STAT_ROWS_DELETED,
				v_STAT_ROWS_EVOLVED,
				v_STAT_ROWS_AUDITED,
				v_STAT_DBERR_COUNT
			);
      EXCEPTION
          WHEN OTHERS THEN
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
            v_AUDIT_NATURAL_KEY := NULL;

            K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'WRITE TASK STATS',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
            );
      END;
    EXCEPTION
      WHEN OTHERS THEN
        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

        K12INTEL_METADATA.WRITE_AUDIT(
                p_PARAM_BUILD_ID,
                p_PARAM_PACKAGE_ID,
                p_PARAM_TASK_ID,
                v_SYS_ETL_SOURCE,
                'OUTER CURSOR LOOP',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped error.',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
        );
      END;
    END LOOP; -- END K12INTEL_USERDATA.XTBL_RISK_FACTORS cursor

    -- Write task stats
    BEGIN
		K12INTEL_METADATA.WORKFLOW_WRITE_TASK_STATS(
			p_PARAM_BUILD_ID,
			p_PARAM_PACKAGE_ID,
			p_PARAM_TASK_ID,
			v_STAT_ROWS_PROCESSED,
			v_STAT_ROWS_INSERTED,
			v_STAT_ROWS_UPDATED,
			v_STAT_ROWS_DELETED,
			v_STAT_ROWS_EVOLVED,
			v_STAT_ROWS_AUDITED,
			v_STAT_DBERR_COUNT
		);
    EXCEPTION
      WHEN OTHERS THEN
        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
        v_AUDIT_NATURAL_KEY := NULL;

        K12INTEL_METADATA.WRITE_AUDIT(
                p_PARAM_BUILD_ID,
                p_PARAM_PACKAGE_ID,
                p_PARAM_TASK_ID,
                v_SYS_ETL_SOURCE,
                'WRITE TASK STATS',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped error.',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
        );
    END;

    DBMS_OUTPUT.PUT_LINE('Run Time: ' || TRUNC((sysdate - v_start_time)*24*60*60) || ' Sec');
  END;
EXCEPTION
  WHEN OTHERS THEN
    v_WAREHOUSE_KEY := 0;
    v_AUDIT_BASE_SEVERITY := 0;
    v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
    v_AUDIT_NATURAL_KEY := NULL;

    K12INTEL_METADATA.WRITE_AUDIT(
            p_PARAM_BUILD_ID,
            p_PARAM_PACKAGE_ID,
            p_PARAM_TASK_ID,
            v_SYS_ETL_SOURCE,
            'TOTAL BUILD FAILURE!',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'Untrapped error.',
            sqlerrm,
            'Y',
            v_AUDIT_BASE_SEVERITY
    );
    -- Write task stats
    BEGIN
		K12INTEL_METADATA.WORKFLOW_WRITE_TASK_STATS(
			p_PARAM_BUILD_ID,
			p_PARAM_PACKAGE_ID,
			p_PARAM_TASK_ID,
			v_STAT_ROWS_PROCESSED,
			v_STAT_ROWS_INSERTED,
			v_STAT_ROWS_UPDATED,
			v_STAT_ROWS_DELETED,
			v_STAT_ROWS_EVOLVED,
			v_STAT_ROWS_AUDITED,
			v_STAT_DBERR_COUNT
		);
    EXCEPTION
      WHEN OTHERS THEN
        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
        v_AUDIT_NATURAL_KEY := NULL;

        K12INTEL_METADATA.WRITE_AUDIT(
                p_PARAM_BUILD_ID,
                p_PARAM_PACKAGE_ID,
                p_PARAM_TASK_ID,
                v_SYS_ETL_SOURCE,
                'WRITE TASK STATS',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped error.',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
        );
    END;
    RAISE;
END BLD_F_SAR_ACAD;
/
