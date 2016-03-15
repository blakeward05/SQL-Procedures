DROP PROCEDURE K12INTEL_METADATA.BLD_F_TEST_SCORES_XTBL;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_F_TEST_SCORES_XTBL"
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
    v_initialize NUMBER(10) := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INITIALIZE(p_PARAM_PACKAGE_ID,p_PARAM_BUILD_ID,p_PARAM_TASK_ID,p_PARAM_USE_FULL_REFRESH);

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
         <TARGET SCHEMA="K12INTEL_METADATA" NAME="FTBL_TEST_SCORES"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="03/14/2012" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
         <CHANGE DATE="01/26/2016" USER="Versifit" VERSION="10.6.0"  DESC="Change to collection based loop over XTBL_TEST_ADMIN"/>
         <CHANGE DATE="03/09/2016  USER="Versifit" VERSION="10.6.0"  DESC="Changed TEST_SCORE_TEXT to put TEST_SCORE_TEXT from record instead of TEST_SCORE_VALUE"/>"
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_METADATA.FTBL_TEST_SCORES WHERE SYS_ETL_SOURCE = 'BLD_F_TEST_SCORES_XTBL'
     DELETE K12INTEL_METADATA.FTBL_TEST_SCORES WHERE SYS_ETL_SOURCE = 'BLD_F_TEST_SCORES_XTBL'
 */

 ------------------------------------------------------------------
 -- setting vars here!
 ------------------------------------------------------------------

 	v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_F_TEST_SCORES_XTBL';
    v_LOCAL_CURRENT_SCHOOL_YEAR NUMBER := K12INTEL_METADATA.GET_SIS_SCHOOL_YEAR_IC(p_PARAM_STAGE_SOURCE);
    v_NETCHANGE_CUTOFF  DATE := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.GET_NETCHANGE_CUTOFF();
    v_LOCAL_DATA_DATE DATE := K12INTEL_METADATA.GET_LASTDATA_DATE();


 	v_WAREHOUSE_KEY NUMBER(10,0) := 0;
 	v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
 	v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
 	v_AUDIT_NATURAL_KEY VARCHAR2(4096) := '';
 	v_BASE_NATURALKEY_TXT VARCHAR(4096) := '';


    -- local varaibles
    v_ok_to_commit          BOOLEAN;
    v_test_admin_audit_ind  CHAR(1);
    v_rowcnt                NUMBER(10);
	v_rowsinserted          NUMBER(10);
    v_rowsupdated           NUMBER(10);
    v_rowsDeleted           NUMBER(10);

    v_district_student_key  K12INTEL_DW.FTBL_TEST_SCORES.STUDENT_KEY%TYPE;
    v_district_student_id   K12INTEL_USERDATA.XTBL_TEST_ADMIN.DISTRICT_STUDENT_ID%TYPE;
    v_state_student_key     K12INTEL_DW.FTBL_TEST_SCORES.STUDENT_KEY%TYPE;
    v_state_student_id      K12INTEL_USERDATA.XTBL_TEST_ADMIN.STATE_STUDENT_ID%TYPE;
    v_federal_student_key   K12INTEL_DW.FTBL_TEST_SCORES.STUDENT_KEY%TYPE;
    v_federal_student_id    K12INTEL_USERDATA.XTBL_TEST_ADMIN.FEDERAL_STUDENT_ID%TYPE;
    v_student_id            K12INTEL_DW.DTBL_STUDENTS.STUDENT_ID%TYPE;

    v_district_school_key   K12INTEL_DW.FTBL_TEST_SCORES.SCHOOL_KEY%TYPE;
    v_district_facility_key K12INTEL_DW.FTBL_TEST_SCORES.FACILITIES_KEY%TYPE;
    v_district_school_id    K12INTEL_USERDATA.XTBL_TEST_ADMIN.DISTRICT_SCHOOL_ID%TYPE;
    v_state_school_key      K12INTEL_DW.FTBL_TEST_SCORES.SCHOOL_KEY%TYPE;
    v_state_facility_key    K12INTEL_DW.FTBL_TEST_SCORES.FACILITIES_KEY%TYPE;
    v_state_school_id       K12INTEL_USERDATA.XTBL_TEST_ADMIN.STATE_SCHOOL_ID%TYPE;
    v_federal_school_key    K12INTEL_DW.FTBL_TEST_SCORES.SCHOOL_KEY%TYPE;
    v_federal_facility_key  K12INTEL_DW.FTBL_TEST_SCORES.FACILITIES_KEY%TYPE;
    v_federal_school_id     K12INTEL_USERDATA.XTBL_TEST_ADMIN.FEDERAL_SCHOOL_ID%TYPE;
    v_school_id             K12INTEL_DW.DTBL_SCHOOLS.SCHOOL_CODE%TYPE;

    v_district_staff_key   K12INTEL_DW.DTBL_STAFF.STAFF_KEY%TYPE;
    v_state_staff_key      K12INTEL_DW.DTBL_STAFF.STAFF_KEY%TYPE;

    v_test_admin_date       DATE;
    v_local_school_year     K12INTEL_DW.DTBL_SCHOOL_DATES.LOCAL_SCHOOL_YEAR%TYPE;
    v_testscore_rec         K12INTEL_DW.FTBL_TEST_SCORES%ROWTYPE;

    v_TESTS_SUBJECT                     K12INTEL_DW.DTBL_TESTS.TEST_SUBJECT%TYPE;
    v_ETL_CUST_BNCH_TYPE                K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_TYPE%TYPE;
    v_ETL_CUST_BNCH_TEST_NUMBER         K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_TEST_NUMBER%TYPE;
    v_ETL_CUST_BNCH_ADMIN_PERIOD        K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_ADMIN_PERIOD%TYPE;
    v_ETL_CUST_BNCH_SUBJECT             K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_SUBJECT%TYPE;
    v_ETL_CUST_BNCH_GRADE_GROUP         K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_GRADE_GROUP%TYPE;
    v_ETL_CUST_BNCH_MEASURE             K12INTEL_USERDATA.XTBL_TESTS.ETL_CUST_BNCH_MEASURE%TYPE;

    v_LOCAL_PASSING_INDICATOR           K12INTEL_DW.DTBL_TEST_BENCHMARKS.PASSING_INDICATOR%TYPE;
    v_LOCAL_CUST_PASS_IND               K12INTEL_DW.DTBL_TEST_BENCHMARKS.PASSING_INDICATOR%TYPE;

    v_LOCAL_SCORE_VALUE                 K12INTEL_DW.FTBL_TEST_SCORES.TEST_SCORE_VALUE%TYPE;
    v_LOCAL_TESTS_KEY                   K12INTEL_DW.DTBL_TESTS.TESTS_KEY%TYPE;
    v_LOCAL_TEST_ADMIN_PERIOD           K12INTEL_DW.FTBL_TEST_SCORES.TEST_ADMIN_PERIOD%TYPE;
    v_LOCAL_TEST_SUBJECT                K12INTEL_DW.DTBL_TESTS.TEST_SUBJECT%TYPE;
    v_LOCAL_TEST_GRADE_GROUP            K12INTEL_DW.DTBL_TESTS.TEST_GRADE_GROUP%TYPE;

    -- Define in memory storage for XTBL_TEST_ADMIN records that
    -- need to be processed.
    v_admin_rec K12INTEL_USERDATA.XTBL_TEST_ADMIN%ROWTYPE;
    TYPE XtblTestAdminTbl IS TABLE OF K12INTEL_USERDATA.XTBL_TEST_ADMIN%ROWTYPE;
    v_admins XtblTestAdminTbl;


    PROCEDURE SET_TESTADMIN_AUDIT_IND
    (
        p_testAdminKey  IN NUMBER,
        p_auditInd      IN CHAR
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        -- Performance enhancement.  Only update if audit indicator needs to be set.
        IF p_auditInd = 'N' THEN
            UPDATE K12INTEL_USERDATA.XTBL_TEST_ADMIN
            SET SYS_AUDIT_IND = 'Y'
            WHERE TEST_ADMIN_KEY = p_testAdminKey;

            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;

            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                v_SYS_ETL_SOURCE,
                'SET_TESTADMIN_AUDIT_IND',
                0,
                'TEST_ADMIN_KEY=' || TO_CHAR(p_testAdminKey) || CHR(9) || 'SYS_AUDIT_IND=' || p_auditInd,
                'FAILED TO UPDATE TEST_ADMIN AUDIT_IND',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
            );
    END SET_TESTADMIN_AUDIT_IND;

    PROCEDURE SET_TESTSCORE_AUDIT_IND
    (
        p_testAdminKey  IN NUMBER,
        p_testNumber    IN VARCHAR2,
        p_auditInd_old  IN CHAR,
        p_auditInd_new  IN CHAR
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF p_auditInd_old <> p_auditInd_new THEN
            UPDATE K12INTEL_USERDATA.XTBL_TEST_SCORES
            SET SYS_AUDIT_IND = p_auditInd_new
            WHERE TEST_ADMIN_KEY = p_testAdminKey AND TEST_NUMBER = p_testNumber;

            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                v_SYS_ETL_SOURCE,
                'SET_TESTSCORE_AUDIT_IND',
                0,
                'TEST_ADMIN_KEY=' || TO_CHAR(p_testAdminKey)
                || CHR(9) || 'TEST_NUMBER=' || p_testNumber
                || CHR(9) || 'SYS_AUDIT_IND=' || p_auditInd_new,
                'FAILED TO UPDATE TESTSCORE_AUDIT_IND',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
            );
    END SET_TESTSCORE_AUDIT_IND;
BEGIN


    --Enrollment Lookup
    BEGIN

        --TRUNCATE K12INTEL_STAGING.ENROLL_LOOKUP;

        --? Create? an ?enrollme?nt loo?kup ta?ble to? help? resolv?e scho?ol cod?es
        --? The ?followin?g fact?ors ar?e used? to ?pick a?n enro?llment ?record ?that
        -- ?best app?roximate?s the st?udent da?ta at or? near th?e time o?f assess?ment
        -- ?1) Match? on dist?rict
        -- ?2) Match? on enro?llment w?here ass?ess date? between? begin a?nd end e?nroll da?te
        -- ?3) Pick ?the enro?llment n?earest t?o the as?sess dat?e. The e?nrollmen?t begin/?end 
        -- ?   dates? are che?cked and? the min? diff in? days is? favored?.
        INSERT INTO K12INTEL_STAGING.ENROLL_LOOKUP (STUDENT_KEY, DISTRICT_CODE, DISTRICT_STUDENT_ID, STATE_STUDENT_ID, TEST_ADMIN_DATE, SCHOOL_KEY, SCHOOL_CODE, ENTRY_GRADE_CODE, SCHOOL_STATE_ID, ENROLL_BEGIN, ENROLL_END)
        WITH ASSESS_STU (
            STUDENT_KEY
            , DISTRICT_STUDENT_ID
            , STATE_STUDENT_ID
            , TEST_ADMIN_DISTRICT_CODE
            , TEST_ADMIN_DATE
        ) AS(
            SELECT DISTINCT COALESCE(b.STUDENT_KEY, c.STUDENT_KEY) AS STUDENT_KEY, 
                            a.DISTRICT_STUDENT_ID, 
                            a.STATE_STUDENT_ID, 
                            a.DISTRICT_CODE TEST_ADMIN_DISTRICT_CODE, 
                            TO_DATE(TEST_ADMIN_DATE_STR, 'MM/DD/YYYY') TEST_ADMIN_DATE
            FROM K12INTEL_USERDATA.XTBL_TEST_ADMIN a
            LEFT JOIN K12INTEL_DW.DTBL_STUDENT_DETAILS b
                ON b.STUDENT_ID = a.DISTRICT_STUDENT_ID
            LEFT JOIN K12INTEL_DW.DTBL_STUDENT_DETAILS c
                ON c.STUDENT_STATE_ID = a.STATE_STUDENT_ID
            WHERE (a.DISTRICT_STUDENT_ID IS NOT NULL OR a.STATE_STUDENT_ID IS NOT NULL)
                AND K12INTEL_METADATA.IS_DATE(a.TEST_ADMIN_DATE_STR, 'MM/DD/YYYY') = 1
                AND (
                    (c.DISTRICT_CODE = b.DISTRICT_CODE AND c.STUDENT_KEY = b.STUDENT_KEY)
                    OR b.DISTRICT_CODE IS NULL
                    OR c.DISTRICT_CODE IS NULL
                )
        ), ENROLL (
            STUDENT_KEY
            , DISTRICT_CODE
            , DISTRICT_STUDENT_ID
            , STATE_STUDENT_ID
            , TEST_ADMIN_DISTRICT_CODE
            , TEST_ADMIN_DATE
            , SCHOOL_KEY
            , SCHOOL_CODE
            , ENTRY_GRADE_CODE
            , SCHOOL_STATE_ID
            , ENROLL_BEGIN
            , ENROLL_END
            , "RANK"
        ) AS(
            SELECT 
                e.STUDENT_KEY,
                a.DISTRICT_CODE,
                e.DISTRICT_STUDENT_ID,
                e.STATE_STUDENT_ID,
                e.TEST_ADMIN_DISTRICT_CODE,
                e.TEST_ADMIN_DATE,
                a.SCHOOL_KEY,
                d.SCHOOL_CODE,
                a.ENTRY_GRADE_CODE,
                d.SCHOOL_STATE_ID,
                b.DATE_VALUE AS ENROLL_BEGIN,
                c.DATE_VALUE AS ENROLL_END,
                ROW_NUMBER() OVER(PARTITION BY e.STUDENT_KEY, e.TEST_ADMIN_DATE ORDER BY 
                                    CASE WHEN e.TEST_ADMIN_DISTRICT_CODE = a.DISTRICT_CODE THEN 0 ELSE 1 END, 
                                    CASE WHEN e.TEST_ADMIN_DATE BETWEEN b.DATE_VALUE AND c.DATE_VALUE THEN 0 ELSE 1 END,
                                    CASE WHEN (b.DATE_VALUE - e.TEST_ADMIN_DATE) < (c.DATE_VALUE - e.TEST_ADMIN_DATE) THEN (b.DATE_VALUE - e.TEST_ADMIN_DATE) ELSE (c.DATE_VALUE - e.TEST_ADMIN_DATE) END,
                                    b.DATE_VALUE,
                                    a.ENROLLMENTS_KEY DESC) AS RANK
            FROM K12INTEL_DW.FTBL_ENROLLMENTS a
            INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES b
                ON b.CALENDAR_DATE_KEY = a.CAL_DATE_KEY_BEGIN_ENROLL
            INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES c
                ON c.CALENDAR_DATE_KEY = a.CAL_DATE_KEY_END_ENROLL
            INNER JOIN K12INTEL_DW.DTBL_SCHOOLS d
                ON d.SCHOOL_KEY = a.SCHOOL_KEY
            INNER JOIN ASSESS_STU e
                ON e.STUDENT_KEY = a.STUDENT_KEY
            WHERE a.HOME_OR_CROSS_ENROLLMENT = 'Home School'
                AND a.ENROLLMENT_TYPE = 'Actual'
        ) 
        SELECT STUDENT_KEY, DISTRICT_CODE, DISTRICT_STUDENT_ID, STATE_STUDENT_ID, TEST_ADMIN_DATE, SCHOOL_KEY, SCHOOL_CODE, ENTRY_GRADE_CODE, SCHOOL_STATE_ID, ENROLL_BEGIN, ENROLL_END
        FROM ENROLL
        WHERE "RANK" = 1;
        

    EXCEPTION
        WHEN OTHERS THEN
            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                v_SYS_ETL_SOURCE,
                'ENROLLMENT_LOOKUP',
                0,
                v_AUDIT_NATURAL_KEY,
                'Error Initializing Lookup',
                sqlerrm,
                'N',
                v_AUDIT_BASE_SEVERITY
            );
    END;


    -- Store a copy of XTBL_TEST_ADMIN records that require processing in a collection
    -- NOTE:  This will consume memory from the Oracle PGA memory area
    -- Possible future memory optimization would be to chunk data using LIMIT clause
    -- Because XTBL_TEST_ADMIN records must be updated record by record (housekeeping)
    -- use of collections was done to resolve fetch out of sequence issues which result
    -- when updating and comitting a table that the cursor was built from.
    SELECT * BULK COLLECT INTO v_admins
    FROM (
        SELECT /*+ INDEX(b IDXTSTADMIN_RECSTAGEAUDIT) */
            b.*
        FROM K12INTEL_USERDATA.XTBL_TEST_BATCH_IDS a
            INNER JOIN K12INTEL_USERDATA.XTBL_TEST_ADMIN b
            ON (a.BATCH_ID = b.BATCH_ID)
        WHERE
            a.LOAD_STATUS = 'APPROVED'
            AND (b.SYS_RECORD_STAGE = 'NOT VALIDATED' OR (b.SYS_RECORD_STAGE = 'LOADED' AND b.SYS_AUDIT_IND = 'Y'))
            AND 
            (

                (p_PARAM_STAGE_SOURCE IS NULL OR p_PARAM_STAGE_SOURCE = '') OR

                (
                    EXISTS(SELECT NULL 
                            FROM K12INTEL_USERDATA.XTBL_SCHOOL_CONTROL xsc 
                            WHERE xsc.SOURCE = p_PARAM_STAGE_SOURCE
                            and b.DISTRICT_CODE = xsc.DISTRICT_CODE)
                ) OR

                (b.DISTRICT_CODE = p_PARAM_STAGE_SOURCE)
            )
            AND b.DELETE_TEST_ADMIN_IND = 'N'
            --AND b.TEST_ADMIN_KEY = '146421' -- FOR TESTING
            --AND rownum < 10
            ORDER BY a.END_DATE ASC
            --AND (a.batch_name LIKE @BATCH_NAME_TO_PROCESS OR @BATCH_NAME_TO_PROCESS IS NULL) --KB_NOTE: Add back in variable.
    );

        FOR i IN 1 .. v_admins.COUNT LOOP BEGIN
            v_admin_rec     := v_admins(i);
            v_rowsinserted  := 0;
            v_rowsupdated   := 0;
            v_test_admin_audit_ind := 'N';
            v_ok_to_commit := TRUE;

            v_BASE_NATURALKEY_TXT := 'TEST_ADMIN_KEY=' || TO_CHAR(v_admin_rec.TEST_ADMIN_KEY);

            BEGIN
                v_BASE_NATURALKEY_TXT := v_BASE_NATURALKEY_TXT
                    || CHR(9) || 'BATCH_ID='             || TO_CHAR(v_admin_rec.BATCH_ID)
                    || CHR(9) || 'TEST_RECORD_TYPE='     || NVL(v_admin_rec.TEST_RECORD_TYPE, ' ')
                    || CHR(9) || 'FEDERAL_STUDENT_ID='   || NVL(v_admin_rec.FEDERAL_STUDENT_ID, ' ')
                    || CHR(9) || 'STATE_STUDENT_ID='     || NVL(v_admin_rec.STATE_STUDENT_ID, ' ')
                    || CHR(9) || 'DISTRICT_STUDENT_ID='  || NVL(v_admin_rec.DISTRICT_STUDENT_ID, ' ')
                    || CHR(9) || 'FEDERAL_SCHOOL_ID='    || NVL(v_admin_rec.FEDERAL_SCHOOL_ID, ' ')
                    || CHR(9) || 'STATE_SCHOOL_ID='      || NVL(v_admin_rec.STATE_SCHOOL_ID, ' ')
                    || CHR(9) || 'DISTRICT_SCHOOL_ID='   || NVL(v_admin_rec.DISTRICT_SCHOOL_ID, ' ')
                    || CHR(9) || 'TEST_ADMIN_DATE_STR='  || NVL(v_admin_rec.TEST_ADMIN_DATE_STR, ' ')
                    || CHR(9) || 'PROD_TEST_ID='         || NVL(v_admin_rec.PROD_TEST_ID, ' ')
                    || CHR(9) || 'DISTRICT_CODE='        || NVL(v_admin_rec.DISTRICT_CODE, ' ');
            EXCEPTION
                WHEN OTHERS THEN
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'v_BASE_NATURALKEY_TXT',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Error setting v_BASE_NATURALKEY_TXT',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- DISTRICT_CODE
            ----------------------------------------
            BEGIN
                v_testscore_rec.DISTRICT_CODE := TRIM(COALESCE(v_admin_rec.DISTRICT_CODE, ''));

                IF v_testscore_rec.DISTRICT_CODE IS NULL THEN
                    --v_ok_to_commit := FALSE;
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.DISTRICT_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Required field DISTRICT_CODE is NULL',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    --v_ok_to_commit := FALSE;
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.DISTRICT_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ----------------------------------------
            -- SYS_PARTITION_VALUE
            ----------------------------------------
            BEGIN
                v_testscore_rec.SYS_PARTITION_VALUE := 0;
            EXCEPTION
                WHEN OTHERS THEN
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SYS_PARTITION_VALUE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Error setting SYS_PARTITION_VALUE',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- SYS_ETL_SOURCE
            ----------------------------------------
            BEGIN
                v_testscore_rec.SYS_ETL_SOURCE := v_SYS_ETL_SOURCE;
            EXCEPTION
                WHEN OTHERS THEN
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SYS_ETL_SOURCE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Error setting SYS_ETL_SOURCE',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ----------------------------------------
            -- Lookup calendar date key
            ----------------------------------------
            BEGIN
                v_test_admin_date := TO_DATE(v_admin_rec.TEST_ADMIN_DATE_STR, 'MM/DD/YYYY');

                SELECT CALENDAR_DATE_KEY
                INTO v_testscore_rec.CALENDAR_DATE_KEY
                FROM K12INTEL_DW.DTBL_CALENDAR_DATES
                WHERE DATE_VALUE = v_test_admin_date;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_ok_to_commit := FALSE;
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.CALENDAR_DATE_KEY := 0;
                    v_test_admin_date := NULL;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST ADMIN DATEKEY LOOKUP',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN OTHERS THEN
                    v_ok_to_commit := FALSE;
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.CALENDAR_DATE_KEY := 0;
                    v_test_admin_date := NULL;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST ADMIN DATEKEY LOOKUP',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'INVALID TEST_ADMIN_DATE_STR',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ----------------------------------------
            -- Lookup district student key
            ----------------------------------------
            IF (RTRIM(v_admin_rec.DISTRICT_STUDENT_ID) IS NOT NULL) THEN
                BEGIN
                    SELECT STUDENT_KEY, STUDENT_ID
                    INTO v_district_student_key, v_district_student_id
                    FROM K12INTEL_DW.DTBL_STUDENTS
                    WHERE 1=1
                        AND DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                        AND STUDENT_ID = v_admin_rec.DISTRICT_STUDENT_ID
                        AND STUDENT_STATUS <> 'Deleted';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_district_student_key  := 0;
                        v_district_student_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT STUDENT KEY LOOKUP - NO DISTRICT',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_district_student_key  := 0;
                        v_district_student_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT STUDENT KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_district_student_key  := 0;
                        v_district_student_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT STUDENT KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;
            ELSE
                v_district_student_key  := 0;
                v_district_student_id   := NULL;
            END IF;


            ----------------------------------------
            -- Lookup state student key
            ----------------------------------------
            IF (RTRIM(v_admin_rec.STATE_STUDENT_ID) IS NOT NULL) THEN
                BEGIN
                    SELECT /*+ INDEX(a IDXDSDT_STATEIDSKEY) INDEX(b VFIDXDSTU_SKEY) */
                        a.STUDENT_KEY, a.STUDENT_ID
                    INTO v_state_student_key, v_state_student_id
                    FROM K12INTEL_DW.DTBL_STUDENT_DETAILS a
                        INNER JOIN K12INTEL_DW.DTBL_STUDENTS b
                        ON (a.STUDENT_KEY = b.STUDENT_KEY)
                    WHERE a.DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                        AND a.STUDENT_STATE_ID = v_admin_rec.STATE_STUDENT_ID
                        AND b.STUDENT_STATUS <> 'Deleted';
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        BEGIN
                            SELECT /*+ INDEX(a IDXDSDT_STATEIDSKEY) INDEX(b VFIDXDSTU_SKEY) */
                                a.STUDENT_KEY, a.STUDENT_ID
                            INTO v_state_student_key, v_state_student_id
                            FROM K12INTEL_DW.DTBL_STUDENT_DETAILS a
                                INNER JOIN K12INTEL_DW.DTBL_STUDENTS b
                                ON (a.STUDENT_KEY = b.STUDENT_KEY)
                            WHERE a.STUDENT_STATE_ID = v_admin_rec.STATE_STUDENT_ID
                                AND b.STUDENT_STATUS <> 'Deleted';
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_test_admin_audit_ind := 'Y';
                                v_state_student_key  := 0;
                                v_state_student_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE STUDENT KEY LOOKUP',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                BEGIN
                                
                                    SELECT STUDENT_KEY, STATE_STUDENT_ID
                                    INTO v_state_student_key, v_state_student_id
                                    FROM K12INTEL_STAGING.ENROLL_LOOKUP
                                    WHERE ROWNUM = 1 -- TOP 1
                                        AND DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                                        AND STATE_STUDENT_ID = v_admin_rec.STATE_STUDENT_ID
                                    ORDER BY ENROLL_BEGIN DESC, ENROLL_END DESC;

                                EXCEPTION
                                    WHEN NO_DATA_FOUND THEN
                                        BEGIN

                                            SELECT STUDENT_KEY, STATE_STUDENT_ID
                                            INTO v_state_student_key, v_state_student_id
                                            FROM K12INTEL_STAGING.ENROLL_LOOKUP
                                            WHERE ROWNUM = 1 -- TOP 1
                                                AND STATE_STUDENT_ID = v_admin_rec.STATE_STUDENT_ID
                                            ORDER BY ENROLL_BEGIN DESC, ENROLL_END DESC;

                                        EXCEPTION
                                            WHEN NO_DATA_FOUND THEN
                                                --v_ok_to_commit := FALSE;
                                                --v_test_admin_audit_ind := 'Y';
                                                v_state_student_key  := 0;
                                                v_state_student_id   := NULL;
                                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                    v_SYS_ETL_SOURCE,
                                                    'STATE STUDENT KEY LOOKUP',
                                                    0,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'NO_DATA_FOUND ENROLL_LOOKUP',
                                                    sqlerrm,
                                                    'N',
                                                    v_AUDIT_BASE_SEVERITY
                                                );
                                            WHEN TOO_MANY_ROWS THEN
                                                --v_ok_to_commit := FALSE;
                                                v_test_admin_audit_ind := 'Y';
                                                v_state_student_key  := 0;
                                                v_state_student_id   := NULL;
                                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                    v_SYS_ETL_SOURCE,
                                                    'STATE STUDENT KEY LOOKUP',
                                                    0,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'TOO_MANY_ROWS ENROLL_LOOKUP',
                                                    sqlerrm,
                                                    'N',
                                                    v_AUDIT_BASE_SEVERITY
                                                );
                                        END;
                                    WHEN TOO_MANY_ROWS THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_student_key  := 0;
                                        v_state_student_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE STUDENT KEY LOOKUP',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'TOO_MANY_ROWS ENROLL_LOOKUP',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );    
                                END;
                        END;
                    WHEN TOO_MANY_ROWS THEN
                        BEGIN

                            SELECT STUDENT_KEY, STATE_STUDENT_ID
                            INTO v_state_student_key, v_state_student_id
                            FROM K12INTEL_STAGING.ENROLL_LOOKUP
                            WHERE ROWNUM = 1 -- TOP 1
                                AND DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                                AND STATE_STUDENT_ID = v_admin_rec.STATE_STUDENT_ID
                            ORDER BY ENROLL_BEGIN DESC, ENROLL_END DESC;

                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                BEGIN

                                    SELECT STUDENT_KEY, STATE_STUDENT_ID
                                    INTO v_state_student_key, v_state_student_id
                                    FROM K12INTEL_STAGING.ENROLL_LOOKUP
                                    WHERE ROWNUM = 1 -- TOP 1
                                        AND STATE_STUDENT_ID = v_admin_rec.STATE_STUDENT_ID
                                    ORDER BY ENROLL_BEGIN DESC, ENROLL_END DESC;

                                EXCEPTION
                                    WHEN NO_DATA_FOUND THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_student_key  := 0;
                                        v_state_student_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE STUDENT KEY LOOKUP',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'NO_DATA_FOUND ENROLL_LOOKUP',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                    WHEN TOO_MANY_ROWS THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_student_key  := 0;
                                        v_state_student_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE STUDENT KEY LOOKUP',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'TOO_MANY_ROWS ENROLL_LOOKUP',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                END;
                            WHEN TOO_MANY_ROWS THEN
                                --v_ok_to_commit := FALSE;
                                --v_test_admin_audit_ind := 'Y';
                                v_state_student_key  := 0;
                                v_state_student_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE STUDENT KEY LOOKUP',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS ENROLL_LOOKUP',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );    
                        END;
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_state_student_key  := 0;
                        v_state_student_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STATE STUDENT KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;
            ELSE
                v_state_student_key  := 0;
                v_state_student_id   := NULL;
            END IF;




            -- Audit on conflicting district student key and state student key
            IF (v_district_student_key <> 0) AND (v_state_student_key <> 0) AND (v_district_student_key <> v_state_student_key) THEN
                v_ok_to_commit := FALSE;
                v_test_admin_audit_ind := 'Y';
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'STUDENT_KEY - COMPARE DISTRICT/STATE STUDENT KEYS',
                    0,
                    v_AUDIT_NATURAL_KEY,
                    'DISTRICT AND STATE STUDENT KEYS ARE DIFFERENT',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
           END IF;



            v_testscore_rec.STUDENT_KEY :=
                CASE
                    WHEN v_district_student_key <> 0    THEN v_district_student_key
                    WHEN v_state_student_key    <> 0    THEN v_state_student_key
                ELSE 0
                END;

            v_student_id :=
                CASE
                    WHEN v_district_student_key <> 0    THEN v_district_student_id
                    WHEN v_state_student_key    <> 0    THEN v_state_student_id
                ELSE NULL
                END;

            -- prevent record from committing when student fails to resolve
            IF v_testscore_rec.STUDENT_KEY = 0 THEN
                v_ok_to_commit := FALSE;
                v_test_admin_audit_ind := 'Y';
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'STUDENT_KEY - KEY NOT FOUND',
                    0,
                    v_AUDIT_NATURAL_KEY,
                    'Record not loaded because student key could not be determined',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            END IF;


            ----------------------------------------
            -- Lookup district school key
            ----------------------------------------
            IF (RTRIM(v_admin_rec.DISTRICT_SCHOOL_ID) IS NOT NULL) THEN
                BEGIN
                    SELECT 
                        SCHOOL_KEY, SCHOOL_CODE, FACILITIES_KEY
                    INTO v_district_school_key, v_district_school_id, v_district_facility_key
                    FROM K12INTEL_DW.DTBL_SCHOOLS
                    WHERE DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                        AND SCHOOL_CODE = v_admin_rec.DISTRICT_SCHOOL_ID;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_district_school_key  := 0;
                        v_district_school_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT SCHOOL KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_district_school_key  := 0;
                        v_district_school_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT SCHOOL KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_district_school_key  := 0;
                        v_district_school_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT SCHOOL KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;
            ELSE
                v_district_school_key  := 0;
                v_district_school_id   := NULL;
            END IF;


            ----------------------------------------
            -- Lookup state school key
            ----------------------------------------
            IF (RTRIM(v_admin_rec.STATE_SCHOOL_ID) IS NOT NULL) THEN
                BEGIN
                    SELECT /*+ INDEX(DTBL_SCHOOLS IDXDSCH_DISTSTATEID) */
                        SCHOOL_KEY, SCHOOL_CODE, FACILITIES_KEY
                    INTO v_state_school_key, v_state_school_id, v_state_facility_key
                    FROM K12INTEL_DW.DTBL_SCHOOLS
                    WHERE DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                        AND SCHOOL_STATE_ID = v_admin_rec.STATE_SCHOOL_ID;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        BEGIN

                            SELECT /*+ INDEX(DTBL_SCHOOLS IDXDSCH_DISTSTATEID) */
                                SCHOOL_KEY, SCHOOL_CODE, FACILITIES_KEY
                            INTO v_state_school_key, v_state_school_id, v_state_facility_key
                            FROM K12INTEL_DW.DTBL_SCHOOLS
                            WHERE SCHOOL_STATE_ID = v_admin_rec.STATE_SCHOOL_ID;

                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                --v_ok_to_commit := FALSE;
                                --v_test_admin_audit_ind := 'Y';
                                v_state_school_key  := 0;
                                v_state_school_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                 );
                            WHEN TOO_MANY_ROWS THEN
                                BEGIN
                                    --
                                    -- Using the state school code more then one school was returned.  This can be a result
                                    -- of program schools sharing same state code with regular school or elementry/middle/high
                                    -- schools having different district school codes but sharing same state school code.
                                    -- The following lookup attempts to resolve multi results using grade code.  Based on
                                    -- profiling this will resolve a large percent lookup failures.
                                    --
                                    SELECT /*+ INDEX(b INDXDSCH_DISTCODEKEY) INDEX(c IDXDSCD_DTVALSCODEYEAR) */
                                        b.SCHOOL_KEY, b.SCHOOL_CODE, b.FACILITIES_KEY
                                    INTO v_state_school_key, v_state_school_id, v_state_facility_key
                                    FROM K12INTEL_USERDATA.XTBL_SCHOOL_GRADES a
                                        INNER JOIN K12INTEL_DW.DTBL_SCHOOLS b
                                        -- NOTE: when DISTRICT_CODE gets added later use this join instead of join to SCHOOL_DISTRICT_CODE
                                        --ON (a.DISTRICT_CODE = b.DISTRICT_CODE AND a.DISTRICT_SCHOOL_CODE = b.SCHOOL_CODE)
                                        ON (a.DISTRICT_CODE = b.SCHOOL_DISTRICT_CODE AND a.SCHOOL_CODE = b.SCHOOL_CODE)
                                        INNER JOIN K12INTEL_DW.DTBL_SCHOOL_DATES c
                                        -- NOTE: Use join below once DISTRICT_CODE gets added to DTBL_SCHOOL_DATES
                                        --ON (a.DISTRICT_CODE = c.DISTRICT_CODE AND a.DISTRICT_SCHOOL_CODE = c.SCHOOL_CODE AND a.SCHOOL_YEAR = c.LOCAL_SCHOOL_YEAR)
                                        ON (a.SCHOOL_CODE = c.SCHOOL_CODE AND a.SCHOOL_YEAR = c.LOCAL_SCHOOL_YEAR)
                                    WHERE
                                        a.DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                                        AND a.SCHOOL_CODE = v_admin_rec.STATE_SCHOOL_ID
                                        AND a.GRADE_CODE = v_admin_rec.TEST_STUDENT_GRADE
                                        AND c.DATE_VALUE = v_test_admin_date;
                                EXCEPTION
                                   WHEN NO_DATA_FOUND THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_school_key  := 0;
                                        v_state_school_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'NO_DATA_FOUND',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                   WHEN TOO_MANY_ROWS THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_school_key  := 0;
                                        v_state_school_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'TOO_MANY_ROWS',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                    WHEN OTHERS THEN
                                        --v_ok_to_commit := FALSE;
                                        --v_test_admin_audit_ind := 'Y';
                                        v_state_school_key  := 0;
                                        v_state_school_id   := NULL;
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                            0,
                                            v_AUDIT_NATURAL_KEY,
                                            'UNTRAPPED ERROR',
                                            sqlerrm,
                                            'Y',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                END;
                        END;
                    WHEN TOO_MANY_ROWS THEN
                        BEGIN
                            --
                            -- Using the state school code more then one school was returned.  This can be a result
                            -- of program schools sharing same state code with regular school or elementry/middle/high
                            -- schools having different district school codes but sharing same state school code.
                            -- The following lookup attempts to resolve multi results using grade code.  Based on
                            -- profiling this will resolve a large percent lookup failures.
                            --
                            SELECT /*+ INDEX(b INDXDSCH_DISTCODEKEY) INDEX(c IDXDSCD_DTVALSCODEYEAR) */
                                b.SCHOOL_KEY, b.SCHOOL_CODE, b.FACILITIES_KEY
                            INTO v_state_school_key, v_state_school_id, v_state_facility_key
                            FROM K12INTEL_USERDATA.XTBL_SCHOOL_GRADES a
                                INNER JOIN K12INTEL_DW.DTBL_SCHOOLS b
                                -- NOTE: when DISTRICT_CODE gets added later use this join instead of join to SCHOOL_DISTRICT_CODE
                                --ON (a.DISTRICT_CODE = b.DISTRICT_CODE AND a.DISTRICT_SCHOOL_CODE = b.SCHOOL_CODE)
                                ON (a.DISTRICT_CODE = b.SCHOOL_DISTRICT_CODE AND a.SCHOOL_CODE = b.SCHOOL_CODE)
                                INNER JOIN K12INTEL_DW.DTBL_SCHOOL_DATES c
                                -- NOTE: Use join below once DISTRICT_CODE gets added to DTBL_SCHOOL_DATES
                                --ON (a.DISTRICT_CODE = c.DISTRICT_CODE AND a.DISTRICT_SCHOOL_CODE = c.SCHOOL_CODE AND a.SCHOOL_YEAR = c.LOCAL_SCHOOL_YEAR)
                                ON (a.SCHOOL_CODE = c.SCHOOL_CODE AND a.SCHOOL_YEAR = c.LOCAL_SCHOOL_YEAR)
                            WHERE
                                a.DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                                AND a.SCHOOL_CODE = v_admin_rec.STATE_SCHOOL_ID
                                AND a.GRADE_CODE = v_admin_rec.TEST_STUDENT_GRADE
                                AND c.DATE_VALUE = v_test_admin_date;
                        EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                                --v_ok_to_commit := FALSE;
                                --v_test_admin_audit_ind := 'Y';
                                v_state_school_key  := 0;
                                v_state_school_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                           WHEN TOO_MANY_ROWS THEN
                                --v_ok_to_commit := FALSE;
                                --v_test_admin_audit_ind := 'Y';
                                v_state_school_key  := 0;
                                v_state_school_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN OTHERS THEN
                                --v_ok_to_commit := FALSE;
                                --v_test_admin_audit_ind := 'Y';
                                v_state_school_key  := 0;
                                v_state_school_id   := NULL;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE SCHOOL KEY LOOKUP - XTBL_SCHOOL_GRADES',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'UNTRAPPED ERROR',
                                    sqlerrm,
                                    'Y',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        --v_test_admin_audit_ind := 'Y';
                        v_state_school_key  := 0;
                        v_state_school_id   := NULL;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STATE SCHOOL KEY LOOKUP',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;
            ELSE
                v_state_school_key  := 0;
                v_state_school_id   := NULL;
            END IF;


            -- Write an audit if no school id was provided or failed to lookup
            IF
            (
                (v_admin_rec.DISTRICT_SCHOOL_ID IS NULL) AND
                (v_admin_rec.STATE_SCHOOL_ID IS NULL)
            ) THEN
                v_ok_to_commit := FALSE;
                v_test_admin_audit_ind := 'Y';
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'VALIDATE REQUIRED SCHOOL ID',
                    0,
                    v_AUDIT_NATURAL_KEY,
                    'NO SCHOOL ID PROVIDED',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            END IF;


            -- Audit on conflicting district school key and state school key
            IF (v_district_school_key <> 0) AND (v_state_school_key <> 0) AND (v_district_school_key <> v_state_school_key) THEN
                v_ok_to_commit := FALSE;
                v_test_admin_audit_ind := 'Y';
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'SCHOOL_KEY - COMPARE DISTRICT/STATE SCHOOL KEYS',
                    0,
                    v_AUDIT_NATURAL_KEY,
                    'DISTRICT AND STATE SCHOOL KEYS ARE DIFFERENT',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            END IF;


            v_testscore_rec.SCHOOL_KEY :=
                CASE
                    WHEN v_district_school_key <> 0    THEN v_district_school_key
                    WHEN v_state_school_key    <> 0    THEN v_state_school_key
                ELSE 0
                END;

            v_school_id :=
                CASE
                    WHEN v_district_school_key <> 0    THEN v_district_school_id
                    WHEN v_state_school_key    <> 0    THEN v_state_school_id
                ELSE NULL
                END;

            v_testscore_rec.FACILITIES_KEY :=
                CASE
                    WHEN v_district_school_key <> 0    THEN v_district_facility_key
                    WHEN v_state_school_key    <> 0    THEN v_state_facility_key
                ELSE 0
                END;

            -- prevent record from committing when school fails to resolve
            IF v_testscore_rec.SCHOOL_KEY = 0 THEN
                v_ok_to_commit := FALSE;
                v_test_admin_audit_ind := 'Y';
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'SCHOOL_KEY - KEY NOT FOUND',
                    0,
                    v_AUDIT_NATURAL_KEY,
                    'Record not loaded because school key could not be determined',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            END IF;



            -- Only perform the following lookups if we have a
            -- valid student_key, school_key and test_admin_date
            IF (v_ok_to_commit = TRUE) THEN



                ----------------------------------------
                -- Lookup student evolve key
                ----------------------------------------
                BEGIN
                    SELECT /*+ INDEX(DTBL_STUDENTS_EVOLVED VFIDXDSTE_STUDKEYBEGINENDDATE) */
                        STUDENT_EVOLVE_KEY
                    INTO v_testscore_rec.STUDENT_EVOLVE_KEY
                    FROM K12INTEL_DW.DTBL_STUDENTS_EVOLVED
                    WHERE STUDENT_KEY = v_testscore_rec.STUDENT_KEY
                        AND v_test_admin_date BETWEEN SYS_BEGIN_DATE AND SYS_END_DATE;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_EVOLVE_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_EVOLVE_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_EVOLVE_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_EVOLVE_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_EVOLVE_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_EVOLVE_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ----------------------------------------
                -- Lookup school dates key
                ----------------------------------------
                BEGIN
                    SELECT /*+ INDEX(DTBL_SCHOOL_DATES VFIDXDSCD_SDDK) */
                        SCHOOL_DATES_KEY, LOCAL_SCHOOL_YEAR
                    INTO v_testscore_rec.SCHOOL_DATES_KEY, v_local_school_year
                    FROM K12INTEL_DW.DTBL_SCHOOL_DATES
                    WHERE
                        --DISTRICT_CODE = v_admin_rec.DISTRICT_CODE
                        SCHOOL_CODE = v_school_id
                        AND DATE_VALUE = v_test_admin_date;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_DATES_KEY := 0;
                        v_local_school_year := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT
                            || CHR(9) || 'v_school_id=' || NVL(v_school_id,'(null)')
                            || CHR(9) || 'v_test_admin_date=' || NVL(TO_CHAR(v_test_admin_date,'MM/DD/YYYY'),'(null)');

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_DATES_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_DATES_KEY := 0;
                        v_local_school_year := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_DATES_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_DATES_KEY := 0;
                        v_local_school_year := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_DATES_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ---------------------------------------------------------------
                -- STUDENT_ATTRIB_KEY
                ---------------------------------------------------------------
                BEGIN
                    IF  v_testscore_rec.STUDENT_KEY = 0 then
                        v_testscore_rec.STUDENT_ATTRIB_KEY := 0;
                    ELSE
                        SELECT STUDENT_ATTRIB_KEY
                        INTO v_testscore_rec.STUDENT_ATTRIB_KEY
                        FROM K12INTEL_DW.DTBL_STUDENTS
                        WHERE STUDENT_KEY = v_testscore_rec.STUDENT_KEY;
                    END IF;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STUDENT_ATTRIB_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ATTRIB_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STUDENT_ATTRIB_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ATTRIB_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STUDENT_ATTRIB_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ATTRIB_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;



                ----------------------------------------
                -- Lookup STUDENT_ANNUAL_ATTRIBS_KEY
                ----------------------------------------
                BEGIN
                    IF  v_testscore_rec.STUDENT_KEY = 0 OR v_LOCAL_SCHOOL_YEAR IS NULL then
                        v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                    ELSE
                        BEGIN
                            
                            SELECT STUDENT_ANNUAL_ATTRIBS_KEY
                            INTO v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY
                            FROM K12INTEL_DW.DTBL_STUDENT_ANNUAL_ATTRIBS 
                            WHERE STUDENT_KEY = v_testscore_rec.STUDENT_KEY
                                and SCHOOL_YEAR = v_LOCAL_SCHOOL_YEAR;

                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                --v_ok_to_commit := FALSE;
                                v_test_admin_audit_ind := 'Y';
                                v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_ANNUAL_ATTRIBS_KEY',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                --v_ok_to_commit := FALSE;
                                v_test_admin_audit_ind := 'Y';
                                v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_ANNUAL_ATTRIBS_KEY',
                                    0,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    END IF;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        --v_ok_to_commit := FALSE;
                        v_test_admin_audit_ind := 'Y';
                        v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;






                ---------------------------------------------------------------
                -- Lookup District STAFF_KEY 
                ---------------------------------------------------------------
                BEGIN
                    --v_testscore_rec.STAFF_KEY := 0;
                    IF (RTRIM(v_admin_rec.DISTRICT_STAFF_ID) IS NOT NULL) THEN
                        BEGIN 
                            SELECT STAFF_KEY
                            INTO v_district_staff_key
                            FROM K12INTEL_DW.DTBL_STAFF
                            WHERE STAFF_EMPLOYEE_ID = v_admin_rec.DISTRICT_STAFF_ID
                              AND DISTRICT_CODE = v_admin_rec.DISTRICT_CODE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_district_staff_key := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'DISTRICT_STAFF_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_district_staff_key := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'DISTRICT_STAFF_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    ELSE
                        v_district_staff_key := 0;
                    END IF;
                EXCEPTION
                    /*WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_district_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_district_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );*/
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_district_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'DISTRICT_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;



                ---------------------------------------------------------------
                -- Lookup State STAFF_KEY 
                ---------------------------------------------------------------
                BEGIN
                    --v_testscore_rec.STAFF_KEY := 0;
                    IF (RTRIM(v_admin_rec.STATE_STAFF_ID) IS NOT NULL) THEN
                        BEGIN 
                            SELECT STAFF_KEY
                            INTO v_state_staff_key
                            FROM K12INTEL_DW.DTBL_STAFF
                            WHERE 1=1
                              AND STAFF_STATE_ID = v_admin_rec.STATE_STAFF_ID
                              --AND STATE_EMPLOYEE_ID = v_admin_rec.STATE_STAFF_ID
                              AND DISTRICT_CODE = v_admin_rec.DISTRICT_CODE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_state_staff_key := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE_STAFF_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_state_staff_key := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STATE_STAFF_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    ELSE
                        v_state_staff_key := 0;
                    END IF;
                EXCEPTION
                    /*WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_state_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STATE_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_state_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STATE_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );*/
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_state_staff_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STATE_STAFF_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                -- Audit on conflicting district staff key and staff school key
                IF (v_district_staff_key <> 0) AND (v_state_staff_key <> 0) AND (v_district_staff_key <> v_state_staff_key) THEN
                    v_test_admin_audit_ind := 'Y';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'STAFF_KEY - COMPARE DISTRICT/STATE STAFF KEYS',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'DISTRICT AND STATE SCHOOL KEYS ARE DIFFERENT',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                END IF;


                v_testscore_rec.STAFF_KEY :=
                    CASE
                        WHEN v_district_staff_key <> 0    THEN v_district_staff_key
                        WHEN v_state_staff_key    <> 0    THEN v_state_staff_key
                        ELSE 0
                    END;



                -- prevent record from committing when staff fails to resolve
                IF v_testscore_rec.STAFF_KEY = 0 AND (RTRIM(v_admin_rec.DISTRICT_STAFF_ID) IS NOT NULL OR RTRIM(v_admin_rec.STATE_STAFF_ID) IS NOT NULL) THEN
                    v_test_admin_audit_ind := 'Y';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'STAFF_KEY - KEY NOT FOUND',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Record not loaded because staff key could not be determined',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                END IF;



                ---------------------------------------------------------------
                -- STAFF_ANNUAL_ATTRIBS_KEY
                ---------------------------------------------------------------
                BEGIN
                    v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                EXCEPTION
                    /*WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ANNUAL_ATTRIBS_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ANNUAL_ATTRIBS_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );*/
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ANNUAL_ATTRIBS_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ---------------------------------------------------------------
                -- STAFF_EVOLVE_KEY
                ---------------------------------------------------------------
                BEGIN
                    
                    IF v_testscore_rec.STAFF_KEY = 0 THEN
                      v_testscore_rec.STAFF_EVOLVE_KEY := 0;  
                    ELSE    
                        BEGIN
                               SELECT STAFF_EVOLVE_KEY
                               INTO v_testscore_rec.STAFF_EVOLVE_KEY
                               FROM K12INTEL_DW.DTBL_STAFF_EVOLVED
                               WHERE STAFF_KEY = v_testscore_rec.STAFF_KEY 
                                   AND v_test_admin_date BETWEEN SYS_BEGIN_DATE AND SYS_END_DATE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.STAFF_EVOLVE_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STAFF_EVOLVE_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.STAFF_EVOLVE_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STAFF_EVOLVE_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    END IF;
                EXCEPTION
                    /*WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_EVOLVE_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_EVOLVE_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_EVOLVE_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_EVOLVE_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );*/
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_EVOLVE_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_EVOLVE_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ---------------------------------------------------------------
                -- STAFF_ASSIGNMENT_KEY
                ---------------------------------------------------------------
                BEGIN
                    v_testscore_rec.STAFF_ASSIGNMENT_KEY := 0;
                EXCEPTION
                    /*WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ASSIGNMENT_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ASSIGNMENT_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ASSIGNMENT_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ASSIGNMENT_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );*/
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STAFF_ASSIGNMENT_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STAFF_ASSIGNMENT_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ---------------------------------------------------------------
                -- COURSE_KEY
                ---------------------------------------------------------------
                BEGIN
                    
                    IF (RTRIM(v_admin_rec.COURSE_CODE) IS NULL) THEN
                        v_testscore_rec.COURSE_KEY := 0;
                    ELSE
                        BEGIN 
                            SELECT COURSE_KEY
                            INTO v_testscore_rec.COURSE_KEY
                            FROM K12INTEL_DW.DTBL_COURSES a
                            WHERE 1=1
                                AND (a.DISTRICT_CODE = v_testscore_rec.DISTRICT_CODE OR v_testscore_rec.DISTRICT_CODE = '[ALL]')
                                AND a.COURSE_CODE = v_admin_rec.COURSE_CODE
                                AND v_test_admin_date BETWEEN a.COURSE_START_DATE AND a.COURSE_END_DATE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.COURSE_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'COURSE_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.COURSE_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'COURSE_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.COURSE_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'COURSE_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;



                ---------------------------------------------------------------
                -- COURSE_OFFERINGS_KEY
                ---------------------------------------------------------------
                BEGIN
                    
                    IF (RTRIM(v_admin_rec.COURSE_SECTION) IS NULL) THEN
                        v_testscore_rec.COURSE_OFFERINGS_KEY := 0;
                    ELSE
                        BEGIN 
                            SELECT COURSE_OFFERINGS_KEY
                            INTO v_testscore_rec.COURSE_OFFERINGS_KEY
                            FROM K12INTEL_DW.DTBL_COURSE_OFFERINGS a
                            WHERE 1=1
                                AND a.COURSE_KEY = v_testscore_rec.COURSE_KEY
                                AND a.COURSE_SECTION = v_admin_rec.COURSE_SECTION
                                AND v_test_admin_date BETWEEN a.COURSE_SECTION_START_DATE AND a.COURSE_SECTION_END_DATE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.COURSE_OFFERINGS_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'COURSE_OFFERINGS_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.COURSE_OFFERINGS_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'COURSE_OFFERINGS_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.COURSE_OFFERINGS_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'COURSE_OFFERINGS_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;




                ---------------------------------------------------------------
                -- STUDENT_SCHEDULES_KEY
                ---------------------------------------------------------------
                BEGIN
                    
                    IF (v_testscore_rec.STUDENT_SCHEDULES_KEY = 0 OR v_testscore_rec.STUDENT_SCHEDULES_KEY = 0) THEN
                        v_testscore_rec.STUDENT_SCHEDULES_KEY := 0;
                    ELSE
                        BEGIN 
                            SELECT STUDENT_SCHEDULES_KEY
                            INTO v_testscore_rec.STUDENT_SCHEDULES_KEY
                            FROM K12INTEL_DW.FTBL_STUDENT_SCHEDULES a
                            WHERE 1=1
                                AND a.STUDENT_KEY = v_testscore_rec.STUDENT_KEY
                                AND a.COURSE_KEY = v_testscore_rec.COURSE_KEY
                                AND a.COURSE_OFFERINGS_KEY = v_testscore_rec.COURSE_OFFERINGS_KEY  
                                AND v_test_admin_date BETWEEN a.SCHEDULE_START_DATE AND a.SCHEDULE_END_DATE;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.STUDENT_SCHEDULES_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_SCHEDULES_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_testscore_rec.SYS_AUDIT_IND := 'Y';
                                v_testscore_rec.STUDENT_SCHEDULES_KEY := 0;
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_SCHEDULES_KEY',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_testscore_rec.SYS_AUDIT_IND := 'Y';
                        v_testscore_rec.STUDENT_SCHEDULES_KEY := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'STUDENT_SCHEDULES_KEY',
                            v_WAREHOUSE_KEY,
                            v_AUDIT_NATURAL_KEY,
                            'Untrapped Error',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;



                ----------------------------------------
                -- Lookup SCHOOL_ANNUAL_ATTRIBS_KEY
                ----------------------------------------
                BEGIN
                    IF v_ok_to_commit = TRUE THEN
                        IF v_testscore_rec.SCHOOL_KEY = 0 OR v_LOCAL_SCHOOL_YEAR IS NULL THEN
                            v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                        ELSE
                            SELECT SCHOOL_ANNUAL_ATTRIBS_KEY INTO v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY
                            FROM K12INTEL_DW.DTBL_SCHOOL_ANNUAL_ATTRIBS
                            WHERE SCHOOL_KEY = v_testscore_rec.SCHOOL_KEY
                                and SCHOOL_YEAR = v_local_school_year;
                        END IF;
                    END IF;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'SCHOOL_ANNUAL_ATTRIBS_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;
            END IF; -- end avoid looking up if student, school or date keys are invalid

            ----------------------------------------
            -- TEST_RECORD_TYPE
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_RECORD_TYPE := COALESCE(v_admin_rec.TEST_RECORD_TYPE,'--'); -- Used to be 'NA'
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_RECORD_TYPE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_RECORD_TYPE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNTRAPPED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;





            ----------------------------------------
            -- TEST_ADMIN_PERIOD
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_ADMIN_PERIOD := COALESCE(v_admin_rec.TEST_ADMIN_PERIOD,'--');
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_ADMIN_PERIOD := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_RECORD_TYPE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNTRAPPED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;






            ----------------------------------------
            -- TEST_STUDENT_GRADE
            ----------------------------------------
            BEGIN
                --v_testscore_rec.TEST_STUDENT_GRADE := NVL(v_admin_rec.TEST_STUDENT_GRADE, 'NA');
                IF (RTRIM(v_admin_rec.TEST_STUDENT_GRADE) IS NOT NULL) THEN
                    v_testscore_rec.TEST_STUDENT_GRADE := COALESCE(v_admin_rec.TEST_STUDENT_GRADE, '--');
                ELSE    
                    BEGIN
                        SELECT enr.ENTRY_GRADE_CODE 
                        INTO v_testscore_rec.TEST_STUDENT_GRADE
                        FROM K12INTEL_DW.FTBL_ENROLLMENTS enr
                            INNER JOIN K12INTEL_DW.DTBL_SCHOOL_DATES bgdte
                                ON bgdte.SCHOOL_DATES_KEY = enr.SCHOOL_DATES_KEY_BEGIN_ENROLL
                            INNER JOIN K12INTEL_DW.DTBL_SCHOOL_DATES endte
                                ON endte.SCHOOL_DATES_KEY = enr.SCHOOL_DATES_KEY_END_ENROLL
                        WHERE 1=1
                            AND enr.STUDENT_KEY = v_testscore_rec.STUDENT_KEY
                            AND v_test_admin_date BETWEEN bgdte.DATE_VALUE AND endte.DATE_VALUE;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            v_test_admin_audit_ind := 'Y';
                            v_testscore_rec.TEST_STUDENT_GRADE := '@ERR';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_STUDENT_GRADE',
                                0,
                                v_AUDIT_NATURAL_KEY,
                                'NO_DATA_FOUND',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                        WHEN TOO_MANY_ROWS THEN
                            v_test_admin_audit_ind := 'Y';
                            v_testscore_rec.TEST_STUDENT_GRADE := '@ERR';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_STUDENT_GRADE',
                                0,
                                v_AUDIT_NATURAL_KEY,
                                'TOO_MANY_ROWS',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_STUDENT_GRADE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_STUDENT_GRADE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNTRAPPED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- TEST_TEACHER
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_TEACHER := TRIM(v_admin_rec.TEST_TEACHER);
/*
                v_testscore_rec.TEST_TEACHER := 
                    CASE
                        WHEN (COALESCE(RTRIM(v_admin_rec.TEST_TEACHER),'') = '') THEN NULL
                        ELSE TRIM(v_admin_rec.TEST_TEACHER)
                    END;
*/

            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_TEACHER := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_TEACHER',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNTRAPPED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- TEST_ADMIN_CODE
            ----------------------------------------
            BEGIN
                IF v_admin_rec.PROD_TEST_ID IS NOT NULL THEN
                    v_testscore_rec.TEST_ADMIN_CODE := SUBSTR(v_admin_rec.PROD_TEST_ID,1,200);
                ELSE
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_ADMIN_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_ADMIN_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'NULL TEST_ADMIN_CODE not allowed',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_ADMIN_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_ADMIN_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- PRIMARY_EXEMPTION_CODE
            ----------------------------------------
            BEGIN
                v_testscore_rec.PRIMARY_EXEMPTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_CODE, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.PRIMARY_EXEMPTION_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_EXEMPTION_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- PRIMARY_EXEMPTION
            ----------------------------------------
            BEGIN
                v_testscore_rec.PRIMARY_EXEMPTION := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_DESC, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.PRIMARY_EXEMPTION := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'PRIMARY_EXEMPTION',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- SECONDARY_EXEMPTION_CODE
            ----------------------------------------
            BEGIN
                v_testscore_rec.SECONDARY_EXEMPTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_CODE_2, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.SECONDARY_EXEMPTION_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_EXEMPTION_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- SECONDARY_EXEMPTION
            ----------------------------------------
            BEGIN
                v_testscore_rec.SECONDARY_EXEMPTION := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_DESC_2, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.SECONDARY_EXEMPTION := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_EXEMPTION',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- TERTIARY_EXEMPTION_CODE
            ----------------------------------------
            BEGIN
                v_testscore_rec.TERTIARY_EXEMPTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_CODE_3, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TERTIARY_EXEMPTION_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_EXEMPTION_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- TERTIARY_EXEMPTION
            ----------------------------------------
            BEGIN
                v_testscore_rec.TERTIARY_EXEMPTION := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_DESC_3, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TERTIARY_EXEMPTION := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_EXEMPTION',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- QUATERNARY_EXEMPTION_CODE
            ----------------------------------------
            BEGIN
                v_testscore_rec.QUATERNARY_EXEMPTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_CODE_4, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.QUATERNARY_EXEMPTION_CODE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_EXEMPTION_CODE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- QUATERNARY_EXEMPTION
            ----------------------------------------
            BEGIN
                v_testscore_rec.QUATERNARY_EXEMPTION := RTRIM(COALESCE(v_admin_rec.TEST_EXEMPTION_DESC_4, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.QUATERNARY_EXEMPTION := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_EXEMPTION',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;



            ----------------------------------------
            -- TEST_INTERVENTION_TYPE
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_INTERVENTION_TYPE := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_CODE, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_INTERVENTION_TYPE := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_INTERVENTION_TYPE',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ----------------------------------------
            -- TEST_INTERVENTION_DESC
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_INTERVENTION_DESC := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_DESC, '--'));
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_INTERVENTION_DESC := '@ERR';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_INTERVENTION_DESC',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;

            ---------------------------------------------------------------
            -- SECONDARY_INTERVENTION_CODE
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.SECONDARY_INTERVENTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_CODE_2, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ---------------------------------------------------------------
            -- SECONDARY_INTERVENTION
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.SECONDARY_INTERVENTION := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_DESC_2, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.SECONDARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'SECONDARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ---------------------------------------------------------------
            -- TERTIARY_INTERVENTION_CODE
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.TERTIARY_INTERVENTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_CODE_3, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ---------------------------------------------------------------
            -- TERTIARY_INTERVENTION
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.TERTIARY_INTERVENTION := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_DESC_3, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.TERTIARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TERTIARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ---------------------------------------------------------------
            -- QUATERNARY_INTERVENTION_CODE
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.QUATERNARY_INTERVENTION_CODE := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_CODE_4, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION_CODE := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;


            ---------------------------------------------------------------
            -- QUATERNARY_INTERVENTION
            ---------------------------------------------------------------
            BEGIN
                v_testscore_rec.QUATERNARY_INTERVENTION := RTRIM(COALESCE(v_admin_rec.TEST_INTERVENTION_DESC_4, '--'));
            EXCEPTION
                /*WHEN NO_DATA_FOUND THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                WHEN TOO_MANY_ROWS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;

                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );*/
                WHEN OTHERS THEN
                    v_testscore_rec.SYS_AUDIT_IND := 'Y';
                    v_testscore_rec.QUATERNARY_INTERVENTION := '@ERR';
                    v_WAREHOUSE_KEY := 0;
                    v_AUDIT_BASE_SEVERITY := 0;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'QUATERNARY_INTERVENTION',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;



            

            ----------------------------------------
            -- TEST_HIGHEST_SCORE_INDICATOR
            ----------------------------------------
            BEGIN
                v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR :=
                    CASE
                        WHEN RTRIM(v_admin_rec.HIGHEST_SCORE_IND) IS NULL THEN 'NA'
                        WHEN UPPER(v_admin_rec.HIGHEST_SCORE_IND) = 'N' THEN 'No'
                        WHEN UPPER(v_admin_rec.HIGHEST_SCORE_IND) = 'Y' THEN 'Yes'
                        ELSE '@ERR'
                    END;

                IF v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR = '@ERR' THEN
                    v_test_admin_audit_ind := 'Y';
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'HIGHEST_SCORE_IND='  || v_admin_rec.HIGHEST_SCORE_IND;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_HIGHEST_SCORE_INDICATOR',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'Highest score indicator does not begin with a N or Y.',
                        sqlerrm,
                        'N',
                        v_AUDIT_BASE_SEVERITY
                    );
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_test_admin_audit_ind := 'Y';
                    v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR := NULL;
                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                        v_SYS_ETL_SOURCE,
                        'TEST_HIGHEST_SCORE_INDICATOR',
                        0,
                        v_AUDIT_NATURAL_KEY,
                        'UNHANDLED ERROR',
                        sqlerrm,
                        'Y',
                        v_AUDIT_BASE_SEVERITY
                    );
            END;



            FOR v_xtbl_testscore_rec IN
            (
                SELECT
                    TEST_NUMBER,
                    TEST_CURRICULUM_CODE,
                    TEST_PASSED_INDICATOR,
                    TEST_CUSTOM_PASS_IND,
                    TEST_PRIMARY_RESULT_CODE,
                    TEST_PRIMARY_RESULT,
                    TEST_SECONDARY_RESULT_CODE,
                    TEST_SECONDARY_RESULT,
                    TEST_TERTIARY_RESULT_CODE,
                    TEST_TERTIARY_RESULT,
                    TEST_QUATERNARY_RESULT_CODE,
                    TEST_QUATERNARY_RESULT,
                    TEST_CUSTOM_RESULT_CODE,
                    TEST_CUSTOM_RESULT,
                    TEST_SCORE_TO_PREDICTED_RESULT,
                    TEST_ITEMS_POSSIBLE,
                    TEST_ITEMS_ATTEMPTED,
                    TEST_SCORE_VALUE,
                    TEST_RAW_SCORE,
                    TEST_SCALED_SCORE,
                    TEST_PREDICTED_SCORE,
                    TEST_LOWER_BOUND,
                    TEST_UPPER_BOUND,
                    TEST_NCE_SCORE,
                    TEST_PERCENTAGE_SCORE,
                    TEST_PERCENTILE_SCORE,
                    TEST_GRADE_EQUIVALENT,
                    TEST_READING_LEVEL,
                    TEST_SCHOOL_ABILITY_INDEX,
                    TEST_GROWTH_PERCENTILE,
                    TEST_GROWTH_RESULT_CODE,
                    TEST_GROWTH_RESULT,
                    TEST_GROWTH_TARGET_1,
                    TEST_GROWTH_TARGET_2,
                    TEST_GROWTH_TARGET_3,
                    TEST_GROWTH_TARGET_4,
                    TEST_STANDARD_ERROR,
                    TEST_Z_SCORE,
                    TEST_T_SCORE,
                    TEST_STANINE_SCORE,
                    TEST_QUARTILE_SCORE,
                    TEST_DECILE_SCORE,
                    TEST_SCORE_ATTRIBUTES,
                    TEST_SCORE_TEXT,
                    BENCHMARK_SCOPE_VALUE_1,
                    BENCHMARK_SCOPE_VALUE_2,
                    SYS_CREATED,
                    SYS_AUDIT_IND
                FROM K12INTEL_USERDATA.XTBL_TEST_SCORES
                WHERE TEST_ADMIN_KEY = v_admin_rec.TEST_ADMIN_KEY
                    AND DELETE_TEST_SCORE_IND = 'N'
            ) LOOP
                v_testscore_rec.sys_audit_ind := v_test_admin_audit_ind;
                v_testscore_rec.TEST_SCORES_KEY := 0;
                --v_BASE_NATURALKEY_TXT := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                ----------------------------------------
                -- Lookup TESTS_KEY
                ----------------------------------------
                BEGIN
                    SELECT /*+ INDEX(DTBL_TESTS VFIDXDTST_NUMKEY) */
                        TESTS_KEY
                        , D.TEST_SUBJECT
                        , X.ETL_CUST_BNCH_TYPE
                        , X.ETL_CUST_BNCH_TEST_NUMBER
                        , X.ETL_CUST_BNCH_ADMIN_PERIOD
                        , X.ETL_CUST_BNCH_SUBJECT
                        , X.ETL_CUST_BNCH_GRADE_GROUP
                        , X.ETL_CUST_BNCH_MEASURE
                    INTO v_testscore_rec.TESTS_KEY
                        , v_TESTS_SUBJECT 
                        , v_ETL_CUST_BNCH_TYPE 
                        , v_ETL_CUST_BNCH_TEST_NUMBER 
                        , v_ETL_CUST_BNCH_ADMIN_PERIOD 
                        , v_ETL_CUST_BNCH_SUBJECT 
                        , v_ETL_CUST_BNCH_GRADE_GROUP 
                        , v_ETL_CUST_BNCH_MEASURE 
                    FROM K12INTEL_DW.DTBL_TESTS D
                        INNER JOIN K12INTEL_USERDATA.XTBL_TESTS X 
                            ON D.TEST_NUMBER = X.TEST_NUMBER
                    WHERE rownum = 1 -- Top 1
                        AND d.TEST_NUMBER = v_xtbl_testscore_rec.TEST_NUMBER
                      ORDER BY 
                      CASE d.DISTRICT_CODE
                          WHEN '[ALL]' THEN 1
                          ELSE 0
                      END ASC;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_ok_to_commit := FALSE;
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.TESTS_KEY := 0;
                        v_TESTS_SUBJECT := null;
                        v_ETL_CUST_BNCH_TYPE := null;
                        v_ETL_CUST_BNCH_TEST_NUMBER := null;
                        v_ETL_CUST_BNCH_ADMIN_PERIOD := null;
                        v_ETL_CUST_BNCH_SUBJECT := null;
                        v_ETL_CUST_BNCH_GRADE_GROUP := null;
                        v_ETL_CUST_BNCH_MEASURE := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'TEST_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'NO_DATA_FOUND',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN TOO_MANY_ROWS THEN
                        v_ok_to_commit := FALSE;
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.TESTS_KEY := 0;
                        v_TESTS_SUBJECT := null;
                        v_ETL_CUST_BNCH_TYPE := null;
                        v_ETL_CUST_BNCH_TEST_NUMBER := null;
                        v_ETL_CUST_BNCH_ADMIN_PERIOD := null;
                        v_ETL_CUST_BNCH_SUBJECT := null;
                        v_ETL_CUST_BNCH_GRADE_GROUP := null;
                        v_ETL_CUST_BNCH_MEASURE := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'TEST_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'TOO_MANY_ROWS',
                            sqlerrm,
                            'N',
                            v_AUDIT_BASE_SEVERITY
                        );
                    WHEN OTHERS THEN
                        v_ok_to_commit := FALSE;
                        v_testscore_rec.sys_audit_ind := 'Y';
                        v_testscore_rec.TESTS_KEY := 0;
                        v_TESTS_SUBJECT := null;
                        v_ETL_CUST_BNCH_TYPE := null;
                        v_ETL_CUST_BNCH_TEST_NUMBER := null;
                        v_ETL_CUST_BNCH_ADMIN_PERIOD := null;
                        v_ETL_CUST_BNCH_SUBJECT := null;
                        v_ETL_CUST_BNCH_GRADE_GROUP := null;
                        v_ETL_CUST_BNCH_MEASURE := null;
                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                            v_SYS_ETL_SOURCE,
                            'TEST_KEY',
                            0,
                            v_AUDIT_NATURAL_KEY,
                            'UNTRAPPED ERROR',
                            sqlerrm,
                            'Y',
                            v_AUDIT_BASE_SEVERITY
                        );
                END;


                ----------------------------------------
                -- Generate TEST_SCORES_KEY
                ----------------------------------------
                IF (v_ok_to_commit = TRUE) THEN
                    BEGIN
                        K12INTEL_METADATA.GEN_TESTSCORE_KEY
                        (
                         v_admin_rec.PROD_TEST_ID,   --p_PROD_TESTSCORE_KEY  IN  VARCHAR2,
                         v_xtbl_testscore_rec.TEST_NUMBER,              --p_TEST_NUMBER   IN  VARCHAR2,
                         v_testscore_rec.TEST_SCORES_KEY            --p_TESTSCORE_KEY   OUT NUMBER
                        );

                        IF (v_testscore_rec.TEST_SCORES_KEY = 0) THEN
                            v_ok_to_commit := FALSE;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_SCORES_KEY := NULL;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'GEN_TESTSCORE_KEY',
                                0,
                                v_AUDIT_NATURAL_KEY,
                                'FAILED TO GENERATE A TESTSCORE KEY',
                                sqlerrm,
                                'N',
                                v_AUDIT_BASE_SEVERITY
                            );
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_ok_to_commit := FALSE;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_SCORES_KEY := NULL;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'GEN_TESTSCORE_KEY',
                                0,
                                v_AUDIT_NATURAL_KEY,
                                'FAILED TO GENERATE A TESTSCORE KEY',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;
                END IF;


                IF (v_ok_to_commit = TRUE) THEN
                
                    ----------------------------------------
                    -- CURRICULUM_KEY
                    ----------------------------------------
                    BEGIN

                        -- KB_NOTE: Convert XTBL_CURRICULUM Items
                        v_testscore_rec.CURRICULUM_KEY := 0;
/*
                        IF (RTRIM(v_xtbl_testscore_rec.TEST_CURRICULUM_CODE) IS NULL) THEN 
                            v_testscore_rec.CURRICULUM_KEY := 0;
                        ELSE
                            BEGIN
                                SELECT CURRICULUM_KEY
                                INTO v_testscore_rec.CURRICULUM_KEY
                                FROM K12INTEL_KEYMAP.KM_CURRICULUM_XTBL
                                WHERE 1=1
                                    AND CURRICULUM_UUID = v_xtbl_testscore_rec.TEST_CURRICULUM_CODE;
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    BEGIN
                                        SELECT CURRICULUM_KEY
                                        INTO v_testscore_rec.CURRICULUM_KEY
                                        FROM K12INTEL_DW.DTBL_CURRICULUM
                                        WHERE 1=1
                                            AND CURRICULUM_CODE = v_xtbl_testscore_rec.TEST_CURRICULUM_CODE;
                                    EXCEPTION
                                        WHEN NO_DATA_FOUND THEN
                                            v_testscore_rec.CURRICULUM_KEY := 0;
                                            v_testscore_rec.sys_audit_ind := 'Y';
                                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                v_SYS_ETL_SOURCE,
                                                'CURRICULUM_KEY - CURRICULUM CODE',
                                                v_testscore_rec.TEST_SCORES_KEY,
                                                v_AUDIT_NATURAL_KEY,
                                                'NO_DATA_FOUND',
                                                sqlerrm,
                                                'Y',
                                                v_AUDIT_BASE_SEVERITY
                                            );
                                        WHEN TOO_MANY_ROWS THEN
                                            v_testscore_rec.CURRICULUM_KEY := 0;
                                            v_testscore_rec.sys_audit_ind := 'Y';
                                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                v_SYS_ETL_SOURCE,
                                                'CURRICULUM_KEY - CURRICULUM CODE',
                                                v_testscore_rec.TEST_SCORES_KEY,
                                                v_AUDIT_NATURAL_KEY,
                                                'TOO_MANY_ROWS',
                                                sqlerrm,
                                                'Y',
                                                v_AUDIT_BASE_SEVERITY
                                            );
                                        WHEN OTHERS THEN
                                            v_testscore_rec.CURRICULUM_KEY := 0;
                                            v_testscore_rec.sys_audit_ind := 'Y';
                                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                v_SYS_ETL_SOURCE,
                                                'CURRICULUM_KEY - CURRICULUM CODE',
                                                v_testscore_rec.TEST_SCORES_KEY,
                                                v_AUDIT_NATURAL_KEY,
                                                'UNHANDLED ERROR',
                                                sqlerrm,
                                                'Y',
                                                v_AUDIT_BASE_SEVERITY
                                            );
                                    END;
                                WHEN TOO_MANY_ROWS THEN
                                    v_testscore_rec.CURRICULUM_KEY := 0;
                                    v_testscore_rec.sys_audit_ind := 'Y';
                                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                        v_SYS_ETL_SOURCE,
                                        'CURRICULUM_KEY - CURRICULUM UUID',
                                        v_testscore_rec.TEST_SCORES_KEY,
                                        v_AUDIT_NATURAL_KEY,
                                        'TOO_MANY_ROWS',
                                        sqlerrm,
                                        'Y',
                                        v_AUDIT_BASE_SEVERITY
                                    );
                                WHEN OTHERS THEN
                                    v_testscore_rec.CURRICULUM_KEY := 0;
                                    v_testscore_rec.sys_audit_ind := 'Y';
                                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                        v_SYS_ETL_SOURCE,
                                        'CURRICULUM_KEY - CURRICULUM UUID',
                                        v_testscore_rec.TEST_SCORES_KEY,
                                        v_AUDIT_NATURAL_KEY,
                                        'UNHANDLED ERROR',
                                        sqlerrm,
                                        'Y',
                                        v_AUDIT_BASE_SEVERITY
                                    );
                            END;
                        END IF;
*/
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.CURRICULUM_KEY := 0;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'CURRICULUM_KEY',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'UNHANDLED ERROR',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;


                
                    ----------------------------------------
                    -- TEST_ITEMS_POSSIBLE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_ITEMS_POSSIBLE := v_xtbl_testscore_rec.TEST_ITEMS_POSSIBLE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_ITEMS_POSSIBLE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_ITEMS_POSSIBLE',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'UNHANDLED ERROR',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;


                
                    ----------------------------------------
                    -- TEST_ITEMS_ATTEMPTED
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_ITEMS_ATTEMPTED := v_xtbl_testscore_rec.TEST_ITEMS_ATTEMPTED;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_ITEMS_ATTEMPTED := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_ITEMS_ATTEMPTED',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'UNHANDLED ERROR',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;

                    ----------------------------------------
                    -- TEST_SCORE_VALUE
                    ----------------------------------------
                    BEGIN
                        IF IS_NUMBER(v_xtbl_testscore_rec.TEST_SCORE_VALUE) = 1 THEN
                            v_testscore_rec.TEST_SCORE_VALUE := v_xtbl_testscore_rec.TEST_SCORE_VALUE;
                        ELSE
                            v_testscore_rec.TEST_SCORE_VALUE := TO_NUMBER(regxReplace(v_xtbl_testscore_rec.TEST_SCORE_VALUE,'[^0-9]', ' '));
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCORE_VALUE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_SCORE_VALUE',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'TEST SCORE COULD NOT BE CONVERTED TO A NUMBER',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;




                    ----------------------------------------
                    -- TEST_BENCHMARKS_KEY
                    ----------------------------------------
                    BEGIN
                        SELECT /*+ INDEX(DTBL_TEST_BENCHMARKS VFIDXDTBEN_RANGELOOKUP) */
                            TEST_BENCHMARK_KEY, PASSING_INDICATOR
                        INTO v_testscore_rec.TEST_BENCHMARKS_KEY, v_local_passing_indicator
                        FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS
                        WHERE TESTS_KEY = v_testscore_rec.TESTS_KEY
                            AND TEST_BENCHMARK_TYPE = 'Result_Code'
                            AND v_testscore_rec.TEST_SCORE_VALUE BETWEEN MIN_VALUE AND MAX_VALUE
                            AND v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                            AND sys_delete_ind = 'N';
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            v_testscore_rec.TEST_BENCHMARKS_KEY := 0;
                            v_local_passing_indicator := NULL;
                            -- NOT AUDITED
                        WHEN TOO_MANY_ROWS THEN
                            --v_ok_to_commit := FALSE;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_BENCHMARKS_KEY := 0;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_BENCHMARKS_KEY',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'TOO_MANY_ROWS',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                        WHEN OTHERS THEN
                            --v_ok_to_commit := FALSE;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_BENCHMARKS_KEY := 0;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_BENCHMARKS_KEY',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'Lookup into DTBL_TEST_BENCHMARKS failed.',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;



                    ----------------------------------------
                    -- VARIABLE: LOCAL_CUSTOM_BENCHMARK_VARS
                    ----------------------------------------
                    --TODO:  MAP TO OTHER SCORE VALUE FIELDS?
                    v_LOCAL_SCORE_VALUE := 
                        CASE 
                            WHEN v_ETL_CUST_BNCH_MEASURE = 'TEST_RAW_SCORE' THEN v_xtbl_testscore_rec.TEST_RAW_SCORE
                            WHEN v_ETL_CUST_BNCH_MEASURE = 'TEST_SCALED_SCORE' THEN v_xtbl_testscore_rec.TEST_SCALED_SCORE
                        ELSE v_xtbl_testscore_rec.TEST_SCORE_VALUE
                        END;
                    v_LOCAL_TESTS_KEY := CASE WHEN v_ETL_CUST_BNCH_TEST_NUMBER = 'Y' THEN v_testscore_rec.TESTS_KEY ELSE -1 END;
                    v_LOCAL_TEST_ADMIN_PERIOD := CASE WHEN v_ETL_CUST_BNCH_ADMIN_PERIOD = 'Y' THEN v_testscore_rec.TEST_ADMIN_PERIOD ELSE '--' END;
                    v_LOCAL_TEST_SUBJECT := CASE WHEN v_ETL_CUST_BNCH_SUBJECT = 'Y' THEN v_TESTS_SUBJECT ELSE '--' END;
                    v_LOCAL_TEST_GRADE_GROUP := CASE WHEN v_ETL_CUST_BNCH_GRADE_GROUP = 'Y' THEN v_testscore_rec.TEST_STUDENT_GRADE ELSE '--' END;


                    ----------------------------------------
                    -- CUSTOM_BENCHMARK_KEY
                    ----------------------------------------
                    BEGIN
                        --v_testscore_rec.CUSTOM_BENCHMARK_KEY := 0;
                        IF (
                            v_ETL_CUST_BNCH_TEST_NUMBER = 'N'
                            AND v_ETL_CUST_BNCH_ADMIN_PERIOD = 'N'
                            AND v_ETL_CUST_BNCH_SUBJECT = 'N'
                            AND v_ETL_CUST_BNCH_GRADE_GROUP = 'N'
                        ) THEN
                            v_testscore_rec.CUSTOM_BENCHMARK_KEY := 0;
                            v_LOCAL_CUST_PASS_IND := NULL;
                        ELSE
                            BEGIN
                                SELECT TEST_BENCHMARK_KEY, PASSING_INDICATOR
                                INTO v_testscore_rec.CUSTOM_BENCHMARK_KEY, v_LOCAL_CUST_PASS_IND
                                FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS 
                                WHERE v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                                    AND sys_delete_ind = 'N'
                                    AND TESTS_KEY = v_LOCAL_TESTS_KEY
                                    AND TEST_ADMIN_PERIOD = v_LOCAL_TEST_ADMIN_PERIOD
                                    AND TEST_SUBJECT = v_LOCAL_TEST_SUBJECT
                                    AND TEST_GRADE_GROUP = v_LOCAL_TEST_GRADE_GROUP
                                    AND TEST_BENCHMARK_TYPE = v_ETL_CUST_BNCH_TYPE
                                    AND v_LOCAL_SCORE_VALUE BETWEEN MIN_VALUE AND MAX_VALUE;
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    --v_ok_to_commit := FALSE;
                                    v_testscore_rec.sys_audit_ind := 'Y';
                                    v_testscore_rec.CUSTOM_BENCHMARK_KEY := 0;
                                    v_LOCAL_CUST_PASS_IND := NULL;
                                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                        v_SYS_ETL_SOURCE,
                                        'CUSTOM_BENCHMARK_KEY',
                                        v_testscore_rec.TEST_SCORES_KEY,
                                        v_AUDIT_NATURAL_KEY,
                                        'NO_DATA_FOUND',
                                        sqlerrm,
                                        'Y',
                                        v_AUDIT_BASE_SEVERITY
                                    );
                                WHEN TOO_MANY_ROWS THEN
                                    --v_ok_to_commit := FALSE;
                                    v_testscore_rec.sys_audit_ind := 'Y';
                                    v_testscore_rec.CUSTOM_BENCHMARK_KEY := 0;
                                    v_LOCAL_CUST_PASS_IND := NULL;
                                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                        v_SYS_ETL_SOURCE,
                                        'CUSTOM_BENCHMARK_KEY',
                                        v_testscore_rec.TEST_SCORES_KEY,
                                        v_AUDIT_NATURAL_KEY,
                                        'TOO_MANY_ROWS',
                                        sqlerrm,
                                        'Y',
                                        v_AUDIT_BASE_SEVERITY
                                    );
                            END;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            --v_ok_to_commit := FALSE;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.CUSTOM_BENCHMARK_KEY := 0;
                            v_LOCAL_CUST_PASS_IND := NULL;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'CUSTOM_BENCHMARK_KEY',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'Lookup into DTBL_TEST_BENCHMARKS failed.',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;


                    ----------------------------------------
                    -- TEST_PRIMARY_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_PRIMARY_RESULT_CODE IS NOT NULL THEN
                            v_testscore_rec.TEST_PRIMARY_RESULT_CODE := v_xtbl_testscore_rec.TEST_PRIMARY_RESULT_CODE;
                        ELSE
                            -- Derive primary result code from test score
                            BEGIN
                                SELECT /*+ INDEX(DTBL_TEST_BENCHMARKS VFIDXDTBEN_RANGELOOKUP3) */
                                    TRIM(TEST_BENCHMARK_CODE)
                                INTO v_testscore_rec.TEST_PRIMARY_RESULT_CODE
                                FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS
                                WHERE TESTS_KEY = v_testscore_rec.TESTS_KEY
                                    AND TEST_BENCHMARK_TYPE = 'Result_Code'
                                    AND v_testscore_rec.TEST_SCORE_VALUE BETWEEN MIN_VALUE AND MAX_VALUE
                                    AND v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                                    AND sys_delete_ind = 'N';
                            EXCEPTION
                                WHEN NO_DATA_FOUND THEN
                                    v_testscore_rec.TEST_PRIMARY_RESULT_CODE := '--';
                                WHEN TOO_MANY_ROWS THEN
                                    v_testscore_rec.sys_audit_ind := 'Y';
                                    v_testscore_rec.TEST_PRIMARY_RESULT_CODE := '@ERR';
                                    v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                    K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                        v_SYS_ETL_SOURCE,
                                        'TEST_PRIMARY_RESULT_CODE',
                                        v_testscore_rec.TEST_SCORES_KEY,
                                        v_AUDIT_NATURAL_KEY,
                                        'TOO_MANY_ROWS',
                                        sqlerrm,
                                        'N',
                                        v_AUDIT_BASE_SEVERITY
                                    );
                            END;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_PRIMARY_RESULT_CODE := '@ERR';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_PRIMARY_RESULT_CODE',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'UNHANDLED ERROR',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;






                    --------------------------------------------------
                    -- VARIABLE: TEST_PRIMARY_RESULT
                    --------------------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_PRIMARY_RESULT IS NOT NULL THEN
                            v_testscore_rec.TEST_PRIMARY_RESULT := v_xtbl_testscore_rec.TEST_PRIMARY_RESULT;
                        ELSE
                            IF v_xtbl_testscore_rec.TEST_PRIMARY_RESULT_CODE IS NOT NULL THEN
                                BEGIN
                                    SELECT TRIM(DOMAIN_DECODE)
                                    INTO v_testscore_rec.TEST_PRIMARY_RESULT
                                    FROM
                                    (
                                        SELECT /*+ INDEX(DTBL_TESTS VFIDXDTST_NUMVENDOR) */
                                            DOMAIN_DECODE
                                        FROM K12INTEL_USERDATA.XTBL_DOMAIN_DECODES A
                                           INNER JOIN K12INTEL_DW.DTBL_TESTS B
                                           ON (A.DOMAIN_ALTERNATE_DECODE = B.TEST_VENDOR)
                                        WHERE
                                            B.TEST_NUMBER = v_xtbl_testscore_rec.TEST_NUMBER AND
                                            DOMAIN_NAME = 'Result_Code' and
                                            DOMAIN_CODE = v_testscore_rec.TEST_PRIMARY_RESULT_CODE AND
                                            DOMAIN_SCOPE IN ('[ALL]',v_testscore_rec.DISTRICT_CODE)
                                        ORDER BY
                                            CASE DOMAIN_SCOPE
                                                WHEN '[ALL]' THEN 1
                                                ELSE 0
                                            END ASC
                                    )
                                    WHERE rownum = 1;
                                EXCEPTION
                                    WHEN NO_DATA_FOUND THEN
                                        -- Lookup into benchmarks using result code
                                        BEGIN
                                            SELECT /*+ INDEX(DTBL_TEST_BENCHMARKS VFIDXDTBEN_RANGELOOKUP3) */
                                                TRIM(TEST_BENCHMARK_NAME)
                                            INTO v_testscore_rec.TEST_PRIMARY_RESULT
                                            FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS
                                            WHERE TESTS_KEY = v_testscore_rec.TESTS_KEY
                                                AND TEST_BENCHMARK_TYPE = 'Result_Code'
                                                AND v_testscore_rec.TEST_PRIMARY_RESULT_CODE = TEST_BENCHMARK_CODE
                                                AND v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                                                AND sys_delete_ind = 'N';
                                        EXCEPTION
                                            WHEN NO_DATA_FOUND THEN
                                                v_testscore_rec.TEST_PRIMARY_RESULT := 'Not Specified';
                                            WHEN TOO_MANY_ROWS THEN
                                                v_testscore_rec.sys_audit_ind := 'Y';
                                                v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
                                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                                    v_SYS_ETL_SOURCE,
                                                    'TEST_PRIMARY_RESULT',
                                                    v_testscore_rec.TEST_SCORES_KEY,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'TOO_MANY_ROWS',
                                                    sqlerrm,
                                                    'N',
                                                    v_AUDIT_BASE_SEVERITY
                                                );
                                        END;
                                    WHEN OTHERS THEN
                                        v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
                                        v_testscore_rec.sys_audit_ind := 'Y';
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'TEST_PRIMARY_RESULT',
                                            v_testscore_rec.TEST_SCORES_KEY,
                                            v_AUDIT_NATURAL_KEY,
                                            'UNHANDLED ERROR',
                                            sqlerrm,
                                            'Y',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                END;
                            ELSE
                                -- Lookup into benchmarks using test score
                                BEGIN
                                    SELECT /*+ INDEX(DTBL_TEST_BENCHMARKS VFIDXDTBEN_RANGELOOKUP3) */
                                        TRIM(TEST_BENCHMARK_NAME)
                                    INTO v_testscore_rec.TEST_PRIMARY_RESULT
                                    FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS
                                    WHERE TESTS_KEY = v_testscore_rec.TESTS_KEY
                                        AND TEST_BENCHMARK_TYPE = 'Result_Code'
                                        AND v_testscore_rec.TEST_SCORE_VALUE BETWEEN MIN_VALUE AND MAX_VALUE
                                        AND v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                                        AND sys_delete_ind = 'N';
                                EXCEPTION
                                    WHEN NO_DATA_FOUND THEN
                                        v_testscore_rec.TEST_PRIMARY_RESULT := 'Not Specified';

--                                        v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
--                                        v_testscore_rec.sys_audit_ind := 'Y';
--                                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
--                                            0,
--                                            'BUILD_FTBL_TEST_SCORES_XTBL',
--                                            'TEST_PRIMARY_RESULT - BENCHMARK LOOKUP USING TEST_SCORE_VALUE',
--                                            v_audit_msg,
--                                            'NO_DATA_FOUND',
--                                            sqlerrm,
--                                            'N'
--                                        );

                                    WHEN TOO_MANY_ROWS THEN
                                        v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
                                        v_testscore_rec.sys_audit_ind := 'Y';
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'TEST_PRIMARY_RESULT - BENCHMARK LOOKUP USING TEST_SCORE_VALUE',
                                            v_testscore_rec.TEST_SCORES_KEY,
                                            v_AUDIT_NATURAL_KEY,
                                            'TOO_MANY_ROWS',
                                            sqlerrm,
                                            'N',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                    WHEN OTHERS THEN
                                        v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
                                        v_testscore_rec.sys_audit_ind := 'Y';
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'TEST_PRIMARY_RESULT - BENCHMARK LOOKUP USING TEST_SCORE_VALUE',
                                            v_testscore_rec.TEST_SCORES_KEY,
                                            v_AUDIT_NATURAL_KEY,
                                            'UNHANDLED ERROR',
                                            sqlerrm,
                                            'Y',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                END;
                            END IF;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_PRIMARY_RESULT := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_PRIMARY_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;






                    ----------------------------------------
                    -- TEST_SECONDARY_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_SECONDARY_RESULT_CODE IS NOT NULL THEN
                            v_testscore_rec.TEST_SECONDARY_RESULT_CODE := v_xtbl_testscore_rec.TEST_SECONDARY_RESULT_CODE;
                        ELSE
                           v_testscore_rec.TEST_SECONDARY_RESULT_CODE := '--';
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_SECONDARY_RESULT_CODE := '@ERR';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SECONDARY_RESULT_CODE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;





                    --------------------------------------------------
                    -- VARIABLE: TEST_SECONDARY_RESULT
                    --------------------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_SECONDARY_RESULT IS NOT NULL THEN
                            v_testscore_rec.TEST_SECONDARY_RESULT := v_xtbl_testscore_rec.TEST_SECONDARY_RESULT;
                        ELSE
                            v_testscore_rec.TEST_SECONDARY_RESULT := '--';                            
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SECONDARY_RESULT := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SECONDARY_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_TERTIARY_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_TERTIARY_RESULT_CODE IS NOT NULL THEN
                            v_testscore_rec.TEST_TERTIARY_RESULT_CODE := v_xtbl_testscore_rec.TEST_TERTIARY_RESULT_CODE;
                        ELSE
                            v_testscore_rec.TEST_TERTIARY_RESULT_CODE := '--';
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_TERTIARY_RESULT_CODE := '@ERR';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT  || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_TERTIARY_RESULT_CODE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;






                    --------------------------------------------------
                    -- VARIABLE: TEST_TERTIARY_RESULT
                    --------------------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_TERTIARY_RESULT IS NOT NULL THEN
                            v_testscore_rec.TEST_TERTIARY_RESULT := v_xtbl_testscore_rec.TEST_TERTIARY_RESULT;
                        ELSE
                            v_testscore_rec.TEST_TERTIARY_RESULT := '--';
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_TERTIARY_RESULT := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_TERTIARY_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_QUATERNARY_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_QUATERNARY_RESULT_CODE IS NOT NULL THEN
                            v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := v_xtbl_testscore_rec.TEST_QUATERNARY_RESULT_CODE;
                        ELSE
                            v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := '--';
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := '@ERR';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_QUATERNARY_RESULT_CODE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    --------------------------------------------------
                    -- VARIABLE: TEST_QUATERNARY_RESULT
                    --------------------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_QUATERNARY_RESULT IS NOT NULL THEN
                            v_testscore_rec.TEST_QUATERNARY_RESULT := v_xtbl_testscore_rec.TEST_QUATERNARY_RESULT;
                        ELSE
                            v_testscore_rec.TEST_QUATERNARY_RESULT := '--';
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_QUATERNARY_RESULT := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_QUATERNARY_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_CUSTOM_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        IF v_xtbl_testscore_rec.TEST_CUSTOM_RESULT_CODE IS NOT NULL THEN
                            v_testscore_rec.TEST_CUSTOM_RESULT_CODE := v_xtbl_testscore_rec.TEST_CUSTOM_RESULT_CODE;
                            v_testscore_rec.TEST_CUSTOM_RESULT := v_xtbl_testscore_rec.TEST_CUSTOM_RESULT;
                        ELSE
                            IF (
                                v_ETL_CUST_BNCH_TEST_NUMBER = 'N'
                                AND v_ETL_CUST_BNCH_ADMIN_PERIOD = 'N'
                                AND v_ETL_CUST_BNCH_SUBJECT = 'N'
                                AND v_ETL_CUST_BNCH_GRADE_GROUP = 'N'
                            ) THEN
                                v_testscore_rec.TEST_CUSTOM_RESULT_CODE := '--';
                                v_testscore_rec.TEST_CUSTOM_RESULT := '--';
                            ELSE
                                BEGIN
                                    SELECT TRIM(TEST_BENCHMARK_CODE), TRIM(TEST_BENCHMARK_NAME)
                                    INTO v_testscore_rec.TEST_CUSTOM_RESULT_CODE, v_testscore_rec.TEST_CUSTOM_RESULT
                                    FROM K12INTEL_DW.DTBL_TEST_BENCHMARKS
                                    WHERE v_test_admin_date BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
                                        AND sys_delete_ind = 'N'
                                        AND TESTS_KEY = v_LOCAL_TESTS_KEY
                                        AND TEST_ADMIN_PERIOD = v_LOCAL_TEST_ADMIN_PERIOD
                                        AND TEST_SUBJECT = v_LOCAL_TEST_SUBJECT
                                        AND TEST_GRADE_GROUP = v_LOCAL_TEST_GRADE_GROUP
                                        AND TEST_BENCHMARK_TYPE = v_ETL_CUST_BNCH_TYPE
                                        AND v_LOCAL_SCORE_VALUE BETWEEN MIN_VALUE AND MAX_VALUE;
                                EXCEPTION
                                    WHEN NO_DATA_FOUND THEN
                                        v_testscore_rec.sys_audit_ind := 'Y';
                                        v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := '--';
                                        v_testscore_rec.TEST_QUATERNARY_RESULT := '--';
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'TEST_CUSTOM_RESULT',
                                            v_testscore_rec.TEST_SCORES_KEY,
                                            v_AUDIT_NATURAL_KEY,
                                            'NO_DATA_FOUND',
                                            sqlerrm,
                                            'Y',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                    WHEN TOO_MANY_ROWS THEN
                                        v_testscore_rec.sys_audit_ind := 'Y';
                                        v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := '--';
                                        v_testscore_rec.TEST_QUATERNARY_RESULT := '--';
                                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                            v_SYS_ETL_SOURCE,
                                            'TEST_CUSTOM_RESULT',
                                            v_testscore_rec.TEST_SCORES_KEY,
                                            v_AUDIT_NATURAL_KEY,
                                            'TOO_MANY_ROWS',
                                            sqlerrm,
                                            'Y',
                                            v_AUDIT_BASE_SEVERITY
                                        );
                                END;
                            END IF;
                            
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_testscore_rec.TEST_QUATERNARY_RESULT_CODE := '--';
                            v_testscore_rec.TEST_QUATERNARY_RESULT := '--';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_CUSTOM_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_SCORE_TO_PREDICTED_RESULT
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT := RTRIM(COALESCE(v_xtbl_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT,'--'));
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SCORE_TO_PREDICTED_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    --------------------------------------------------
                    -- VARIABLE: TEST_PASSED_INDICATOR
                    --------------------------------------------------
                    BEGIN
                        IF (RTRIM(v_xtbl_testscore_rec.TEST_PASSED_INDICATOR) IS NOT NULL) THEN
                            v_testscore_rec.TEST_PASSED_INDICATOR := 
                                CASE
                                    WHEN UPPER(SUBSTR(v_xtbl_testscore_rec.TEST_PASSED_INDICATOR,1,1)) = 'Y' THEN 'Yes'
                                    WHEN UPPER(SUBSTR(v_xtbl_testscore_rec.TEST_PASSED_INDICATOR,1,1)) = 'N' THEN 'No'
                                    ELSE '--'
                                END;
                        ELSE
                            v_testscore_rec.TEST_PASSED_INDICATOR := 
                                CASE
                                    WHEN UPPER(SUBSTR(v_local_passing_indicator,1,1)) = 'Y' THEN 'Yes'
                                    WHEN UPPER(SUBSTR(v_local_passing_indicator,1,1)) = 'N' THEN 'No'
                                    ELSE '--'
                                END;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_PASSED_INDICATOR := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_PASSED_INDICATOR',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    --------------------------------------------------
                    -- VARIABLE: TEST_CUSTOM_PASS_IND
                    --------------------------------------------------
                    BEGIN
                        IF (RTRIM(v_xtbl_testscore_rec.TEST_CUSTOM_PASS_IND) IS NOT NULL) THEN
                            v_testscore_rec.TEST_CUSTOM_PASS_IND := 
                                CASE
                                    WHEN UPPER(SUBSTR(v_xtbl_testscore_rec.TEST_CUSTOM_PASS_IND,1,1)) = 'Y' THEN 'Yes'
                                    WHEN UPPER(SUBSTR(v_xtbl_testscore_rec.TEST_CUSTOM_PASS_IND,1,1)) = 'N' THEN 'No'
                                    ELSE '--'
                                END;
                        ELSE
                            v_testscore_rec.TEST_CUSTOM_PASS_IND := 
                                CASE
                                    WHEN UPPER(SUBSTR(v_LOCAL_CUST_PASS_IND,1,1)) = 'Y' THEN 'Yes'
                                    WHEN UPPER(SUBSTR(v_LOCAL_CUST_PASS_IND,1,1)) = 'N' THEN 'No'
                                    ELSE '--'
                                END;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_CUSTOM_PASS_IND := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_CUSTOM_PASS_IND',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_ATTEMPT_NUMBER (logic implemented below)
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_ATTEMPT_NUMBER := NULL;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_ATTEMPT_NUMBER := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_ATTEMPT_NUMBER',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_RAW_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_RAW_SCORE := v_xtbl_testscore_rec.TEST_RAW_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_RAW_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_RAW_SCORE',
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'UNHANDLED ERROR',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;




                    ----------------------------------------
                    -- TEST_SCALED_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_SCALED_SCORE := v_xtbl_testscore_rec.TEST_SCALED_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCALED_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SCALED_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_PREDICTED_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_PREDICTED_SCORE := v_xtbl_testscore_rec.TEST_PREDICTED_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_PREDICTED_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_PREDICTED_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ---------------------------------------------------------------
                    -- TEST_LOWER_BOUND
                    ---------------------------------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_LOWER_BOUND := v_xtbl_testscore_rec.TEST_LOWER_BOUND;
                    EXCEPTION
                        /*WHEN NO_DATA_FOUND THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_LOWER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;

                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_LOWER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'NO_DATA_FOUND',
                                sqlerrm,
                                'N',
                                v_AUDIT_BASE_SEVERITY
                            );
                        WHEN TOO_MANY_ROWS THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_LOWER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;

                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_LOWER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'TOO_MANY_ROWS',
                                sqlerrm,
                                'N',
                                v_AUDIT_BASE_SEVERITY
                            );*/
                        WHEN OTHERS THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_LOWER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_LOWER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'Untrapped Error',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;


                    ---------------------------------------------------------------
                    -- TEST_UPPER_BOUND
                    ---------------------------------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_UPPER_BOUND := v_xtbl_testscore_rec.TEST_UPPER_BOUND;
                    EXCEPTION
                        /*WHEN NO_DATA_FOUND THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_UPPER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;

                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_UPPER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'NO_DATA_FOUND',
                                sqlerrm,
                                'N',
                                v_AUDIT_BASE_SEVERITY
                            );
                        WHEN TOO_MANY_ROWS THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_UPPER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;

                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_UPPER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'TOO_MANY_ROWS',
                                sqlerrm,
                                'N',
                                v_AUDIT_BASE_SEVERITY
                            );*/
                        WHEN OTHERS THEN
                            v_testscore_rec.SYS_AUDIT_IND := 'Y';
                            v_testscore_rec.TEST_UPPER_BOUND := 0;
                            v_WAREHOUSE_KEY := 0;
                            v_AUDIT_BASE_SEVERITY := 0;
                            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

                            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                v_SYS_ETL_SOURCE,
                                'TEST_UPPER_BOUND',
                                v_WAREHOUSE_KEY,
                                v_AUDIT_NATURAL_KEY,
                                'Untrapped Error',
                                sqlerrm,
                                'Y',
                                v_AUDIT_BASE_SEVERITY
                            );
                    END;

                    ----------------------------------------
                    -- TEST_NCE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_NCE_SCORE := v_xtbl_testscore_rec.TEST_NCE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_NCE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_NCE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_PERCENTAGE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_PERCENTAGE_SCORE := v_xtbl_testscore_rec.TEST_PERCENTAGE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_PERCENTAGE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_PERCENTAGE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_PERCENTILE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_PERCENTILE_SCORE := v_xtbl_testscore_rec.TEST_PERCENTILE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_PERCENTILE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_PERCENTILE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;

                    ----------------------------------------
                    -- TEST_GRADE_EQUIVALENT
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GRADE_EQUIVALENT := v_xtbl_testscore_rec.TEST_GRADE_EQUIVALENT;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GRADE_EQUIVALENT := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GRADE_EQUIVALENT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_READING_LEVEL
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_READING_LEVEL := v_xtbl_testscore_rec.TEST_READING_LEVEL;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_READING_LEVEL := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_READING_LEVEL',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_SCHOOL_ABILITY_INDEX
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX := v_xtbl_testscore_rec.TEST_SCHOOL_ABILITY_INDEX;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SCHOOL_ABILITY_INDEX',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_GROWTH_PERCENTILE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_PERCENTILE := v_xtbl_testscore_rec.TEST_GROWTH_PERCENTILE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_PERCENTILE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_PERCENTILE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_GROWTH_RESULT_CODE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_RESULT_CODE := v_xtbl_testscore_rec.TEST_GROWTH_RESULT_CODE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_RESULT_CODE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_RESULT_CODE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_GROWTH_RESULT
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_RESULT := v_xtbl_testscore_rec.TEST_GROWTH_RESULT;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_RESULT := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_RESULT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_GROWTH_TARGET_1
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_TARGET_1 := v_xtbl_testscore_rec.TEST_GROWTH_TARGET_1;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_TARGET_1 := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_TARGET_1',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_GROWTH_TARGET_2
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_TARGET_2 := v_xtbl_testscore_rec.TEST_GROWTH_TARGET_2;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_TARGET_2 := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_TARGET_2',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;



                    ----------------------------------------
                    -- TEST_GROWTH_TARGET_3
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_TARGET_3 := v_xtbl_testscore_rec.TEST_GROWTH_TARGET_3;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_TARGET_3 := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_TARGET_3',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_GROWTH_TARGET_4
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_GROWTH_TARGET_4 := v_xtbl_testscore_rec.TEST_GROWTH_TARGET_4;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_GROWTH_TARGET_4 := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_GROWTH_TARGET_4',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    ----------------------------------------
                    -- TEST_STANDARD_ERROR
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_STANDARD_ERROR := v_xtbl_testscore_rec.TEST_STANDARD_ERROR;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_STANDARD_ERROR := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_STANDARD_ERROR',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;

                    ----------------------------------------
                    -- TEST_Z_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_Z_SCORE := v_xtbl_testscore_rec.TEST_Z_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_Z_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_Z_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;





                    ----------------------------------------
                    -- TEST_T_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_T_SCORE := v_xtbl_testscore_rec.TEST_T_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_T_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_T_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_STANINE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_STANINE_SCORE := v_xtbl_testscore_rec.TEST_STANINE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_STANINE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_STANINE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_QUARTILE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_QUARTILE_SCORE := v_xtbl_testscore_rec.TEST_QUARTILE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_QUARTILE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_QUARTILE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;




                    ----------------------------------------
                    -- TEST_DECILE_SCORE
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_DECILE_SCORE := v_xtbl_testscore_rec.TEST_DECILE_SCORE;
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_DECILE_SCORE := NULL;
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_DECILE_SCORE',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;





                    ----------------------------------------
                    -- TEST_SCORE_ATTRIBUTES
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_SCORE_ATTRIBUTES := TRIM(v_xtbl_testscore_rec.TEST_SCORE_ATTRIBUTES);
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCORE_ATTRIBUTES := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SCORE_ATTRIBUTES',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;

                    ----------------------------------------
                    -- TEST_SCORE_TEXT
                    ----------------------------------------
                    BEGIN
                        v_testscore_rec.TEST_SCORE_TEXT := TO_CHAR(v_xtbl_testscore_rec.TEST_SCORE_TEXT);
                    EXCEPTION
                        WHEN OTHERS THEN
                            v_testscore_rec.TEST_SCORE_TEXT := '@ERR';
                            v_testscore_rec.sys_audit_ind := 'Y';
							v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

							K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
								v_SYS_ETL_SOURCE,
								'TEST_SCORE_TEXT',
								v_testscore_rec.TEST_SCORES_KEY,
								v_AUDIT_NATURAL_KEY,
								'UNHANDLED ERROR',
								sqlerrm,
								'Y',
								v_AUDIT_BASE_SEVERITY
							);
                    END;


                    SELECT /*+ INDEX(FTBL_TEST_SCORES VFIDX_PROD_TEST_SCORES_KEY) */
                        COUNT(*)
                    INTO v_rowcnt
                    FROM K12INTEL_DW.FTBL_TEST_SCORES
                    WHERE TEST_SCORES_KEY = v_testscore_rec.TEST_SCORES_KEY;

                    IF (v_rowcnt = 0) THEN
                        BEGIN
                            INSERT INTO K12INTEL_DW.FTBL_TEST_SCORES
                            VALUES
                            (
                                v_testscore_rec.TEST_SCORES_KEY,
                                v_testscore_rec.TESTS_KEY,
                                v_testscore_rec.TEST_BENCHMARKS_KEY,
                                v_testscore_rec.CUSTOM_BENCHMARK_KEY,
                                v_testscore_rec.CURRICULUM_KEY,
                                v_testscore_rec.SCHOOL_KEY,
                                v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY,
                                v_testscore_rec.FACILITIES_KEY,
                                v_testscore_rec.STUDENT_KEY,
                                v_testscore_rec.STUDENT_ATTRIB_KEY,
                                v_testscore_rec.STUDENT_EVOLVE_KEY,
                                v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY,
                                v_testscore_rec.STAFF_KEY,
                                v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY,
                                v_testscore_rec.STAFF_EVOLVE_KEY,
                                v_testscore_rec.STAFF_ASSIGNMENT_KEY,
                                v_testscore_rec.COURSE_KEY,
                                v_testscore_rec.COURSE_OFFERINGS_KEY,
                                v_testscore_rec.STUDENT_SCHEDULES_KEY,
                                v_testscore_rec.CALENDAR_DATE_KEY,
                                v_testscore_rec.SCHOOL_DATES_KEY,
                                v_testscore_rec.TEST_RECORD_TYPE,
                                v_testscore_rec.TEST_ADMIN_PERIOD,
                                v_testscore_rec.TEST_PASSED_INDICATOR,
                                v_testscore_rec.TEST_CUSTOM_PASS_IND,
                                v_testscore_rec.TEST_PRIMARY_RESULT_CODE,
                                v_testscore_rec.TEST_PRIMARY_RESULT,
                                v_testscore_rec.TEST_SECONDARY_RESULT_CODE,
                                v_testscore_rec.TEST_SECONDARY_RESULT,
                                v_testscore_rec.TEST_TERTIARY_RESULT_CODE,
                                v_testscore_rec.TEST_TERTIARY_RESULT,
                                v_testscore_rec.TEST_QUATERNARY_RESULT_CODE,
                                v_testscore_rec.TEST_QUATERNARY_RESULT,
                                v_testscore_rec.TEST_CUSTOM_RESULT_CODE,
                                v_testscore_rec.TEST_CUSTOM_RESULT,
                                v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT,
                                v_testscore_rec.PRIMARY_EXEMPTION_CODE,
                                v_testscore_rec.PRIMARY_EXEMPTION,
                                v_testscore_rec.SECONDARY_EXEMPTION_CODE,
                                v_testscore_rec.SECONDARY_EXEMPTION,
                                v_testscore_rec.TERTIARY_EXEMPTION_CODE,
                                v_testscore_rec.TERTIARY_EXEMPTION,
                                v_testscore_rec.QUATERNARY_EXEMPTION_CODE,
                                v_testscore_rec.QUATERNARY_EXEMPTION,
                                v_testscore_rec.TEST_INTERVENTION_TYPE,
                                v_testscore_rec.TEST_INTERVENTION_DESC,
                                v_testscore_rec.SECONDARY_INTERVENTION_CODE,
                                v_testscore_rec.SECONDARY_INTERVENTION,
                                v_testscore_rec.TERTIARY_INTERVENTION_CODE,
                                v_testscore_rec.TERTIARY_INTERVENTION,
                                v_testscore_rec.QUATERNARY_INTERVENTION_CODE,
                                v_testscore_rec.QUATERNARY_INTERVENTION,
                                v_testscore_rec.TEST_ATTEMPT_NUMBER,
                                v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR,
                                v_testscore_rec.TEST_ITEMS_POSSIBLE,
                                v_testscore_rec.TEST_ITEMS_ATTEMPTED,
                                v_testscore_rec.TEST_SCORE_VALUE,
                                v_testscore_rec.TEST_RAW_SCORE,
                                v_testscore_rec.TEST_SCALED_SCORE,
                                v_testscore_rec.TEST_PREDICTED_SCORE,
                                v_testscore_rec.TEST_LOWER_BOUND,
                                v_testscore_rec.TEST_UPPER_BOUND,
                                v_testscore_rec.TEST_NCE_SCORE,
                                v_testscore_rec.TEST_PERCENTAGE_SCORE,
                                v_testscore_rec.TEST_PERCENTILE_SCORE,
                                v_testscore_rec.TEST_GRADE_EQUIVALENT,
                                v_testscore_rec.TEST_READING_LEVEL,
                                v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX,
                                v_testscore_rec.TEST_GROWTH_PERCENTILE,
                                v_testscore_rec.TEST_GROWTH_RESULT_CODE,
                                v_testscore_rec.TEST_GROWTH_RESULT,
                                v_testscore_rec.TEST_GROWTH_TARGET_1,
                                v_testscore_rec.TEST_GROWTH_TARGET_2,
                                v_testscore_rec.TEST_GROWTH_TARGET_3,
                                v_testscore_rec.TEST_GROWTH_TARGET_4,
                                v_testscore_rec.TEST_STANDARD_ERROR,
                                v_testscore_rec.TEST_Z_SCORE,
                                v_testscore_rec.TEST_T_SCORE,
                                v_testscore_rec.TEST_STANINE_SCORE,
                                v_testscore_rec.TEST_QUARTILE_SCORE,
                                v_testscore_rec.TEST_DECILE_SCORE,
                                v_testscore_rec.TEST_SCORE_ATTRIBUTES,
                                v_testscore_rec.TEST_SCORE_TEXT,
                                v_testscore_rec.TEST_STUDENT_GRADE,
                                v_testscore_rec.TEST_TEACHER,
                                v_testscore_rec.TEST_ADMIN_CODE,
                                v_testscore_rec.DISTRICT_CODE,
                                v_testscore_rec.SYS_ETL_SOURCE,
                                SYSDATE, -- SYS_CREATED,
                                SYSDATE, --SYS_UPDATED,
                                v_testscore_rec.SYS_AUDIT_IND,		
                                v_testscore_rec.SYS_PARTITION_VALUE		
                            );
							v_rowsinserted  := v_rowsinserted + SQL%ROWCOUNT;
                            SET_TESTSCORE_AUDIT_IND(v_admin_rec.TEST_admin_key, v_xtbl_testscore_rec.TEST_NUMBER, v_xtbl_testscore_rec.SYS_AUDIT_IND, v_testscore_rec.SYS_AUDIT_IND);
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_ok_to_commit := FALSE;
                                SET_TESTSCORE_AUDIT_IND(v_admin_rec.TEST_admin_key, v_xtbl_testscore_rec.TEST_NUMBER, v_xtbl_testscore_rec.SYS_AUDIT_IND, 'Y');
								v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

								K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
									v_SYS_ETL_SOURCE,
									'INSERT FTBL_TEST_SCORES',
									v_testscore_rec.TEST_SCORES_KEY,
									v_AUDIT_NATURAL_KEY,
									'UNHANDLED ERROR',
									sqlerrm,
									'Y',
									v_AUDIT_BASE_SEVERITY
								);
                        END; -- INSERT exception handler
                    ELSIF (v_rowcnt = 1) THEN
                        BEGIN
                            UPDATE K12INTEL_DW.FTBL_TEST_SCORES
                            SET
                                TESTS_KEY = v_testscore_rec.TESTS_KEY,
                                TEST_BENCHMARKS_KEY = v_testscore_rec.TEST_BENCHMARKS_KEY,
                                CUSTOM_BENCHMARK_KEY = v_testscore_rec.CUSTOM_BENCHMARK_KEY,
                                CURRICULUM_KEY = v_testscore_rec.CURRICULUM_KEY,
                                SCHOOL_KEY = v_testscore_rec.SCHOOL_KEY,
                                SCHOOL_ANNUAL_ATTRIBS_KEY = v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY,
                                FACILITIES_KEY = v_testscore_rec.FACILITIES_KEY,
                                STUDENT_KEY = v_testscore_rec.STUDENT_KEY,
                                STUDENT_ATTRIB_KEY = v_testscore_rec.STUDENT_ATTRIB_KEY,
                                STUDENT_EVOLVE_KEY = v_testscore_rec.STUDENT_EVOLVE_KEY,
                                STUDENT_ANNUAL_ATTRIBS_KEY = v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY,
                                STAFF_KEY = v_testscore_rec.STAFF_KEY,
                                STAFF_ANNUAL_ATTRIBS_KEY = v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY,
                                STAFF_EVOLVE_KEY = v_testscore_rec.STAFF_EVOLVE_KEY,
                                STAFF_ASSIGNMENT_KEY = v_testscore_rec.STAFF_ASSIGNMENT_KEY,
                                COURSE_KEY = v_testscore_rec.COURSE_KEY,
                                COURSE_OFFERINGS_KEY = v_testscore_rec.COURSE_OFFERINGS_KEY,
                                STUDENT_SCHEDULES_KEY = v_testscore_rec.STUDENT_SCHEDULES_KEY,
                                CALENDAR_DATE_KEY = v_testscore_rec.CALENDAR_DATE_KEY,
                                SCHOOL_DATES_KEY = v_testscore_rec.SCHOOL_DATES_KEY,
                                TEST_RECORD_TYPE = v_testscore_rec.TEST_RECORD_TYPE,
                                TEST_ADMIN_PERIOD = v_testscore_rec.TEST_ADMIN_PERIOD,
                                TEST_PASSED_INDICATOR = v_testscore_rec.TEST_PASSED_INDICATOR,
                                TEST_CUSTOM_PASS_IND = v_testscore_rec.TEST_CUSTOM_PASS_IND,
                                TEST_PRIMARY_RESULT_CODE = v_testscore_rec.TEST_PRIMARY_RESULT_CODE,
                                TEST_PRIMARY_RESULT = v_testscore_rec.TEST_PRIMARY_RESULT,
                                TEST_SECONDARY_RESULT_CODE = v_testscore_rec.TEST_SECONDARY_RESULT_CODE,
                                TEST_SECONDARY_RESULT = v_testscore_rec.TEST_SECONDARY_RESULT,
                                TEST_TERTIARY_RESULT_CODE = v_testscore_rec.TEST_TERTIARY_RESULT_CODE,
                                TEST_TERTIARY_RESULT = v_testscore_rec.TEST_TERTIARY_RESULT,
                                TEST_QUATERNARY_RESULT_CODE = v_testscore_rec.TEST_QUATERNARY_RESULT_CODE,
                                TEST_QUATERNARY_RESULT = v_testscore_rec.TEST_QUATERNARY_RESULT,
                                TEST_CUSTOM_RESULT_CODE = v_testscore_rec.TEST_CUSTOM_RESULT_CODE,
                                TEST_CUSTOM_RESULT = v_testscore_rec.TEST_CUSTOM_RESULT,
                                TEST_SCORE_TO_PREDICTED_RESULT = v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT,
                                PRIMARY_EXEMPTION_CODE = v_testscore_rec.PRIMARY_EXEMPTION_CODE,
                                PRIMARY_EXEMPTION = v_testscore_rec.PRIMARY_EXEMPTION,
                                SECONDARY_EXEMPTION_CODE = v_testscore_rec.SECONDARY_EXEMPTION_CODE,
                                SECONDARY_EXEMPTION = v_testscore_rec.SECONDARY_EXEMPTION,
                                TERTIARY_EXEMPTION_CODE = v_testscore_rec.TERTIARY_EXEMPTION_CODE,
                                TERTIARY_EXEMPTION = v_testscore_rec.TERTIARY_EXEMPTION,
                                QUATERNARY_EXEMPTION_CODE = v_testscore_rec.QUATERNARY_EXEMPTION_CODE,
                                QUATERNARY_EXEMPTION = v_testscore_rec.QUATERNARY_EXEMPTION,
                                TEST_INTERVENTION_TYPE = v_testscore_rec.TEST_INTERVENTION_TYPE,
                                TEST_INTERVENTION_DESC = v_testscore_rec.TEST_INTERVENTION_DESC,
                                SECONDARY_INTERVENTION_CODE = v_testscore_rec.SECONDARY_INTERVENTION_CODE,
                                SECONDARY_INTERVENTION = v_testscore_rec.SECONDARY_INTERVENTION,
                                TERTIARY_INTERVENTION_CODE = v_testscore_rec.TERTIARY_INTERVENTION_CODE,
                                TERTIARY_INTERVENTION = v_testscore_rec.TERTIARY_INTERVENTION,
                                QUATERNARY_INTERVENTION_CODE = v_testscore_rec.QUATERNARY_INTERVENTION_CODE,
                                QUATERNARY_INTERVENTION = v_testscore_rec.QUATERNARY_INTERVENTION,
                                TEST_HIGHEST_SCORE_INDICATOR = v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR,
                                TEST_ITEMS_POSSIBLE = v_testscore_rec.TEST_ITEMS_POSSIBLE,
                                TEST_ITEMS_ATTEMPTED = v_testscore_rec.TEST_ITEMS_ATTEMPTED,
                                TEST_SCORE_VALUE = v_testscore_rec.TEST_SCORE_VALUE,
                                TEST_RAW_SCORE = v_testscore_rec.TEST_RAW_SCORE,
                                TEST_SCALED_SCORE = v_testscore_rec.TEST_SCALED_SCORE,
                                TEST_PREDICTED_SCORE = v_testscore_rec.TEST_PREDICTED_SCORE,
                                TEST_LOWER_BOUND = v_testscore_rec.TEST_LOWER_BOUND,
                                TEST_UPPER_BOUND = v_testscore_rec.TEST_UPPER_BOUND,
                                TEST_NCE_SCORE = v_testscore_rec.TEST_NCE_SCORE,
                                TEST_PERCENTAGE_SCORE = v_testscore_rec.TEST_PERCENTAGE_SCORE,
                                TEST_PERCENTILE_SCORE = v_testscore_rec.TEST_PERCENTILE_SCORE,
                                TEST_GRADE_EQUIVALENT = v_testscore_rec.TEST_GRADE_EQUIVALENT,
                                TEST_READING_LEVEL = v_testscore_rec.TEST_READING_LEVEL,
                                TEST_SCHOOL_ABILITY_INDEX = v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX,
                                TEST_GROWTH_PERCENTILE = v_testscore_rec.TEST_GROWTH_PERCENTILE,
                                TEST_GROWTH_RESULT_CODE = v_testscore_rec.TEST_GROWTH_RESULT_CODE,
                                TEST_GROWTH_RESULT = v_testscore_rec.TEST_GROWTH_RESULT,
                                TEST_GROWTH_TARGET_1 = v_testscore_rec.TEST_GROWTH_TARGET_1,
                                TEST_GROWTH_TARGET_2 = v_testscore_rec.TEST_GROWTH_TARGET_2,
                                TEST_GROWTH_TARGET_3 = v_testscore_rec.TEST_GROWTH_TARGET_3,
                                TEST_GROWTH_TARGET_4 = v_testscore_rec.TEST_GROWTH_TARGET_4,
                                TEST_STANDARD_ERROR = v_testscore_rec.TEST_STANDARD_ERROR,
                                TEST_Z_SCORE = v_testscore_rec.TEST_Z_SCORE,
                                TEST_T_SCORE = v_testscore_rec.TEST_T_SCORE,
                                TEST_STANINE_SCORE = v_testscore_rec.TEST_STANINE_SCORE,
                                TEST_QUARTILE_SCORE = v_testscore_rec.TEST_QUARTILE_SCORE,
                                TEST_DECILE_SCORE = v_testscore_rec.TEST_DECILE_SCORE,
                                TEST_SCORE_ATTRIBUTES = v_testscore_rec.TEST_SCORE_ATTRIBUTES,
                                TEST_SCORE_TEXT = v_testscore_rec.TEST_SCORE_TEXT,
                                TEST_STUDENT_GRADE = v_testscore_rec.TEST_STUDENT_GRADE,
                                TEST_TEACHER = v_testscore_rec.TEST_TEACHER,
                                TEST_ADMIN_CODE = v_testscore_rec.TEST_ADMIN_CODE,
                                DISTRICT_CODE = v_testscore_rec.DISTRICT_CODE,
                                SYS_ETL_SOURCE = v_testscore_rec.SYS_ETL_SOURCE,
                                SYS_UPDATED = SYSDATE,
                                SYS_AUDIT_IND = v_testscore_rec.SYS_AUDIT_IND,
                                SYS_PARTITION_VALUE = v_testscore_rec.SYS_PARTITION_VALUE
                            WHERE TEST_SCORES_KEY = v_testscore_rec.TEST_SCORES_KEY AND
                            (
                                (
                                    (TESTS_KEY <> v_testscore_rec.TESTS_KEY) OR
                                    (TESTS_KEY IS NULL AND v_testscore_rec.TESTS_KEY IS NOT NULL) OR
                                    (TESTS_KEY IS NOT NULL AND v_testscore_rec.TESTS_KEY IS NULL)
                                ) OR
                                (
                                    (TEST_BENCHMARKS_KEY <> v_testscore_rec.TEST_BENCHMARKS_KEY) OR
                                    (TEST_BENCHMARKS_KEY IS NULL AND v_testscore_rec.TEST_BENCHMARKS_KEY IS NOT NULL) OR
                                    (TEST_BENCHMARKS_KEY IS NOT NULL AND v_testscore_rec.TEST_BENCHMARKS_KEY IS NULL)
                                ) OR
                                (
                                    (CUSTOM_BENCHMARK_KEY <> v_testscore_rec.CUSTOM_BENCHMARK_KEY) OR
                                    (CUSTOM_BENCHMARK_KEY IS NULL AND v_testscore_rec.CUSTOM_BENCHMARK_KEY IS NOT NULL) OR
                                    (CUSTOM_BENCHMARK_KEY IS NOT NULL AND v_testscore_rec.CUSTOM_BENCHMARK_KEY IS NULL)
                                ) OR
                                (
                                    (CURRICULUM_KEY <> v_testscore_rec.CURRICULUM_KEY) OR
                                    (CURRICULUM_KEY IS NULL AND v_testscore_rec.CURRICULUM_KEY IS NOT NULL) OR
                                    (CURRICULUM_KEY IS NOT NULL AND v_testscore_rec.CURRICULUM_KEY IS NULL)
                                ) OR
                                (
                                    (SCHOOL_KEY <> v_testscore_rec.SCHOOL_KEY) OR
                                    (SCHOOL_KEY IS NULL AND v_testscore_rec.SCHOOL_KEY IS NOT NULL) OR
                                    (SCHOOL_KEY IS NOT NULL AND v_testscore_rec.SCHOOL_KEY IS NULL)
                                ) OR
                                (
                                    (SCHOOL_ANNUAL_ATTRIBS_KEY <> v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY) OR
                                    (SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL AND v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL) OR
                                    (SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL AND v_testscore_rec.SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL)
                                ) OR
                                (
                                    (COURSE_KEY <> v_testscore_rec.COURSE_KEY) OR
                                    (COURSE_KEY IS NULL AND v_testscore_rec.COURSE_KEY IS NOT NULL) OR
                                    (COURSE_KEY IS NOT NULL AND v_testscore_rec.COURSE_KEY IS NULL)
                                ) OR
                                (
                                    (COURSE_OFFERINGS_KEY <> v_testscore_rec.COURSE_OFFERINGS_KEY) OR
                                    (COURSE_OFFERINGS_KEY IS NULL AND v_testscore_rec.COURSE_OFFERINGS_KEY IS NOT NULL) OR
                                    (COURSE_OFFERINGS_KEY IS NOT NULL AND v_testscore_rec.COURSE_OFFERINGS_KEY IS NULL)
                                ) OR
                                (
                                    (STUDENT_SCHEDULES_KEY <> v_testscore_rec.STUDENT_SCHEDULES_KEY) OR
                                    (STUDENT_SCHEDULES_KEY IS NULL AND v_testscore_rec.STUDENT_SCHEDULES_KEY IS NOT NULL) OR
                                    (STUDENT_SCHEDULES_KEY IS NOT NULL AND v_testscore_rec.STUDENT_SCHEDULES_KEY IS NULL)
                                ) OR
                                (
                                    (FACILITIES_KEY <> v_testscore_rec.FACILITIES_KEY) OR
                                    (FACILITIES_KEY IS NULL AND v_testscore_rec.FACILITIES_KEY IS NOT NULL) OR
                                    (FACILITIES_KEY IS NOT NULL AND v_testscore_rec.FACILITIES_KEY IS NULL)
                                ) OR
                                (
                                    (STUDENT_KEY <> v_testscore_rec.STUDENT_KEY) OR
                                    (STUDENT_KEY IS NULL AND v_testscore_rec.STUDENT_KEY IS NOT NULL) OR
                                    (STUDENT_KEY IS NOT NULL AND v_testscore_rec.STUDENT_KEY IS NULL)
                                ) OR
                                (
                                    (STUDENT_ATTRIB_KEY <> v_testscore_rec.STUDENT_ATTRIB_KEY) OR
                                    (STUDENT_ATTRIB_KEY IS NULL AND v_testscore_rec.STUDENT_ATTRIB_KEY IS NOT NULL) OR
                                    (STUDENT_ATTRIB_KEY IS NOT NULL AND v_testscore_rec.STUDENT_ATTRIB_KEY IS NULL)
                                ) OR
                                (
                                    (STUDENT_EVOLVE_KEY <> v_testscore_rec.STUDENT_EVOLVE_KEY) OR
                                    (STUDENT_EVOLVE_KEY IS NULL AND v_testscore_rec.STUDENT_EVOLVE_KEY IS NOT NULL) OR
                                    (STUDENT_EVOLVE_KEY IS NOT NULL AND v_testscore_rec.STUDENT_EVOLVE_KEY IS NULL)
                                ) OR
                                (
                                    (STUDENT_ANNUAL_ATTRIBS_KEY <> v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY) OR
                                    (STUDENT_ANNUAL_ATTRIBS_KEY IS NULL AND v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL) OR
                                    (STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL AND v_testscore_rec.STUDENT_ANNUAL_ATTRIBS_KEY IS NULL)
                                ) OR
                                (
                                    (STAFF_KEY <> v_testscore_rec.STAFF_KEY) OR
                                    (STAFF_KEY IS NULL AND v_testscore_rec.STAFF_KEY IS NOT NULL) OR
                                    (STAFF_KEY IS NOT NULL AND v_testscore_rec.STAFF_KEY IS NULL)
                                ) OR
                                (
                                    (STAFF_ANNUAL_ATTRIBS_KEY <> v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY) OR
                                    (STAFF_ANNUAL_ATTRIBS_KEY IS NULL AND v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY IS NOT NULL) OR
                                    (STAFF_ANNUAL_ATTRIBS_KEY IS NOT NULL AND v_testscore_rec.STAFF_ANNUAL_ATTRIBS_KEY IS NULL)
                                ) OR
                                (
                                    (STAFF_EVOLVE_KEY <> v_testscore_rec.STAFF_EVOLVE_KEY) OR
                                    (STAFF_EVOLVE_KEY IS NULL AND v_testscore_rec.STAFF_EVOLVE_KEY IS NOT NULL) OR
                                    (STAFF_EVOLVE_KEY IS NOT NULL AND v_testscore_rec.STAFF_EVOLVE_KEY IS NULL)
                                ) OR
                                (
                                    (STAFF_ASSIGNMENT_KEY <> v_testscore_rec.STAFF_ASSIGNMENT_KEY) OR
                                    (STAFF_ASSIGNMENT_KEY IS NULL AND v_testscore_rec.STAFF_ASSIGNMENT_KEY IS NOT NULL) OR
                                    (STAFF_ASSIGNMENT_KEY IS NOT NULL AND v_testscore_rec.STAFF_ASSIGNMENT_KEY IS NULL)
                                ) OR
                                (
                                    (CALENDAR_DATE_KEY <> v_testscore_rec.CALENDAR_DATE_KEY) OR
                                    (CALENDAR_DATE_KEY IS NULL AND v_testscore_rec.CALENDAR_DATE_KEY IS NOT NULL) OR
                                    (CALENDAR_DATE_KEY IS NOT NULL AND v_testscore_rec.CALENDAR_DATE_KEY IS NULL)
                                ) OR
                                (
                                    (SCHOOL_DATES_KEY <> v_testscore_rec.SCHOOL_DATES_KEY) OR
                                    (SCHOOL_DATES_KEY IS NULL AND v_testscore_rec.SCHOOL_DATES_KEY IS NOT NULL) OR
                                    (SCHOOL_DATES_KEY IS NOT NULL AND v_testscore_rec.SCHOOL_DATES_KEY IS NULL)
                                ) OR
                                (
                                    (TEST_RECORD_TYPE <> v_testscore_rec.TEST_RECORD_TYPE) OR
                                    (TEST_RECORD_TYPE IS NULL AND v_testscore_rec.TEST_RECORD_TYPE IS NOT NULL) OR
                                    (TEST_RECORD_TYPE IS NOT NULL AND v_testscore_rec.TEST_RECORD_TYPE IS NULL)
                                ) OR
                                (
                                    (TEST_ADMIN_PERIOD <> v_testscore_rec.TEST_ADMIN_PERIOD) OR
                                    (TEST_ADMIN_PERIOD IS NULL AND v_testscore_rec.TEST_ADMIN_PERIOD IS NOT NULL) OR
                                    (TEST_ADMIN_PERIOD IS NOT NULL AND v_testscore_rec.TEST_ADMIN_PERIOD IS NULL)
                                ) OR
                                (
                                    (TEST_PASSED_INDICATOR <> v_testscore_rec.TEST_PASSED_INDICATOR) OR
                                    (TEST_PASSED_INDICATOR IS NULL AND v_testscore_rec.TEST_PASSED_INDICATOR IS NOT NULL) OR
                                    (TEST_PASSED_INDICATOR IS NOT NULL AND v_testscore_rec.TEST_PASSED_INDICATOR IS NULL)
                                ) OR
                                (
                                    (TEST_CUSTOM_PASS_IND <> v_testscore_rec.TEST_CUSTOM_PASS_IND) OR
                                    (TEST_CUSTOM_PASS_IND IS NULL AND v_testscore_rec.TEST_CUSTOM_PASS_IND IS NOT NULL) OR
                                    (TEST_CUSTOM_PASS_IND IS NOT NULL AND v_testscore_rec.TEST_CUSTOM_PASS_IND IS NULL)
                                ) OR
                                (
                                    (TEST_PRIMARY_RESULT_CODE <> v_testscore_rec.TEST_PRIMARY_RESULT_CODE) OR
                                    (TEST_PRIMARY_RESULT_CODE IS NULL AND v_testscore_rec.TEST_PRIMARY_RESULT_CODE IS NOT NULL) OR
                                    (TEST_PRIMARY_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_PRIMARY_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_PRIMARY_RESULT <> v_testscore_rec.TEST_PRIMARY_RESULT) OR
                                    (TEST_PRIMARY_RESULT IS NULL AND v_testscore_rec.TEST_PRIMARY_RESULT IS NOT NULL) OR
                                    (TEST_PRIMARY_RESULT IS NOT NULL AND v_testscore_rec.TEST_PRIMARY_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_SECONDARY_RESULT_CODE <> v_testscore_rec.TEST_SECONDARY_RESULT_CODE) OR
                                    (TEST_SECONDARY_RESULT_CODE IS NULL AND v_testscore_rec.TEST_SECONDARY_RESULT_CODE IS NOT NULL) OR
                                    (TEST_SECONDARY_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_SECONDARY_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_SECONDARY_RESULT <> v_testscore_rec.TEST_SECONDARY_RESULT) OR
                                    (TEST_SECONDARY_RESULT IS NULL AND v_testscore_rec.TEST_SECONDARY_RESULT IS NOT NULL) OR
                                    (TEST_SECONDARY_RESULT IS NOT NULL AND v_testscore_rec.TEST_SECONDARY_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_TERTIARY_RESULT_CODE <> v_testscore_rec.TEST_TERTIARY_RESULT_CODE) OR
                                    (TEST_TERTIARY_RESULT_CODE IS NULL AND v_testscore_rec.TEST_TERTIARY_RESULT_CODE IS NOT NULL) OR
                                    (TEST_TERTIARY_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_TERTIARY_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_TERTIARY_RESULT <> v_testscore_rec.TEST_TERTIARY_RESULT) OR
                                    (TEST_TERTIARY_RESULT IS NULL AND v_testscore_rec.TEST_TERTIARY_RESULT IS NOT NULL) OR
                                    (TEST_TERTIARY_RESULT IS NOT NULL AND v_testscore_rec.TEST_TERTIARY_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_QUATERNARY_RESULT_CODE <> v_testscore_rec.TEST_QUATERNARY_RESULT_CODE) OR
                                    (TEST_QUATERNARY_RESULT_CODE IS NULL AND v_testscore_rec.TEST_QUATERNARY_RESULT_CODE IS NOT NULL) OR
                                    (TEST_QUATERNARY_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_QUATERNARY_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_QUATERNARY_RESULT <> v_testscore_rec.TEST_QUATERNARY_RESULT) OR
                                    (TEST_QUATERNARY_RESULT IS NULL AND v_testscore_rec.TEST_QUATERNARY_RESULT IS NOT NULL) OR
                                    (TEST_QUATERNARY_RESULT IS NOT NULL AND v_testscore_rec.TEST_QUATERNARY_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_CUSTOM_RESULT_CODE <> v_testscore_rec.TEST_CUSTOM_RESULT_CODE) OR
                                    (TEST_CUSTOM_RESULT_CODE IS NULL AND v_testscore_rec.TEST_CUSTOM_RESULT_CODE IS NOT NULL) OR
                                    (TEST_CUSTOM_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_CUSTOM_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_CUSTOM_RESULT <> v_testscore_rec.TEST_CUSTOM_RESULT) OR
                                    (TEST_CUSTOM_RESULT IS NULL AND v_testscore_rec.TEST_CUSTOM_RESULT IS NOT NULL) OR
                                    (TEST_CUSTOM_RESULT IS NOT NULL AND v_testscore_rec.TEST_CUSTOM_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_SCORE_TO_PREDICTED_RESULT <> v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT) OR
                                    (TEST_SCORE_TO_PREDICTED_RESULT IS NULL AND v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT IS NOT NULL) OR
                                    (TEST_SCORE_TO_PREDICTED_RESULT IS NOT NULL AND v_testscore_rec.TEST_SCORE_TO_PREDICTED_RESULT IS NULL)
                                ) OR
                                (
                                    (PRIMARY_EXEMPTION_CODE <> v_testscore_rec.PRIMARY_EXEMPTION_CODE) OR
                                    (PRIMARY_EXEMPTION_CODE IS NULL AND v_testscore_rec.PRIMARY_EXEMPTION_CODE IS NOT NULL) OR
                                    (PRIMARY_EXEMPTION_CODE IS NOT NULL AND v_testscore_rec.PRIMARY_EXEMPTION_CODE IS NULL)
                                ) OR
                                (
                                    (PRIMARY_EXEMPTION <> v_testscore_rec.PRIMARY_EXEMPTION) OR
                                    (PRIMARY_EXEMPTION IS NULL AND v_testscore_rec.PRIMARY_EXEMPTION IS NOT NULL) OR
                                    (PRIMARY_EXEMPTION IS NOT NULL AND v_testscore_rec.PRIMARY_EXEMPTION IS NULL)
                                ) OR
                                (
                                    (SECONDARY_EXEMPTION_CODE <> v_testscore_rec.SECONDARY_EXEMPTION_CODE) OR
                                    (SECONDARY_EXEMPTION_CODE IS NULL AND v_testscore_rec.SECONDARY_EXEMPTION_CODE IS NOT NULL) OR
                                    (SECONDARY_EXEMPTION_CODE IS NOT NULL AND v_testscore_rec.SECONDARY_EXEMPTION_CODE IS NULL)
                                ) OR
                                (
                                    (SECONDARY_EXEMPTION <> v_testscore_rec.SECONDARY_EXEMPTION) OR
                                    (SECONDARY_EXEMPTION IS NULL AND v_testscore_rec.SECONDARY_EXEMPTION IS NOT NULL) OR
                                    (SECONDARY_EXEMPTION IS NOT NULL AND v_testscore_rec.SECONDARY_EXEMPTION IS NULL)
                                ) OR
                                (
                                    (TERTIARY_EXEMPTION_CODE <> v_testscore_rec.TERTIARY_EXEMPTION_CODE) OR
                                    (TERTIARY_EXEMPTION_CODE IS NULL AND v_testscore_rec.TERTIARY_EXEMPTION_CODE IS NOT NULL) OR
                                    (TERTIARY_EXEMPTION_CODE IS NOT NULL AND v_testscore_rec.TERTIARY_EXEMPTION_CODE IS NULL)
                                ) OR
                                (
                                    (TERTIARY_EXEMPTION <> v_testscore_rec.TERTIARY_EXEMPTION) OR
                                    (TERTIARY_EXEMPTION IS NULL AND v_testscore_rec.TERTIARY_EXEMPTION IS NOT NULL) OR
                                    (TERTIARY_EXEMPTION IS NOT NULL AND v_testscore_rec.TERTIARY_EXEMPTION IS NULL)
                                ) OR
                                (
                                    (QUATERNARY_EXEMPTION_CODE <> v_testscore_rec.QUATERNARY_EXEMPTION_CODE) OR
                                    (QUATERNARY_EXEMPTION_CODE IS NULL AND v_testscore_rec.QUATERNARY_EXEMPTION_CODE IS NOT NULL) OR
                                    (QUATERNARY_EXEMPTION_CODE IS NOT NULL AND v_testscore_rec.QUATERNARY_EXEMPTION_CODE IS NULL)
                                ) OR
                                (
                                    (QUATERNARY_EXEMPTION <> v_testscore_rec.QUATERNARY_EXEMPTION) OR
                                    (QUATERNARY_EXEMPTION IS NULL AND v_testscore_rec.QUATERNARY_EXEMPTION IS NOT NULL) OR
                                    (QUATERNARY_EXEMPTION IS NOT NULL AND v_testscore_rec.QUATERNARY_EXEMPTION IS NULL)
                                ) OR
                                (
                                    (TEST_INTERVENTION_TYPE <> v_testscore_rec.TEST_INTERVENTION_TYPE) OR
                                    (TEST_INTERVENTION_TYPE IS NULL AND v_testscore_rec.TEST_INTERVENTION_TYPE IS NOT NULL) OR
                                    (TEST_INTERVENTION_TYPE IS NOT NULL AND v_testscore_rec.TEST_INTERVENTION_TYPE IS NULL)
                                ) OR
                                (
                                    (TEST_INTERVENTION_DESC <> v_testscore_rec.TEST_INTERVENTION_DESC) OR
                                    (TEST_INTERVENTION_DESC IS NULL AND v_testscore_rec.TEST_INTERVENTION_DESC IS NOT NULL) OR
                                    (TEST_INTERVENTION_DESC IS NOT NULL AND v_testscore_rec.TEST_INTERVENTION_DESC IS NULL)
                                ) OR
                                (
                                    (SECONDARY_INTERVENTION_CODE <> v_testscore_rec.SECONDARY_INTERVENTION_CODE) OR
                                    (SECONDARY_INTERVENTION_CODE IS NULL AND v_testscore_rec.SECONDARY_INTERVENTION_CODE IS NOT NULL) OR
                                    (SECONDARY_INTERVENTION_CODE IS NOT NULL AND v_testscore_rec.SECONDARY_INTERVENTION_CODE IS NULL)
                                ) OR
                                (
                                    (SECONDARY_INTERVENTION <> v_testscore_rec.SECONDARY_INTERVENTION) OR
                                    (SECONDARY_INTERVENTION IS NULL AND v_testscore_rec.SECONDARY_INTERVENTION IS NOT NULL) OR
                                    (SECONDARY_INTERVENTION IS NOT NULL AND v_testscore_rec.SECONDARY_INTERVENTION IS NULL)
                                ) OR
                                (
                                    (TERTIARY_INTERVENTION_CODE <> v_testscore_rec.TERTIARY_INTERVENTION_CODE) OR
                                    (TERTIARY_INTERVENTION_CODE IS NULL AND v_testscore_rec.TERTIARY_INTERVENTION_CODE IS NOT NULL) OR
                                    (TERTIARY_INTERVENTION_CODE IS NOT NULL AND v_testscore_rec.TERTIARY_INTERVENTION_CODE IS NULL)
                                ) OR
                                (
                                    (TERTIARY_INTERVENTION <> v_testscore_rec.TERTIARY_INTERVENTION) OR
                                    (TERTIARY_INTERVENTION IS NULL AND v_testscore_rec.TERTIARY_INTERVENTION IS NOT NULL) OR
                                    (TERTIARY_INTERVENTION IS NOT NULL AND v_testscore_rec.TERTIARY_INTERVENTION IS NULL)
                                ) OR
                                (
                                    (QUATERNARY_INTERVENTION_CODE <> v_testscore_rec.QUATERNARY_INTERVENTION_CODE) OR
                                    (QUATERNARY_INTERVENTION_CODE IS NULL AND v_testscore_rec.QUATERNARY_INTERVENTION_CODE IS NOT NULL) OR
                                    (QUATERNARY_INTERVENTION_CODE IS NOT NULL AND v_testscore_rec.QUATERNARY_INTERVENTION_CODE IS NULL)
                                ) OR
                                (
                                    (QUATERNARY_INTERVENTION <> v_testscore_rec.QUATERNARY_INTERVENTION) OR
                                    (QUATERNARY_INTERVENTION IS NULL AND v_testscore_rec.QUATERNARY_INTERVENTION IS NOT NULL) OR
                                    (QUATERNARY_INTERVENTION IS NOT NULL AND v_testscore_rec.QUATERNARY_INTERVENTION IS NULL)
                                ) OR
                                /*(
                                    (TEST_ATTEMPT_NUMBER <> v_testscore_rec.TEST_ATTEMPT_NUMBER) OR
                                    (TEST_ATTEMPT_NUMBER IS NULL AND v_testscore_rec.TEST_ATTEMPT_NUMBER IS NOT NULL) OR
                                    (TEST_ATTEMPT_NUMBER IS NOT NULL AND v_testscore_rec.TEST_ATTEMPT_NUMBER IS NULL)
                                ) OR*/
                                (
                                    (TEST_HIGHEST_SCORE_INDICATOR <> v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR) OR
                                    (TEST_HIGHEST_SCORE_INDICATOR IS NULL AND v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR IS NOT NULL) OR
                                    (TEST_HIGHEST_SCORE_INDICATOR IS NOT NULL AND v_testscore_rec.TEST_HIGHEST_SCORE_INDICATOR IS NULL)
                                ) OR
                                (
                                    (TEST_ITEMS_POSSIBLE <> v_testscore_rec.TEST_ITEMS_POSSIBLE) OR
                                    (TEST_ITEMS_POSSIBLE IS NULL AND v_testscore_rec.TEST_ITEMS_POSSIBLE IS NOT NULL) OR
                                    (TEST_ITEMS_POSSIBLE IS NOT NULL AND v_testscore_rec.TEST_ITEMS_POSSIBLE IS NULL)
                                ) OR
                                (
                                    (TEST_ITEMS_ATTEMPTED <> v_testscore_rec.TEST_ITEMS_ATTEMPTED) OR
                                    (TEST_ITEMS_ATTEMPTED IS NULL AND v_testscore_rec.TEST_ITEMS_ATTEMPTED IS NOT NULL) OR
                                    (TEST_ITEMS_ATTEMPTED IS NOT NULL AND v_testscore_rec.TEST_ITEMS_ATTEMPTED IS NULL)
                                ) OR
                                (
                                    (TEST_SCORE_VALUE <> v_testscore_rec.TEST_SCORE_VALUE) OR
                                    (TEST_SCORE_VALUE IS NULL AND v_testscore_rec.TEST_SCORE_VALUE IS NOT NULL) OR
                                    (TEST_SCORE_VALUE IS NOT NULL AND v_testscore_rec.TEST_SCORE_VALUE IS NULL)
                                ) OR
                                (
                                    (TEST_RAW_SCORE <> v_testscore_rec.TEST_RAW_SCORE) OR
                                    (TEST_RAW_SCORE IS NULL AND v_testscore_rec.TEST_RAW_SCORE IS NOT NULL) OR
                                    (TEST_RAW_SCORE IS NOT NULL AND v_testscore_rec.TEST_RAW_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_SCALED_SCORE <> v_testscore_rec.TEST_SCALED_SCORE) OR
                                    (TEST_SCALED_SCORE IS NULL AND v_testscore_rec.TEST_SCALED_SCORE IS NOT NULL) OR
                                    (TEST_SCALED_SCORE IS NOT NULL AND v_testscore_rec.TEST_SCALED_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_PREDICTED_SCORE <> v_testscore_rec.TEST_PREDICTED_SCORE) OR
                                    (TEST_PREDICTED_SCORE IS NULL AND v_testscore_rec.TEST_PREDICTED_SCORE IS NOT NULL) OR
                                    (TEST_PREDICTED_SCORE IS NOT NULL AND v_testscore_rec.TEST_PREDICTED_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_LOWER_BOUND <> v_testscore_rec.TEST_LOWER_BOUND) OR
                                    (TEST_LOWER_BOUND IS NULL AND v_testscore_rec.TEST_LOWER_BOUND IS NOT NULL) OR
                                    (TEST_LOWER_BOUND IS NOT NULL AND v_testscore_rec.TEST_LOWER_BOUND IS NULL)
                                ) OR
                                (
                                    (TEST_UPPER_BOUND <> v_testscore_rec.TEST_UPPER_BOUND) OR
                                    (TEST_UPPER_BOUND IS NULL AND v_testscore_rec.TEST_UPPER_BOUND IS NOT NULL) OR
                                    (TEST_UPPER_BOUND IS NOT NULL AND v_testscore_rec.TEST_UPPER_BOUND IS NULL)
                                ) OR
                                (
                                    (TEST_NCE_SCORE <> v_testscore_rec.TEST_NCE_SCORE) OR
                                    (TEST_NCE_SCORE IS NULL AND v_testscore_rec.TEST_NCE_SCORE IS NOT NULL) OR
                                    (TEST_NCE_SCORE IS NOT NULL AND v_testscore_rec.TEST_NCE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_PERCENTAGE_SCORE <> v_testscore_rec.TEST_PERCENTAGE_SCORE) OR
                                    (TEST_PERCENTAGE_SCORE IS NULL AND v_testscore_rec.TEST_PERCENTAGE_SCORE IS NOT NULL) OR
                                    (TEST_PERCENTAGE_SCORE IS NOT NULL AND v_testscore_rec.TEST_PERCENTAGE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_PERCENTILE_SCORE <> v_testscore_rec.TEST_PERCENTILE_SCORE) OR
                                    (TEST_PERCENTILE_SCORE IS NULL AND v_testscore_rec.TEST_PERCENTILE_SCORE IS NOT NULL) OR
                                    (TEST_PERCENTILE_SCORE IS NOT NULL AND v_testscore_rec.TEST_PERCENTILE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_GRADE_EQUIVALENT <> v_testscore_rec.TEST_GRADE_EQUIVALENT) OR
                                    (TEST_GRADE_EQUIVALENT IS NULL AND v_testscore_rec.TEST_GRADE_EQUIVALENT IS NOT NULL) OR
                                    (TEST_GRADE_EQUIVALENT IS NOT NULL AND v_testscore_rec.TEST_GRADE_EQUIVALENT IS NULL)
                                ) OR
                                (
                                    (TEST_READING_LEVEL <> v_testscore_rec.TEST_READING_LEVEL) OR
                                    (TEST_READING_LEVEL IS NULL AND v_testscore_rec.TEST_READING_LEVEL IS NOT NULL) OR
                                    (TEST_READING_LEVEL IS NOT NULL AND v_testscore_rec.TEST_READING_LEVEL IS NULL)
                                ) OR
                                (
                                    (TEST_SCHOOL_ABILITY_INDEX <> v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX) OR
                                    (TEST_SCHOOL_ABILITY_INDEX IS NULL AND v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX IS NOT NULL) OR
                                    (TEST_SCHOOL_ABILITY_INDEX IS NOT NULL AND v_testscore_rec.TEST_SCHOOL_ABILITY_INDEX IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_PERCENTILE <> v_testscore_rec.TEST_GROWTH_PERCENTILE) OR
                                    (TEST_GROWTH_PERCENTILE IS NULL AND v_testscore_rec.TEST_GROWTH_PERCENTILE IS NOT NULL) OR
                                    (TEST_GROWTH_PERCENTILE IS NOT NULL AND v_testscore_rec.TEST_GROWTH_PERCENTILE IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_RESULT_CODE <> v_testscore_rec.TEST_GROWTH_RESULT_CODE) OR
                                    (TEST_GROWTH_RESULT_CODE IS NULL AND v_testscore_rec.TEST_GROWTH_RESULT_CODE IS NOT NULL) OR
                                    (TEST_GROWTH_RESULT_CODE IS NOT NULL AND v_testscore_rec.TEST_GROWTH_RESULT_CODE IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_RESULT <> v_testscore_rec.TEST_GROWTH_RESULT) OR
                                    (TEST_GROWTH_RESULT IS NULL AND v_testscore_rec.TEST_GROWTH_RESULT IS NOT NULL) OR
                                    (TEST_GROWTH_RESULT IS NOT NULL AND v_testscore_rec.TEST_GROWTH_RESULT IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_TARGET_1 <> v_testscore_rec.TEST_GROWTH_TARGET_1) OR
                                    (TEST_GROWTH_TARGET_1 IS NULL AND v_testscore_rec.TEST_GROWTH_TARGET_1 IS NOT NULL) OR
                                    (TEST_GROWTH_TARGET_1 IS NOT NULL AND v_testscore_rec.TEST_GROWTH_TARGET_1 IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_TARGET_2 <> v_testscore_rec.TEST_GROWTH_TARGET_2) OR
                                    (TEST_GROWTH_TARGET_2 IS NULL AND v_testscore_rec.TEST_GROWTH_TARGET_2 IS NOT NULL) OR
                                    (TEST_GROWTH_TARGET_2 IS NOT NULL AND v_testscore_rec.TEST_GROWTH_TARGET_2 IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_TARGET_3 <> v_testscore_rec.TEST_GROWTH_TARGET_3) OR
                                    (TEST_GROWTH_TARGET_3 IS NULL AND v_testscore_rec.TEST_GROWTH_TARGET_3 IS NOT NULL) OR
                                    (TEST_GROWTH_TARGET_3 IS NOT NULL AND v_testscore_rec.TEST_GROWTH_TARGET_3 IS NULL)
                                ) OR
                                (
                                    (TEST_GROWTH_TARGET_4 <> v_testscore_rec.TEST_GROWTH_TARGET_4) OR
                                    (TEST_GROWTH_TARGET_4 IS NULL AND v_testscore_rec.TEST_GROWTH_TARGET_4 IS NOT NULL) OR
                                    (TEST_GROWTH_TARGET_4 IS NOT NULL AND v_testscore_rec.TEST_GROWTH_TARGET_4 IS NULL)
                                ) OR              
                                (
                                    (TEST_STANDARD_ERROR <> v_testscore_rec.TEST_STANDARD_ERROR) OR
                                    (TEST_STANDARD_ERROR IS NULL AND v_testscore_rec.TEST_STANDARD_ERROR IS NOT NULL) OR
                                    (TEST_STANDARD_ERROR IS NOT NULL AND v_testscore_rec.TEST_STANDARD_ERROR IS NULL)
                                ) OR
                                (
                                    (TEST_Z_SCORE <> v_testscore_rec.TEST_Z_SCORE) OR
                                    (TEST_Z_SCORE IS NULL AND v_testscore_rec.TEST_Z_SCORE IS NOT NULL) OR
                                    (TEST_Z_SCORE IS NOT NULL AND v_testscore_rec.TEST_Z_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_T_SCORE <> v_testscore_rec.TEST_T_SCORE) OR
                                    (TEST_T_SCORE IS NULL AND v_testscore_rec.TEST_T_SCORE IS NOT NULL) OR
                                    (TEST_T_SCORE IS NOT NULL AND v_testscore_rec.TEST_T_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_STANINE_SCORE <> v_testscore_rec.TEST_STANINE_SCORE) OR
                                    (TEST_STANINE_SCORE IS NULL AND v_testscore_rec.TEST_STANINE_SCORE IS NOT NULL) OR
                                    (TEST_STANINE_SCORE IS NOT NULL AND v_testscore_rec.TEST_STANINE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_QUARTILE_SCORE <> v_testscore_rec.TEST_QUARTILE_SCORE) OR
                                    (TEST_QUARTILE_SCORE IS NULL AND v_testscore_rec.TEST_QUARTILE_SCORE IS NOT NULL) OR
                                    (TEST_QUARTILE_SCORE IS NOT NULL AND v_testscore_rec.TEST_QUARTILE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_DECILE_SCORE <> v_testscore_rec.TEST_DECILE_SCORE) OR
                                    (TEST_DECILE_SCORE IS NULL AND v_testscore_rec.TEST_DECILE_SCORE IS NOT NULL) OR
                                    (TEST_DECILE_SCORE IS NOT NULL AND v_testscore_rec.TEST_DECILE_SCORE IS NULL)
                                ) OR
                                (
                                    (TEST_SCORE_ATTRIBUTES <> v_testscore_rec.TEST_SCORE_ATTRIBUTES) OR
                                    (TEST_SCORE_ATTRIBUTES IS NULL AND v_testscore_rec.TEST_SCORE_ATTRIBUTES IS NOT NULL) OR
                                    (TEST_SCORE_ATTRIBUTES IS NOT NULL AND v_testscore_rec.TEST_SCORE_ATTRIBUTES IS NULL)
                                ) OR
                                (
                                    (TEST_SCORE_TEXT <> v_testscore_rec.TEST_SCORE_TEXT) OR
                                    (TEST_SCORE_TEXT IS NULL AND v_testscore_rec.TEST_SCORE_TEXT IS NOT NULL) OR
                                    (TEST_SCORE_TEXT IS NOT NULL AND v_testscore_rec.TEST_SCORE_TEXT IS NULL)
                                ) OR
                                (
                                    (TEST_STUDENT_GRADE <> v_testscore_rec.TEST_STUDENT_GRADE) OR
                                    (TEST_STUDENT_GRADE IS NULL AND v_testscore_rec.TEST_STUDENT_GRADE IS NOT NULL) OR
                                    (TEST_STUDENT_GRADE IS NOT NULL AND v_testscore_rec.TEST_STUDENT_GRADE IS NULL)
                                ) OR
                                (
                                    (TEST_TEACHER <> v_testscore_rec.TEST_TEACHER) OR
                                    (TEST_TEACHER IS NULL AND v_testscore_rec.TEST_TEACHER IS NOT NULL) OR
                                    (TEST_TEACHER IS NOT NULL AND v_testscore_rec.TEST_TEACHER IS NULL)
                                ) OR
                                (
                                    (TEST_ADMIN_CODE <> v_testscore_rec.TEST_ADMIN_CODE) OR
                                    (TEST_ADMIN_CODE IS NULL AND v_testscore_rec.TEST_ADMIN_CODE IS NOT NULL) OR
                                    (TEST_ADMIN_CODE IS NOT NULL AND v_testscore_rec.TEST_ADMIN_CODE IS NULL)
                                ) OR
                                (
                                    (DISTRICT_CODE <> v_testscore_rec.DISTRICT_CODE) OR
                                    (DISTRICT_CODE IS NULL AND v_testscore_rec.DISTRICT_CODE IS NOT NULL) OR
                                    (DISTRICT_CODE IS NOT NULL AND v_testscore_rec.DISTRICT_CODE IS NULL)
                                ) OR
                                (
                                    (SYS_ETL_SOURCE <> v_testscore_rec.SYS_ETL_SOURCE) OR
                                    (SYS_ETL_SOURCE IS NULL AND v_testscore_rec.SYS_ETL_SOURCE IS NOT NULL) OR
                                    (SYS_ETL_SOURCE IS NOT NULL AND v_testscore_rec.SYS_ETL_SOURCE IS NULL)
                                ) OR
                                (
                                    (SYS_AUDIT_IND <> v_testscore_rec.SYS_AUDIT_IND) OR
                                    (SYS_AUDIT_IND IS NULL AND v_testscore_rec.SYS_AUDIT_IND IS NOT NULL) OR
                                    (SYS_AUDIT_IND IS NOT NULL AND v_testscore_rec.SYS_AUDIT_IND IS NULL)
                                ) OR
                                (
                                    (SYS_PARTITION_VALUE <> v_testscore_rec.SYS_PARTITION_VALUE) OR
                                    (SYS_PARTITION_VALUE IS NULL AND v_testscore_rec.SYS_PARTITION_VALUE IS NOT NULL) OR
                                    (SYS_PARTITION_VALUE IS NOT NULL AND v_testscore_rec.SYS_PARTITION_VALUE IS NULL)
                                )
                            );
                            v_rowsupdated  := v_rowsupdated + SQL%ROWCOUNT;
                            SET_TESTSCORE_AUDIT_IND(v_admin_rec.TEST_admin_key, v_xtbl_testscore_rec.TEST_NUMBER, v_xtbl_testscore_rec.SYS_AUDIT_IND, v_testscore_rec.SYS_AUDIT_IND);
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_ok_to_commit := FALSE;
                                SET_TESTSCORE_AUDIT_IND(v_admin_rec.TEST_admin_key, v_xtbl_testscore_rec.TEST_NUMBER, v_xtbl_testscore_rec.SYS_AUDIT_IND, 'Y');
								v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

								K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
									v_SYS_ETL_SOURCE,
									'UPDATE FTBL_TEST_SCORES',
									v_testscore_rec.TEST_SCORES_KEY,
									v_AUDIT_NATURAL_KEY,
									'UNHANDLED ERROR',
									sqlerrm,
									'Y',
									v_AUDIT_BASE_SEVERITY
								);
                        END; -- update exception handler;
                    ELSE
                        v_ok_to_commit := FALSE;
                    END IF;
                END IF;

                -- commit and update the rows processed
                v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_PROCESSED(1);

                --**************************************************
                --Write the task stats after processing school
                --**************************************************
                BEGIN
                    IF MOD(v_rowcnt, 1000) = 0 THEN
                        K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
						v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || CHR(9) || 'TEST_NUMBER=' || v_xtbl_testscore_rec.TEST_NUMBER;

						K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
							v_SYS_ETL_SOURCE,
							'WRITE TASK STATS',
							v_testscore_rec.TEST_SCORES_KEY,
							v_AUDIT_NATURAL_KEY,
							'UNHANDLED ERROR',
							sqlerrm,
							'Y',
							v_AUDIT_BASE_SEVERITY
						);
                END;

                -- Set global admin audit indicator
                IF v_testscore_rec.SYS_AUDIT_IND = 'Y' THEN
                    v_test_admin_audit_ind := 'Y';
                END IF;

            END LOOP; -- end testscore loop


            IF (v_ok_to_commit = TRUE) THEN
                BEGIN
                    -- NOTE: AUDIT_IND reflects test_Admin's audit status not test_scores
                    -- It is possible for the test_admin audit_ind to = 'N' and its
                    -- related test scores to have their audit_ind set to 'Y'
                    UPDATE K12INTEL_USERDATA.XTBL_TEST_ADMIN
                    SET SYS_RECORD_STAGE = 'LOADED', SYS_AUDIT_IND = v_test_admin_audit_ind
                    WHERE TEST_ADMIN_KEY = v_admin_rec.TEST_ADMIN_KEY  AND (SYS_RECORD_STAGE <> 'LOADED' OR SYS_AUDIT_IND <> v_test_admin_audit_ind);

                    COMMIT;
					v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED(v_rowsInserted);
                    v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(v_rowsUpdated);
                EXCEPTION
                    WHEN OTHERS THEN
                        ROLLBACK;
                        SET_TESTADMIN_AUDIT_IND (v_admin_rec.TEST_ADMIN_KEY, v_test_admin_audit_ind);
						v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

						K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
							v_SYS_ETL_SOURCE,
							'UPDATE XTBL_TEST_ADMIN.SYS_RECORD_STAGE',
							v_testscore_rec.TEST_SCORES_KEY,
							v_AUDIT_NATURAL_KEY,
							'UNHANDLED ERROR',
							sqlerrm,
							'Y',
							v_AUDIT_BASE_SEVERITY
						);
                END;
            ELSE
                ROLLBACK;
                SET_TESTADMIN_AUDIT_IND (v_admin_rec.TEST_ADMIN_KEY, v_test_admin_audit_ind);
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                SET_TESTADMIN_AUDIT_IND (v_admin_rec.TEST_ADMIN_KEY, v_test_admin_audit_ind);
				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
					v_SYS_ETL_SOURCE,
					'TEST_ADMIN LOOP',
					v_testscore_rec.TEST_SCORES_KEY,
					v_AUDIT_NATURAL_KEY,
					'UNHANDLED ERROR',
					sqlerrm,
					'Y',
					v_AUDIT_BASE_SEVERITY
				);
        END;

    END LOOP;


    /*******************************************
        Delete Test Scores
    *******************************************/

    BEGIN

        DELETE K12INTEL_DW.FTBL_TEST_SCORES FTS
        WHERE EXISTS
        (
            SELECT 1
            FROM K12INTEL_USERDATA.XTBL_TEST_ADMIN XTA
                INNER JOIN K12INTEL_USERDATA.XTBL_TEST_SCORES XTS
                    ON (XTA.TEST_ADMIN_KEY = XTS.TEST_ADMIN_KEY)
                INNER JOIN K12INTEL_KEYMAP.KM_TESTSCORES KTS
                    ON (KTS.PROD_TEST_ID = XTA.PROD_TEST_ID AND KTS.TEST_NUMBER = XTS.TEST_NUMBER)
            WHERE FTS.TEST_SCORES_KEY = KTS.SURROGATE_KEY
                AND (XTA.DELETE_TEST_ADMIN_IND = 'Y' OR XTS.DELETE_TEST_SCORE_IND = 'Y')
        );


        v_rowsDeleted  := v_rowsDeleted + SQL%ROWCOUNT;
            

        DELETE K12INTEL_KEYMAP.KM_TESTSCORES KM
        WHERE EXISTS
        (
            SELECT 1
            FROM K12INTEL_USERDATA.XTBL_TEST_ADMIN XTA
                INNER JOIN K12INTEL_USERDATA.XTBL_TEST_SCORES XTS
                    ON (XTA.TEST_ADMIN_KEY = XTS.TEST_ADMIN_KEY)
            WHERE KM.PROD_TEST_ID = XTA.PROD_TEST_ID AND KM.TEST_NUMBER = XTS.TEST_NUMBER
                AND (XTA.DELETE_TEST_ADMIN_IND = 'Y' OR XTS.DELETE_TEST_SCORE_IND = 'Y')
        );

        v_rowsDeleted  := v_rowsDeleted + SQL%ROWCOUNT;

        v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_DELETED(v_rowsDeleted);

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
			v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
				v_SYS_ETL_SOURCE,
				'Delete Test Scores',
				0,
				v_AUDIT_NATURAL_KEY,
				'UNHANDLED ERROR',
				sqlerrm,
				'Y',
				v_AUDIT_BASE_SEVERITY
			);
    END;



    /*******************************************
        Update Test Attempt Number
    *******************************************/
/*
    BEGIN

        UPDATE K12INTEL_DW.FTBL_TEST_SCORES
        SET TEST_ATTEMPT_NUMBER = (
            SELECT ROW_NUMBER() OVER (
                    PARTITION BY a1.STUDENT_KEY
                        , a1.TESTS_KEY
                    ORDER BY b1.DATE_VALUE
                ) "ATTEMPT_NUM"
            FROM K12INTEL_DW.FTBL_TEST_SCORES a1
                INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES b1
                ON (a1.CALENDAR_DATE_KEY = b1.CALENDAR_DATE_KEY)
        )
        WHERE TEST_SCORES_KEY IN (
            SELECT x.TEST_SCORES_KEY
            FROM K12INTEL_DW.FTBL_TEST_SCORES x
                INNER JOIN
                (
                    SELECT
                        TEST_SCORES_KEY,
                        ROW_NUMBER()
                        OVER (PARTITION BY STUDENT_KEY, TESTS_KEY
                        ORDER BY b.DATE_VALUE) "ATTEMPT_NUM"
                    FROM K12INTEL_DW.FTBL_TEST_SCORES a
                        INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES b
                        ON (A.CALENDAR_DATE_KEY = b.CALENDAR_DATE_KEY)
                ) y
                ON (x.TEST_SCORES_KEY = y.TEST_SCORES_KEY)
            WHERE
                (
                    (x.TEST_ATTEMPT_NUMBER <> y.ATTEMPT_NUM) OR
                    (x.TEST_ATTEMPT_NUMBER IS NULL AND y.ATTEMPT_NUM IS NOT NULL) OR
                    (x.TEST_ATTEMPT_NUMBER IS NOT NULL AND y.ATTEMPT_NUM IS NULL)
                )
        );


        v_rowsUpdated  := v_rowsUpdated + SQL%ROWCOUNT;

        v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(v_rowsUpdated);

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
			v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
				v_SYS_ETL_SOURCE,
				'Update the TEST_ATTMPT_NUMBER field',
				0,
				v_AUDIT_NATURAL_KEY,
				'UNHANDLED ERROR',
				sqlerrm,
				'Y',
				v_AUDIT_BASE_SEVERITY
			);
    END;
*/


    /*******************************************
    Write the task stats at end of main loop
    *******************************************/
    BEGIN
        K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
    EXCEPTION
        WHEN OTHERS THEN
			v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
				v_SYS_ETL_SOURCE,
				'WRITE TASK STATS',
				0,
				v_AUDIT_NATURAL_KEY,
				'UNHANDLED ERROR',
				sqlerrm,
				'Y',
				v_AUDIT_BASE_SEVERITY
			);
    END; -- end write task stats
EXCEPTION
    WHEN OTHERS THEN
		v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

		K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
			v_SYS_ETL_SOURCE,
			'TOTAL BUILD FAILURE!',
			0,
			v_AUDIT_NATURAL_KEY,
			'UNHANDLED ERROR',
			sqlerrm,
			'Y',
			v_AUDIT_BASE_SEVERITY
		);

		K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
			v_SYS_ETL_SOURCE,
			'TOTAL BUILD FAILURE!',
			0,
			v_AUDIT_NATURAL_KEY,
			'CALL STACK TRACE',
			DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(),
			'Y',
			v_AUDIT_BASE_SEVERITY
		);



        /*******************************************
        Write the task stats at end of main loop
        *******************************************/
        BEGIN
            K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
        EXCEPTION
            WHEN OTHERS THEN
				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
					v_SYS_ETL_SOURCE,
					'WRITE TASK STATS',
					0,
					'',
					'UNHANDLED ERROR',
					sqlerrm,
					'Y',
					v_AUDIT_BASE_SEVERITY
				);
        END; -- end write task stats

        RAISE;
END BLD_F_TEST_SCORES_XTBL;
/
