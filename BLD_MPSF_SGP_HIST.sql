DROP PROCEDURE K12INTEL_METADATA.BLD_MPSF_SGP_HIST;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_MPSF_SGP_HIST"
(
    p_PARAM_BUILD_ID IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
    p_PARAM_PACKAGE_ID IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
    p_PARAM_TASK_ID IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
    p_PARAM_USE_FULL_REFRESH IN NUMBER,
    p_PARAM_STAGE_SOURCE IN VARCHAR2,
    p_PARAM_MISC_PARAMS   IN VARCHAR2,
    p_PARAM_EXECUTION_STATUS OUT NUMBER
)	
IS
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
         <TARGET SCHEMA="K12INTEL_METADATA" NAME="DTBL_STUDENTS_EVOLVED"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="03/02/2016" USER="WARDB" VERSION="10.6.0"  DESC="Procedure Created"/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_METADATA.DTBL_STUDENTS_EVOLVED WHERE SYS_ETL_SOURCE = 'BLD_D_STUDENTS_EVOL'
     DELETE K12INTEL_METADATA.DTBL_STUDENTS_EVOLVED WHERE SYS_ETL_SOURCE = 'BLD_D_STUDENTS_EVOL'
 */

	PRAGMA AUTONOMOUS_TRANSACTION;

    v_initialize NUMBER(10) := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INITIALIZE(p_PARAM_PACKAGE_ID,p_PARAM_BUILD_ID,p_PARAM_TASK_ID,p_PARAM_USE_FULL_REFRESH);

 	v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_MPSF_SGP_HIST';
 	v_WAREHOUSE_KEY NUMBER(10,0) := 0;
 	v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
 	v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
 	v_AUDIT_NATURAL_KEY VARCHAR2(512) := '';
 	v_BASE_NATURALKEY_TXT VARCHAR(512) := '';

	v_start_time		DATE := sysdate;
	v_rowcnt				NUMBER;

   BEGIN
    
        BEGIN
        
        INSERT INTO K12INTEL_DW.MPSF_SGP_HIST
        SELECT
            tsc.test_scores_key
            ,tsc.calendar_date_key
            ,tsc.school_dates_key
            ,tst.tests_key
            ,tst.test_subject
            ,TSC.STUDENT_KEY
            ,tsc.test_growth_percentile
            ,v_SYS_ETL_SOURCE
            ,v_start_time as sys_created
            ,v_start_time as sys_updated
            ,'N' as SYS_CURRENT_IND
            ,0 as sys_partition_value
        FROM  K12INTEL_DW.FTBL_TEST_SCORES tsc
        INNER JOIN K12INTEL_DW.DTBL_TESTS tst on (tst.tests_key = tsc.tests_key)
        WHERE 1=1
              and  tst.test_name like 'STAR%'
              and tst.test_class = 'Component'
              and tsc.test_growth_target_1 is not null
              and NOT EXISTS (SELECT 1 FROM K12INTEL_DW.MPSF_SGP_HIST sgp 
                              WHERE sgp.test_scores_key = tsc.test_scores_key);
        
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_AUDIT_NATURAL_KEY := '';
                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'INSERT RECORDS',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'No new tests found.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
                        );
            WHEN OTHERS THEN
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_AUDIT_NATURAL_KEY := '';
                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'INSERT RECORDS',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Untrapped Error.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY);
        
        COMMIT;
        
        END;
        
        v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED(SQL%ROWCOUNT);
        
        BEGIN
            
        UPDATE K12INTEL_DW.MPSF_SGP_HIST a
        SET SYS_CURRENT_IND = 'Y'
        WHERE EXISTS (SELECT 1
                      FROM
                        (SELECT 
                            TEST_SCORES_KEY
                            ,student_key
                            ,test_SUBJECT
                            ,ROW_NUMBER() OVER (PARTITION BY student_key, test_subject 
                            ORDER BY cd.date_value desc) as r
                        FROM
                            K12INTEL_DW.MPSF_SGP_HIST b 
                              INNER JOIN K12INTEL_DW.DTBL_CALENDAR_DATES cd 
                                        on cd.calendar_date_key = b.calendar_date_key) top
                       WHERE top.r = 1 and top.test_scores_key = a.test_scores_key)
                       ;   
        EXCEPTION
        WHEN OTHERS THEN
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_AUDIT_NATURAL_KEY := '';
                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'UPDATE CURRENT IND',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'Failed to update current ind.',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY);
        
        COMMIT;  
        
        END;     
        
        v_STAT_ROWS_PROCESSED := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(SQL%ROWCOUNT);
        
         BEGIN
            K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := '';

                  K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'WRITE TASK STATS',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

         COMMIT;    
   EXCEPTION
    WHEN OTHERS THEN
        v_WAREHOUSE_KEY := 0;
        v_AUDIT_BASE_SEVERITY := 0;
        v_AUDIT_NATURAL_KEY := '';
        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
            v_SYS_ETL_SOURCE,
            'WRITE TASK STATS',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'Untrapped Error.',
            sqlerrm,
            'Y',
            v_AUDIT_BASE_SEVERITY);                             
   END;
    /
