DROP PROCEDURE K12INTEL_METADATA.BLD_D_CURRICULUM_GT_IC;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_D_CURRICULUM_GT_IC"
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
 
 	v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_D_CURRICULUM_GT_IC';
 	v_WAREHOUSE_KEY NUMBER(10,0) := 0;
 	v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
 	v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
 	v_AUDIT_NATURAL_KEY VARCHAR2(512) := '';
 	v_BASE_NATURALKEY_TXT VARCHAR(512) := '';
 
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
     <CONNECTOR>IC</CONNECTOR>
     <QUALIFIER></QUALIFIER>
     <TARGETS>
         <TARGET SCHEMA="K12INTEL_METADATA" NAME="DTBL_CURRICULUM"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="06/20/2012" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_METADATA.DTBL_CURRICULUM WHERE SYS_ETL_SOURCE = 'BLD_D_CURRICULUM_GT_IC'
     DELETE K12INTEL_METADATA.DTBL_CURRICULUM WHERE SYS_ETL_SOURCE = 'BLD_D_CURRICULUM_GT_IC'
 */

 
 -- Auditing realted variables
 v_table_id NUMBER(10) := 0;
 
 -- Local variables and cursors
 v_start_time 	DATE := sysdate;
 v_rowcnt 	NUMBER;
 v_existing_CURRICULUM_KEY	 NUMBER(10);
 v_curriculum_record	K12INTEL_DW.DTBL_CURRICULUM%ROWTYPE;
 
 CURSOR c_some_cursor IS
 	SELECT 
		  gt.STAGE_SOURCE 
        , gt.TASKID 
        , gt.CODE 
        , gt."NUMBER" 
        , gt."NAME" 
        , gt.DESCRIPTION 
        , gt.STANDARDID 
        , curr.PARENTID 
        , curr.DISTRICTID 
        , curr."NAME" STANDARD_NAME 
        , curr.DESCRIPTION STANDARD_DESCRIPTION 
        , curr_parent."NAME" STANDARD_PARENT_NAME 
        , curr.DESCRIPTION STANDARD_PARENT_DESCRIPTION 
        , dis."NUMBER" DISTRICT_CODE
    FROM K12INTEL_STAGING_IC.GRADINGTASK gt
	INNER JOIN K12INTEL_STAGING_IC.CURRICULUMSTANDARD curr
		ON gt.STANDARDID = curr.STANDARDID 
		AND gt.STAGE_SOURCE = curr.STAGE_SOURCE
	LEFT JOIN K12INTEL_STAGING_IC.CURRICULUMSTANDARD curr_parent
		ON curr.PARENTID = curr_parent.STANDARDID 
		AND curr.STAGE_SOURCE = curr_parent.STAGE_SOURCE
	INNER JOIN K12INTEL_STAGING_IC.DISTRICT dis
		ON curr.DISTRICTID = dis.DISTRICTID 
		AND curr.STAGE_SOURCE = dis.STAGE_SOURCE		
	WHERE gt.STAGE_SOURCE = p_PARAM_STAGE_SOURCE;
 BEGIN
 
 	FOR v_source_data IN c_some_cursor LOOP
 	BEGIN
        ---------------------------------------------------------------
        -- SYSTEM VARIABLES
        ---------------------------------------------------------------
        v_BASE_NATURALKEY_TXT := 'STAGE_SOURCE=' || v_source_data.STAGE_SOURCE || ';TASKID=' || v_source_data.TASKID;
        v_curriculum_record.SYS_CREATED := SYSDATE;
        v_curriculum_record.SYS_UPDATED := SYSDATE;
        v_curriculum_record.SYS_AUDIT_IND := 'N';
        v_curriculum_record.SYS_DUMMY_IND := 'N';
        v_curriculum_record.SYS_PARTITION_VALUE := 0;
        v_curriculum_record.SYS_ETL_SOURCE := v_SYS_ETL_SOURCE;

 		---------------------------------------------------------------
 		-- CURRICULUM_KEY
 		---------------------------------------------------------------
 		BEGIN
            K12INTEL_METADATA.GEN_CURRICULUM_KEY_GT_IC(
                v_curriculum_record.CURRICULUM_KEY,
                v_source_data.TASKID,
                v_source_data.STAGE_SOURCE
            );

            IF v_curriculum_record.CURRICULUM_KEY = 0 THEN
				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'ERROR GENERATING KEY',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);

                RAISE_APPLICATION_ERROR(-20000, 'FAILED TO GENERATE CURRICULUM_KEY');
            END IF;
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_CODE
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_CODE := v_source_data.TASKID;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_1
        -- Modified 4/14/15 to get consistent Curriculum Level 1 BW
 		---------------------------------------------------------------
 		BEGIN
--            IF v_source_data.STANDARD_PARENT_NAME IS NOT NULL THEN
--                v_curriculum_record.CURRICULUM_LEVEL_1 := trim(v_source_data.STANDARD_PARENT_NAME);
--            ELSIF v_source_data.STANDARD_NAME IS NOT NULL THEN
--                v_curriculum_record.CURRICULUM_LEVEL_1 := trim(v_source_data.STANDARD_NAME);
--            ELSE
--                v_curriculum_record.CURRICULUM_LEVEL_1 := trim(v_source_data."NAME");
--            END IF;  --BW 4/14/15
 			v_curriculum_record.CURRICULUM_LEVEL_1 := trim(COALESCE(v_source_data.STANDARD_PARENT_NAME, v_source_data.STANDARD_NAME, v_source_data."NAME"));
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_1 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_1',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_1 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_1',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_1 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_1',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_2
        -- Modified 4/14/15 to make grading task name the primary Curriculum Level 2
 		---------------------------------------------------------------
 		BEGIN
--            IF v_source_data.STANDARD_PARENT_NAME IS NOT NULL and v_source_data.STANDARD_NAME IS NOT NULL THEN
--                v_curriculum_record.CURRICULUM_LEVEL_2 := trim(v_source_data.STANDARD_NAME);
--            ELSIF v_source_data.STANDARD_NAME IS NOT NULL THEN
--                v_curriculum_record.CURRICULUM_LEVEL_2 := trim(v_source_data."NAME");
--            ELSE
--                v_curriculum_record.CURRICULUM_LEVEL_2 := '--';
--            END IF;
            v_curriculum_record.CURRICULUM_LEVEL_2 := trim(COALESCE(v_source_data."NAME", v_source_data.STANDARD_NAME, v_source_data.STANDARD_PARENT_NAME));
 			--v_curriculum_record.CURRICULUM_LEVEL_2 := CASE WHEN trim(v_source_data.STANDARD_NAME) IS NOT NULL THEN trim(v_source_data.STANDARD_NAME) ELSE v_source_data."NAME" END;
            --v_curriculum_record.CURRICULUM_LEVEL_2 := CASE WHEN v_curriculum_record.CURRICULUM_LEVEL_1 <> trim(v_source_data.STANDARD_NAME) THEN trim(v_source_data.STANDARD_NAME) ELSE v_source_data."NAME" END;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_2 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_2',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_2 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_2',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_2 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_2',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_3
 		---------------------------------------------------------------
 		BEGIN
            v_curriculum_record.CURRICULUM_LEVEL_3 := 'NA';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_3 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_3',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_3 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_3',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_3 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_3',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_4
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_LEVEL_4 := 'NA';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_4 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_4',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_4 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_4',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_4 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_4',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_5
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_LEVEL_5 := 'NA';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_5 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_5',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_5 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_5',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_5 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_5',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_LEVEL_6
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_LEVEL_6 := 'NA';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_6 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_6',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_6 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_6',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_LEVEL_6 := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_LEVEL_6',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_GRADE_LEVEL
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_GRADE_LEVEL := '--';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_GRADE_LEVEL := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_GRADE_LEVEL',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_GRADE_LEVEL := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_GRADE_LEVEL',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_GRADE_LEVEL := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_GRADE_LEVEL',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_DESCRIPTION
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_DESCRIPTION := '--';
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DESCRIPTION := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DESCRIPTION',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DESCRIPTION := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DESCRIPTION',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DESCRIPTION := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DESCRIPTION',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_COMMENTS
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_COMMENTS := NULL;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_COMMENTS := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_COMMENTS',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_COMMENTS := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_COMMENTS',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_COMMENTS := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_COMMENTS',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_DEPTH
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_DEPTH := 0;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DEPTH := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DEPTH',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DEPTH := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DEPTH',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_DEPTH := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_DEPTH',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- CURRICULUM_SORT
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.CURRICULUM_SORT := 0;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_SORT := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_SORT',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_SORT := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_SORT',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.CURRICULUM_SORT := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'CURRICULUM_SORT',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		---------------------------------------------------------------
 		-- DISTRICT_CODE
 		---------------------------------------------------------------
 		BEGIN
 			v_curriculum_record.DISTRICT_CODE := v_source_data.DISTRICT_CODE;
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.DISTRICT_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'DISTRICT_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.DISTRICT_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'DISTRICT_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_curriculum_record.SYS_AUDIT_IND := 'Y';
 				v_curriculum_record.DISTRICT_CODE := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'DISTRICT_CODE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
 
 
 		
 
 		BEGIN
 			SELECT CURRICULUM_KEY INTO v_existing_CURRICULUM_KEY
 			FROM K12INTEL_DW.DTBL_CURRICULUM
 			WHERE CURRICULUM_KEY = v_curriculum_record.CURRICULUM_KEY;
 			BEGIN
 				UPDATE K12INTEL_DW.DTBL_CURRICULUM
 				SET
 					CURRICULUM_CODE=			v_curriculum_record.CURRICULUM_CODE,
 					CURRICULUM_LEVEL_1=			v_curriculum_record.CURRICULUM_LEVEL_1,
 					CURRICULUM_LEVEL_2=			v_curriculum_record.CURRICULUM_LEVEL_2,
 					CURRICULUM_LEVEL_3=			v_curriculum_record.CURRICULUM_LEVEL_3,
 					CURRICULUM_LEVEL_4=			v_curriculum_record.CURRICULUM_LEVEL_4,
 					CURRICULUM_LEVEL_5=			v_curriculum_record.CURRICULUM_LEVEL_5,
 					CURRICULUM_LEVEL_6=			v_curriculum_record.CURRICULUM_LEVEL_6,
 					CURRICULUM_GRADE_LEVEL=			v_curriculum_record.CURRICULUM_GRADE_LEVEL,
 					CURRICULUM_DESCRIPTION=			v_curriculum_record.CURRICULUM_DESCRIPTION,
 					CURRICULUM_COMMENTS=			v_curriculum_record.CURRICULUM_COMMENTS,
 					CURRICULUM_DEPTH=			v_curriculum_record.CURRICULUM_DEPTH,
 					CURRICULUM_SORT=			v_curriculum_record.CURRICULUM_SORT,
                    CURRICULUM_URL_1=			v_curriculum_record.CURRICULUM_URL_1,
                    CURRICULUM_URL_2=			v_curriculum_record.CURRICULUM_URL_2,
 					DISTRICT_CODE=			v_curriculum_record.DISTRICT_CODE,
 					SYS_ETL_SOURCE=			v_curriculum_record.SYS_ETL_SOURCE,
 					SYS_UPDATED=			v_curriculum_record.SYS_UPDATED,
 					SYS_AUDIT_IND=			v_curriculum_record.SYS_AUDIT_IND,
 					SYS_DUMMY_IND=			v_curriculum_record.SYS_DUMMY_IND,
 					SYS_PARTITION_VALUE=			v_curriculum_record.SYS_PARTITION_VALUE
 				WHERE
 					CURRICULUM_KEY = v_existing_CURRICULUM_KEY AND
 					(
 						(
 							(v_curriculum_record.CURRICULUM_CODE <> CURRICULUM_CODE) OR
 							(v_curriculum_record.CURRICULUM_CODE IS NOT NULL AND CURRICULUM_CODE IS NULL) OR
 							(v_curriculum_record.CURRICULUM_CODE IS NULL AND CURRICULUM_CODE IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_1 <> CURRICULUM_LEVEL_1) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_1 IS NOT NULL AND CURRICULUM_LEVEL_1 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_1 IS NULL AND CURRICULUM_LEVEL_1 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_2 <> CURRICULUM_LEVEL_2) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_2 IS NOT NULL AND CURRICULUM_LEVEL_2 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_2 IS NULL AND CURRICULUM_LEVEL_2 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_3 <> CURRICULUM_LEVEL_3) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_3 IS NOT NULL AND CURRICULUM_LEVEL_3 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_3 IS NULL AND CURRICULUM_LEVEL_3 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_4 <> CURRICULUM_LEVEL_4) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_4 IS NOT NULL AND CURRICULUM_LEVEL_4 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_4 IS NULL AND CURRICULUM_LEVEL_4 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_5 <> CURRICULUM_LEVEL_5) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_5 IS NOT NULL AND CURRICULUM_LEVEL_5 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_5 IS NULL AND CURRICULUM_LEVEL_5 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_LEVEL_6 <> CURRICULUM_LEVEL_6) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_6 IS NOT NULL AND CURRICULUM_LEVEL_6 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_LEVEL_6 IS NULL AND CURRICULUM_LEVEL_6 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_GRADE_LEVEL <> CURRICULUM_GRADE_LEVEL) OR
 							(v_curriculum_record.CURRICULUM_GRADE_LEVEL IS NOT NULL AND CURRICULUM_GRADE_LEVEL IS NULL) OR
 							(v_curriculum_record.CURRICULUM_GRADE_LEVEL IS NULL AND CURRICULUM_GRADE_LEVEL IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_DESCRIPTION <> CURRICULUM_DESCRIPTION) OR
 							(v_curriculum_record.CURRICULUM_DESCRIPTION IS NOT NULL AND CURRICULUM_DESCRIPTION IS NULL) OR
 							(v_curriculum_record.CURRICULUM_DESCRIPTION IS NULL AND CURRICULUM_DESCRIPTION IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_COMMENTS <> CURRICULUM_COMMENTS) OR
 							(v_curriculum_record.CURRICULUM_COMMENTS IS NOT NULL AND CURRICULUM_COMMENTS IS NULL) OR
 							(v_curriculum_record.CURRICULUM_COMMENTS IS NULL AND CURRICULUM_COMMENTS IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_DEPTH <> CURRICULUM_DEPTH) OR
 							(v_curriculum_record.CURRICULUM_DEPTH IS NOT NULL AND CURRICULUM_DEPTH IS NULL) OR
 							(v_curriculum_record.CURRICULUM_DEPTH IS NULL AND CURRICULUM_DEPTH IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_SORT <> CURRICULUM_SORT) OR
 							(v_curriculum_record.CURRICULUM_SORT IS NOT NULL AND CURRICULUM_SORT IS NULL) OR
 							(v_curriculum_record.CURRICULUM_SORT IS NULL AND CURRICULUM_SORT IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_URL_1 <> CURRICULUM_URL_1) OR
 							(v_curriculum_record.CURRICULUM_URL_1 IS NOT NULL AND CURRICULUM_URL_1 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_URL_1 IS NULL AND CURRICULUM_URL_1 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.CURRICULUM_URL_2 <> CURRICULUM_URL_2) OR
 							(v_curriculum_record.CURRICULUM_URL_2 IS NOT NULL AND CURRICULUM_URL_2 IS NULL) OR
 							(v_curriculum_record.CURRICULUM_URL_2 IS NULL AND CURRICULUM_URL_2 IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.DISTRICT_CODE <> DISTRICT_CODE) OR
 							(v_curriculum_record.DISTRICT_CODE IS NOT NULL AND DISTRICT_CODE IS NULL) OR
 							(v_curriculum_record.DISTRICT_CODE IS NULL AND DISTRICT_CODE IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.SYS_ETL_SOURCE <> SYS_ETL_SOURCE) OR
 							(v_curriculum_record.SYS_ETL_SOURCE IS NOT NULL AND SYS_ETL_SOURCE IS NULL) OR
 							(v_curriculum_record.SYS_ETL_SOURCE IS NULL AND SYS_ETL_SOURCE IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.SYS_AUDIT_IND <> SYS_AUDIT_IND) OR
 							(v_curriculum_record.SYS_AUDIT_IND IS NOT NULL AND SYS_AUDIT_IND IS NULL) OR
 							(v_curriculum_record.SYS_AUDIT_IND IS NULL AND SYS_AUDIT_IND IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.SYS_DUMMY_IND <> SYS_DUMMY_IND) OR
 							(v_curriculum_record.SYS_DUMMY_IND IS NOT NULL AND SYS_DUMMY_IND IS NULL) OR
 							(v_curriculum_record.SYS_DUMMY_IND IS NULL AND SYS_DUMMY_IND IS NOT NULL)
 						) OR
 						(
 							(v_curriculum_record.SYS_PARTITION_VALUE <> SYS_PARTITION_VALUE) OR
 							(v_curriculum_record.SYS_PARTITION_VALUE IS NOT NULL AND SYS_PARTITION_VALUE IS NULL) OR
 							(v_curriculum_record.SYS_PARTITION_VALUE IS NULL AND SYS_PARTITION_VALUE IS NOT NULL)
 						)
 					);
 				v_rowcnt := SQL%ROWCOUNT;
 				COMMIT;
 				v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(v_rowcnt);
 			EXCEPTION
 				WHEN OTHERS THEN
 					ROLLBACK;
 					v_WAREHOUSE_KEY := 0;
 					v_AUDIT_BASE_SEVERITY := 0;
 					v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 					K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 						v_SYS_ETL_SOURCE,
 						'INSERT/COMMIT',
 						v_WAREHOUSE_KEY,
 						v_AUDIT_NATURAL_KEY,
 						'Untrapped Error',
 						sqlerrm,
 						'Y',
 						v_AUDIT_BASE_SEVERITY
 					);
 			END;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				BEGIN
 					-- Insert new record
 					INSERT INTO K12INTEL_DW.DTBL_CURRICULUM
 					VALUES (
 						v_curriculum_record.CURRICULUM_KEY,			--CURRICULUM_KEY
 						v_curriculum_record.CURRICULUM_CODE,			--CURRICULUM_CODE
 						v_curriculum_record.CURRICULUM_LEVEL_1,			--CURRICULUM_LEVEL_1
 						v_curriculum_record.CURRICULUM_LEVEL_2,			--CURRICULUM_LEVEL_2
 						v_curriculum_record.CURRICULUM_LEVEL_3,			--CURRICULUM_LEVEL_3
 						v_curriculum_record.CURRICULUM_LEVEL_4,			--CURRICULUM_LEVEL_4
 						v_curriculum_record.CURRICULUM_LEVEL_5,			--CURRICULUM_LEVEL_5
 						v_curriculum_record.CURRICULUM_LEVEL_6,			--CURRICULUM_LEVEL_6
 						v_curriculum_record.CURRICULUM_GRADE_LEVEL,			--CURRICULUM_GRADE_LEVEL
 						v_curriculum_record.CURRICULUM_DESCRIPTION,			--CURRICULUM_DESCRIPTION
 						v_curriculum_record.CURRICULUM_COMMENTS,			--CURRICULUM_COMMENTS
 						v_curriculum_record.CURRICULUM_DEPTH,			--CURRICULUM_DEPTH
 						v_curriculum_record.CURRICULUM_SORT,			--CURRICULUM_SORT
 						v_curriculum_record.CURRICULUM_URL_1,			--CURRICULUM_URL_1
 						v_curriculum_record.CURRICULUM_URL_2,			--CURRICULUM_URL_2
 						v_curriculum_record.DISTRICT_CODE,			--DISTRICT_CODE
 						v_curriculum_record.SYS_ETL_SOURCE,			--SYS_ETL_SOURCE
 						SYSDATE,			-- SYS_CREATED
 						SYSDATE,			-- SYS_UPDATED
 						v_curriculum_record.SYS_AUDIT_IND,			--SYS_AUDIT_IND
 						v_curriculum_record.SYS_DUMMY_IND,			-- SYS_DUMMY_IND
 						v_curriculum_record.SYS_PARTITION_VALUE			--SYS_PARTITION_VALUE
 					);
                    v_rowcnt := SQL%ROWCOUNT;
 					COMMIT;

 					v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED(v_rowcnt);
 				EXCEPTION
 					WHEN OTHERS THEN
 						ROLLBACK;
 						v_WAREHOUSE_KEY := 0;
 						v_AUDIT_BASE_SEVERITY := 0;
 						
 						v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 						K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 							v_SYS_ETL_SOURCE,
 							'INSERT/COMMIT',
 							v_WAREHOUSE_KEY,
 							v_AUDIT_NATURAL_KEY,
 							'Untrapped Error',
 							sqlerrm,
 							'Y',
 							v_AUDIT_BASE_SEVERITY
 						);
 				END;
 			WHEN OTHERS THEN
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
 				K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'INSERT/COMMIT',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 				RAISE;
 		END;
 		v_STAT_ROWS_PROCESSED := v_STAT_ROWS_PROCESSED + 1;
 		v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_PROCESSED(1);
 		-- Update the stats every 1000 records
 		IF MOD(v_STAT_ROWS_PROCESSED, 1000) = 0 THEN
 			-- Write task stats
 			BEGIN
 				K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
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
 						'Untrapped Error',
 						sqlerrm,
 						'Y',
 						v_AUDIT_BASE_SEVERITY
 					);
 			END;
 		END IF;
 	EXCEPTION  -- Loop expection handler
 		WHEN OTHERS THEN
 			v_WAREHOUSE_KEY := 0;
 			v_AUDIT_BASE_SEVERITY := 0;
 			v_AUDIT_NATURAL_KEY := '';
 
 			K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 				v_SYS_ETL_SOURCE,
 				'CURSOR LOOP',
 				v_WAREHOUSE_KEY,
 				v_AUDIT_NATURAL_KEY,
 				'Untrapped Error',
 				sqlerrm,
 				'Y',
 				v_AUDIT_BASE_SEVERITY
 			);
 	END;
 	END LOOP;
 	DBMS_OUTPUT.PUT_LINE(TRUNC((sysdate - v_start_time)*24*60*60));
 	-- Write task stats
 	BEGIN
 		K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
 	EXCEPTION
 		WHEN OTHERS THEN
 			v_WAREHOUSE_KEY := 0;
 			v_AUDIT_BASE_SEVERITY := 0;
 			v_AUDIT_NATURAL_KEY := '';
 
 			K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 				v_SYS_ETL_SOURCE,
 				'CURSOR LOOP',
 				v_WAREHOUSE_KEY,
 				v_AUDIT_NATURAL_KEY,
 				'Untrapped Error',
 				sqlerrm,
 				'Y',
 				v_AUDIT_BASE_SEVERITY
 			);
 	END;
 EXCEPTION
 	WHEN OTHERS THEN
 		v_WAREHOUSE_KEY := 0;
 		v_AUDIT_BASE_SEVERITY := 0;
 		v_AUDIT_NATURAL_KEY := '';
 		P_PARAM_EXECUTION_STATUS := 1;
 
 		K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 			v_SYS_ETL_SOURCE,
 			'TOTAL BUILD FAILURE!',
 			v_WAREHOUSE_KEY,
 			v_AUDIT_NATURAL_KEY,
 			'Untrapped Error',
 			sqlerrm,
 			'Y',
 			v_AUDIT_BASE_SEVERITY
 		);
 	-- Write task stats
 	BEGIN
 		K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
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
 				'Untrapped Error',
 				sqlerrm,
 				'Y',
 				v_AUDIT_BASE_SEVERITY
 			);
 	END;
 	RAISE;
 END;
/
