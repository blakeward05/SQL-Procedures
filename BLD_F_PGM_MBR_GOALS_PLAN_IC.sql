DROP PROCEDURE K12INTEL_METADATA.BLD_F_PGM_MBR_GOALS_PLAN_IC;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_F_PGM_MBR_GOALS_PLAN_IC"
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
 
 	v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_F_PGM_MBR_GOALS_PLAN_IC';
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
         <TARGET SCHEMA="K12INTEL_DW" NAME="FTBL_PROGRAM_MEMBERSHIP_GOALS"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="03/26/2014" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS WHERE SYS_ETL_SOURCE = 'BLD_F_PGM_MBR_GOALS_PLAN_IC'
     DELETE K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS WHERE SYS_ETL_SOURCE = 'BLD_F_PGM_MBR_GOALS_PLAN_IC'
*/

 -- Auditing realted variables
 v_table_id NUMBER(10) := 0;

 -- Local variables and cursors
 v_start_time 	DATE := sysdate;
 v_rowcnt 	NUMBER;
 v_existing_MEMBERSHIP_GOAL_	 NUMBER(10);
 v_membership_record	K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS%ROWTYPE;

 CURSOR c_goal_cursor IS
    SELECT 
        --MEMBERSHIP_GOAL_KEY, 
        z.MEMBERSHIP_KEY, 
        PROGRAM_KEY, 
        INTERVENTIONS_KEY, 
        STUDENT_KEY, 
        STUDENT_ANNUAL_ATTRIBS_KEY, 
        FACILITIES_KEY, 
        SCHOOL_KEY, 
        SCHOOL_ANNUAL_ATTRIBS_KEY, 
        0 as STAFF_KEY, 
        z.BEGIN_DATE_KEY as START_DATE, 
        z.END_DATE_KEY as END_DATE, 
        "NAME" as GOAL_NAME, 
        goalscore as TARGET_VALUE, 
        BASESCORE as BASELINE_VALUE, 
        DISTRICT_CODE,
        x.GOALID,
        x.STAGE_SOURCE
    FROM k12intel_dw.FTBL_PROGRAM_MEMBERSHIP z
    INNER join k12intel_keymap.KM_PROG_MBRSHP_PLAN_IC y 
    on z.MEMBERSHIP_KEY=y.MEMBERSHIP_KEY
    INNER JOIN (
        SELECT a.PLANID, d.PROVIDEDID, a.STAGE_SOURCE, h.GOALID, h."NAME", g.reportmethod,h.BASESCORE,h.goalscore--, ROW_NUMBER() over(partition by PLANID, y.[NAME], y.GOALSCORE, y.BASESCORE order by GOALID ASC) r 
        FROM k12intel_Staging_ic.PLAN a
        inner join k12intel_Staging_ic.PlanType b
        on a.TYPEID = b.TYPEID and a.STAGE_SOURCE = b.STAGE_SOURCE
        left join k12intel_staging_ic.PLANSERVICEPROVIDED d
        on a.PLANID = d.PLANID and a.STAGE_SOURCE = d.STAGE_SOURCE
        inner join k12intel_staging_ic.PERSON e
        on a.PERSONID = e.PERSONID and a.STAGE_SOURCE = e.STAGE_SOURCE
        inner join k12intel_staging_ic.DISTRICT f
        on a.DISTRICTID = f.DISTRICTID and a.STAGE_SOURCE = f.STAGE_SOURCE
        inner join k12intel_staging_ic.SEPLAN g
        on a.PLANID = g.PLANID and a.STAGE_SOURCE = g.STAGE_SOURCE
        inner join k12intel_staging_ic.plangoal h
        on a.PLANID = h.PLANID and a.STAGE_SOURCE = h.STAGE_SOURCE
--        inner join k12intel_staging_ic.INTERVENTIONDELIVERY i
--        on h.GOALID = i.GOALID and a.STAGE_SOURCE = i.STAGE_SOURCE
        WHERE 1=1
          and a.STAGE_SOURCE = 'MPS_IC'
          and b.MODULE in ('lprti')
    ) x 
    on y.PLANID = x.PLANID and y.PROVIDEDID = x.PROVIDEDID and y.STAGE_SOURCE = x.STAGE_SOURCE
    WHERE SYS_ETL_SOURCE = 'BLD_F_PROG_MBRSHP_PLAN_IC';
    --and rownum < 100;
 BEGIN
    --EXECUTE IMMEDIATE('TRUNCATE TABLE K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS');

 	FOR v_some_data IN c_goal_cursor LOOP
 	BEGIN
dbms_output.put_line('GOALID=' || TO_CHAR(v_some_data.GOALID) || ';MEMBERSHIP_KEY=' || NVL(TO_CHAR(v_some_data.MEMBERSHIP_KEY),'[NULL]'));
	   	v_membership_record.SYS_ETL_SOURCE := v_SYS_ETL_SOURCE;
	   	v_membership_record.DISTRICT_CODE := '3619';
	   	v_membership_record.sys_created := sysdate;
		v_membership_record.sys_updated := sysdate;
		v_membership_record.sys_audit_ind := 'N';
		v_membership_record.sys_partition_value := 0;

		v_BASE_NATURALKEY_TXT := 'GOALID=' || TO_CHAR(v_some_data.GOALID) || ';MEMBERSHIP_KEY=' || NVL(TO_CHAR(v_some_data.MEMBERSHIP_KEY),'[NULL]');

 		---------------------------------------------------------------
 		-- MEMBERSHIP_GOAL_KEY
 		---------------------------------------------------------------
 		BEGIN
            K12INTEL_METADATA.GEN_PROG_MBRSHP_GOAL_PLAN_KEY
            (
                v_membership_record.MEMBERSHIP_GOAL_KEY, 
                v_some_data.MEMBERSHIP_KEY, 
                v_some_data.GOALID, 
                v_some_data.STAGE_SOURCE  
            );

            IF v_membership_record.MEMBERSHIP_GOAL_KEY = 0 THEN
                v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.MEMBERSHIP_GOAL_KEY := 0;
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                    v_SYS_ETL_SOURCE,
                    'MEMBERSHIP_GOAL_KEY',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'ERROR GENERATING KEY',
                    sqlerrm,
                    'Y',
                    v_AUDIT_BASE_SEVERITY
                );

                RAISE_APPLICATION_ERROR(-20000, 'FAILED TO GENERATE MEMBERSHIP_GOAL_KEY');
            END IF;
        /*EXCEPTION 	
            WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.MEMBERSHIP_GOAL_KEY := 0;
                v_membership_record.INTERVENTIONS_KEY := -1;
                v_membership_record.STUDENT_KEY := 0;
                v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY := -1;
                v_membership_record.FACILITIES_KEY := 0;
                v_membership_record.SCHOOL_KEY := 0;
                v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'MEMBERSHIP_GOAL_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 		END;
dbms_output.put_line('AFTER GEN');

 		---------------------------------------------------------------
 		-- MEMBERSHIP_KEY
 		---------------------------------------------------------------
 		BEGIN
            v_membership_record.MEMBERSHIP_KEY := v_some_data.MEMBERSHIP_KEY;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.MEMBERSHIP_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'MEMBERSHIP_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.MEMBERSHIP_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'MEMBERSHIP_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.MEMBERSHIP_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'MEMBERSHIP_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;



 		---------------------------------------------------------------
 		-- PROGRAM_KEY
 		---------------------------------------------------------------
 		BEGIN
            v_membership_record.PROGRAM_KEY := v_some_data.PROGRAM_KEY;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.PROGRAM_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'PROGRAM_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.PROGRAM_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'PROGRAM_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.PROGRAM_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'PROGRAM_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;

         ---------------------------------------------------------------
         -- INTERVENTION_KEY
         ---------------------------------------------------------------
         BEGIN
            v_membership_record.interventions_key := v_some_data.interventions_key;
         EXCEPTION
            /*WHEN NO_DATA_FOUND THEN
                v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.INTERVENTION_KEY := 0;
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'INTERVENTION_KEY',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'NO_DATA_FOUND',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            WHEN TOO_MANY_ROWS THEN
                v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.INTERVENTION_KEY := 0;
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'INTERVENTION_KEY',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'TOO_MANY_ROWS',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );*/
            WHEN OTHERS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.interventions_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_dberr_count := v_stat_dberr_count + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'INTERVENTION_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'Untrapped Error',
                                              SQLERRM,
                                              'Y',
                                              v_audit_base_severity);
         END;

 		---------------------------------------------------------------
 		-- STUDENT_KEY
 		---------------------------------------------------------------
 		BEGIN
 			v_membership_record.STUDENT_KEY := v_some_data.STUDENT_KEY;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STUDENT_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STUDENT_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STUDENT_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STUDENT_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STUDENT_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STUDENT_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;

         ---------------------------------------------------------------
         -- STUDENT_ANNUAL_ATTRIBS_KEY
         ---------------------------------------------------------------
         BEGIN
            v_membership_record.student_annual_attribs_key := v_some_data.student_annual_attribs_key;
         EXCEPTION
            /*WHEN NO_DATA_FOUND THEN
                v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'STUDENT_ANNUAL_ATTRIBS_KEY',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'NO_DATA_FOUND',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );
            WHEN TOO_MANY_ROWS THEN
                v_membership_record.SYS_AUDIT_IND := 'Y';
                v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                v_WAREHOUSE_KEY := 0;
                v_AUDIT_BASE_SEVERITY := 0;
                v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                K12INTEL_METADATA.WRITE_AUDIT(
                    p_PARAM_BUILD_ID,
                    p_PARAM_PACKAGE_ID,
                    p_PARAM_TASK_ID,
                    v_SYS_ETL_SOURCE,
                    'STUDENT_ANNUAL_ATTRIBS_KEY',
                    v_WAREHOUSE_KEY,
                    v_AUDIT_NATURAL_KEY,
                    'TOO_MANY_ROWS',
                    sqlerrm,
                    'N',
                    v_AUDIT_BASE_SEVERITY
                );*/
            WHEN OTHERS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.student_annual_attribs_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_dberr_count := v_stat_dberr_count + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'STUDENT_ANNUAL_ATTRIBS_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'Untrapped Error',
                                              SQLERRM,
                                              'Y',
                                              v_audit_base_severity);
         END;

         ---------------------------------------------------------------
         -- SCHOOL_KEY, FACILITIES_KEY
         ---------------------------------------------------------------
         BEGIN
            v_membership_record.school_key :=     v_some_data.school_key;
            v_membership_record.facilities_key := v_some_data.facilities_key;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_key := 0;
               v_membership_record.facilities_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_rows_audited := v_stat_rows_audited + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_KEY,FACILITIES_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'NO_DATA_FOUND',
                                              SQLERRM,
                                              'N',
                                              v_audit_base_severity);
            WHEN TOO_MANY_ROWS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_key := 0;
               v_membership_record.facilities_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_rows_audited := v_stat_rows_audited + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_KEY,FACILITIES_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'TOO_MANY_ROWS',
                                              SQLERRM,
                                              'N',
                                              v_audit_base_severity);
            WHEN OTHERS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_key := 0;
               v_membership_record.facilities_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_dberr_count := v_stat_dberr_count + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_KEY,FACILITIES_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'Untrapped Error',
                                              SQLERRM,
                                              'Y',
                                              v_audit_base_severity);
         END;


         ---------------------------------------------------------------
         -- SCHOOL_ANNUAL_ATTRIBS_KEY
         ---------------------------------------------------------------
         BEGIN
            v_membership_record.school_annual_attribs_key := v_some_data.school_annual_attribs_key ;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_annual_attribs_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_rows_audited := v_stat_rows_audited + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_ANNUAL_ATTRIBS_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'NO_DATA_FOUND',
                                              SQLERRM,
                                              'N',
                                              v_audit_base_severity);
            WHEN TOO_MANY_ROWS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_annual_attribs_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_rows_audited := v_stat_rows_audited + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_ANNUAL_ATTRIBS_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'TOO_MANY_ROWS',
                                              SQLERRM,
                                              'N',
                                              v_audit_base_severity);
            WHEN OTHERS
            THEN
               v_membership_record.sys_audit_ind := 'Y';
               v_membership_record.school_annual_attribs_key := 0;
               v_warehouse_key := 0;
               v_audit_base_severity := 0;
               v_stat_dberr_count := v_stat_dberr_count + 1;
               v_audit_natural_key := v_base_naturalkey_txt;

               k12intel_metadata.write_audit (p_param_build_id,
                                              p_param_package_id,
                                              p_param_task_id,
                                              v_sys_etl_source,
                                              'SCHOOL_ANNUAL_ATTRIBS_KEY',
                                              v_warehouse_key,
                                              v_audit_natural_key,
                                              'Untrapped Error',
                                              SQLERRM,
                                              'Y',
                                              v_audit_base_severity);
         END;

 		---------------------------------------------------------------
 		-- STAFF_KEY
 		---------------------------------------------------------------
 		BEGIN
            v_membership_record.STAFF_KEY := 0;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STAFF_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STAFF_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STAFF_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STAFF_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.STAFF_KEY := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'STAFF_KEY',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;

 		---------------------------------------------------------------
 		-- START_DATE
 		---------------------------------------------------------------
 		BEGIN
 			--v_membership_record.START_DATE := v_some_data.START_DATE; 

            SELECT sd.DATE_VALUE 
            into v_membership_record.START_DATE
            FROM k12intel_dw.DTBL_SCHOOL_DATES sd
            WHERE sd.SCHOOL_DATES_KEY = v_some_data.START_DATE; 
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.START_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'START_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.START_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'START_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.START_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'START_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;


 		---------------------------------------------------------------
 		-- END_DATE
 		---------------------------------------------------------------
 		BEGIN
 			--v_membership_record.END_DATE := v_some_data.END_DATE;  

            SELECT sd.DATE_VALUE 
            into v_membership_record.END_DATE
            FROM k12intel_dw.DTBL_SCHOOL_DATES sd
            WHERE sd.SCHOOL_DATES_KEY = v_some_data.END_DATE;
 		EXCEPTION
 			WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.END_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'END_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.END_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'END_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.END_DATE := TO_DATE('01/01/1900','MM/DD/YYYY');
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'END_DATE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;


 		---------------------------------------------------------------
 		-- GOAL_NAME
 		---------------------------------------------------------------
 		BEGIN
 			v_membership_record.GOAL_NAME := COALESCE(v_some_data.GOAL_NAME, '@ERR');  --plangoal.name (maybe null)
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.GOAL_NAME := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'GOAL_NAME',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.GOAL_NAME := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'GOAL_NAME',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.GOAL_NAME := '@ERR';
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'GOAL_NAME',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;


 		---------------------------------------------------------------
 		-- TARGET_VALUE
 		---------------------------------------------------------------
 		BEGIN
 			v_membership_record.TARGET_VALUE := v_some_data.TARGET_VALUE;  --plangoal.goalscore
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.TARGET_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'TARGET_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.TARGET_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'TARGET_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.TARGET_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'TARGET_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;


 		---------------------------------------------------------------
 		-- BASELINE_VALUE
 		---------------------------------------------------------------
 		BEGIN
 			v_membership_record.BASELINE_VALUE := v_some_data.BASELINE_VALUE;   --plangoal.basescore
 		EXCEPTION
 			/*WHEN NO_DATA_FOUND THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.BASELINE_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'BASELINE_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'NO_DATA_FOUND',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);
 			WHEN TOO_MANY_ROWS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.BASELINE_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_ROWS_AUDITED := v_STAT_ROWS_AUDITED + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.WRITE_AUDIT(
 					p_PARAM_BUILD_ID,
 					p_PARAM_PACKAGE_ID,
 					p_PARAM_TASK_ID,
 					v_SYS_ETL_SOURCE,
 					'BASELINE_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'TOO_MANY_ROWS',
 					sqlerrm,
 					'N',
 					v_AUDIT_BASE_SEVERITY
 				);*/
 			WHEN OTHERS THEN
 				v_membership_record.SYS_AUDIT_IND := 'Y';
 				v_membership_record.BASELINE_VALUE := 0;
 				v_WAREHOUSE_KEY := 0;
 				v_AUDIT_BASE_SEVERITY := 0;
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
 					v_SYS_ETL_SOURCE,
 					'BASELINE_VALUE',
 					v_WAREHOUSE_KEY,
 					v_AUDIT_NATURAL_KEY,
 					'Untrapped Error',
 					sqlerrm,
 					'Y',
 					v_AUDIT_BASE_SEVERITY
 				);
 		END;
dbms_output.put_line('mEM_GOAL_KEY=' || TO_CHAR(v_membership_record.MEMBERSHIP_GOAL_KEY));
 		BEGIN
 			SELECT MEMBERSHIP_GOAL_KEY 
            INTO v_existing_MEMBERSHIP_GOAL_
 			FROM K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS
 			WHERE MEMBERSHIP_GOAL_KEY = v_membership_record.MEMBERSHIP_GOAL_KEY;

 			BEGIN
 				UPDATE K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS
 				SET
 					MEMBERSHIP_KEY=			v_membership_record.MEMBERSHIP_KEY,
 					PROGRAM_KEY=			v_membership_record.PROGRAM_KEY,
 					INTERVENTIONS_KEY=			v_membership_record.INTERVENTIONS_KEY,
 					STUDENT_KEY=			v_membership_record.STUDENT_KEY,
 					STUDENT_ANNUAL_ATTRIBS_KEY=			v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY,
 					FACILITIES_KEY=			v_membership_record.FACILITIES_KEY,
 					SCHOOL_KEY=			v_membership_record.SCHOOL_KEY,
 					SCHOOL_ANNUAL_ATTRIBS_KEY=			v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY,
 					STAFF_KEY=			v_membership_record.STAFF_KEY,
 					START_DATE=			v_membership_record.START_DATE,
 					END_DATE=			v_membership_record.END_DATE,
 					GOAL_NAME=			v_membership_record.GOAL_NAME,
 					TARGET_VALUE=			v_membership_record.TARGET_VALUE,
 					BASELINE_VALUE=			v_membership_record.BASELINE_VALUE,
 					DISTRICT_CODE=			v_membership_record.DISTRICT_CODE,
 					SYS_ETL_SOURCE=			v_membership_record.SYS_ETL_SOURCE,
 					SYS_UPDATED=			v_membership_record.SYS_UPDATED,
 					SYS_AUDIT_IND=			v_membership_record.SYS_AUDIT_IND,
 					SYS_PARTITION_VALUE=			v_membership_record.SYS_PARTITION_VALUE
 				WHERE
 					MEMBERSHIP_GOAL_KEY = v_existing_MEMBERSHIP_GOAL_ AND
 					(
 						(
 							(v_membership_record.MEMBERSHIP_KEY <> MEMBERSHIP_KEY) OR
 							(v_membership_record.MEMBERSHIP_KEY IS NOT NULL AND MEMBERSHIP_KEY IS NULL) OR
 							(v_membership_record.MEMBERSHIP_KEY IS NULL AND MEMBERSHIP_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.PROGRAM_KEY <> PROGRAM_KEY) OR
 							(v_membership_record.PROGRAM_KEY IS NOT NULL AND PROGRAM_KEY IS NULL) OR
 							(v_membership_record.PROGRAM_KEY IS NULL AND PROGRAM_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.INTERVENTIONS_KEY <> INTERVENTIONS_KEY) OR
 							(v_membership_record.INTERVENTIONS_KEY IS NOT NULL AND INTERVENTIONS_KEY IS NULL) OR
 							(v_membership_record.INTERVENTIONS_KEY IS NULL AND INTERVENTIONS_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.STUDENT_KEY <> STUDENT_KEY) OR
 							(v_membership_record.STUDENT_KEY IS NOT NULL AND STUDENT_KEY IS NULL) OR
 							(v_membership_record.STUDENT_KEY IS NULL AND STUDENT_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY <> STUDENT_ANNUAL_ATTRIBS_KEY) OR
 							(v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL AND STUDENT_ANNUAL_ATTRIBS_KEY IS NULL) OR
 							(v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NULL AND STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.FACILITIES_KEY <> FACILITIES_KEY) OR
 							(v_membership_record.FACILITIES_KEY IS NOT NULL AND FACILITIES_KEY IS NULL) OR
 							(v_membership_record.FACILITIES_KEY IS NULL AND FACILITIES_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.SCHOOL_KEY <> SCHOOL_KEY) OR
 							(v_membership_record.SCHOOL_KEY IS NOT NULL AND SCHOOL_KEY IS NULL) OR
 							(v_membership_record.SCHOOL_KEY IS NULL AND SCHOOL_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY <> SCHOOL_ANNUAL_ATTRIBS_KEY) OR
 							(v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL AND SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL) OR
 							(v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL AND SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.STAFF_KEY <> STAFF_KEY) OR
 							(v_membership_record.STAFF_KEY IS NOT NULL AND STAFF_KEY IS NULL) OR
 							(v_membership_record.STAFF_KEY IS NULL AND STAFF_KEY IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.START_DATE <> START_DATE) OR
 							(v_membership_record.START_DATE IS NOT NULL AND START_DATE IS NULL) OR
 							(v_membership_record.START_DATE IS NULL AND START_DATE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.END_DATE <> END_DATE) OR
 							(v_membership_record.END_DATE IS NOT NULL AND END_DATE IS NULL) OR
 							(v_membership_record.END_DATE IS NULL AND END_DATE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.GOAL_NAME <> GOAL_NAME) OR
 							(v_membership_record.GOAL_NAME IS NOT NULL AND GOAL_NAME IS NULL) OR
 							(v_membership_record.GOAL_NAME IS NULL AND GOAL_NAME IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.TARGET_VALUE <> TARGET_VALUE) OR
 							(v_membership_record.TARGET_VALUE IS NOT NULL AND TARGET_VALUE IS NULL) OR
 							(v_membership_record.TARGET_VALUE IS NULL AND TARGET_VALUE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.BASELINE_VALUE <> BASELINE_VALUE) OR
 							(v_membership_record.BASELINE_VALUE IS NOT NULL AND BASELINE_VALUE IS NULL) OR
 							(v_membership_record.BASELINE_VALUE IS NULL AND BASELINE_VALUE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.DISTRICT_CODE <> DISTRICT_CODE) OR
 							(v_membership_record.DISTRICT_CODE IS NOT NULL AND DISTRICT_CODE IS NULL) OR
 							(v_membership_record.DISTRICT_CODE IS NULL AND DISTRICT_CODE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.SYS_ETL_SOURCE <> SYS_ETL_SOURCE) OR
 							(v_membership_record.SYS_ETL_SOURCE IS NOT NULL AND SYS_ETL_SOURCE IS NULL) OR
 							(v_membership_record.SYS_ETL_SOURCE IS NULL AND SYS_ETL_SOURCE IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.SYS_AUDIT_IND <> SYS_AUDIT_IND) OR
 							(v_membership_record.SYS_AUDIT_IND IS NOT NULL AND SYS_AUDIT_IND IS NULL) OR
 							(v_membership_record.SYS_AUDIT_IND IS NULL AND SYS_AUDIT_IND IS NOT NULL)
 						) OR
 						(
 							(v_membership_record.SYS_PARTITION_VALUE <> SYS_PARTITION_VALUE) OR
 							(v_membership_record.SYS_PARTITION_VALUE IS NOT NULL AND SYS_PARTITION_VALUE IS NULL) OR
 							(v_membership_record.SYS_PARTITION_VALUE IS NULL AND SYS_PARTITION_VALUE IS NOT NULL)
 						)
 					);
 				v_rowcnt := SQL%ROWCOUNT;
 				COMMIT;
                v_STAT_ROWS_UPDATED := v_STAT_ROWS_UPDATED + v_rowcnt;
 			EXCEPTION
 				WHEN OTHERS THEN
 					ROLLBACK;
 					v_WAREHOUSE_KEY := 0;
 					v_AUDIT_BASE_SEVERITY := 0;
 					v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 					v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 					K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
dbms_output.put_line('Insert');
 					-- Insert new record
 					INSERT INTO K12INTEL_DW.FTBL_PROGRAM_MEMBERSHIP_GOALS
 					VALUES (
 						v_membership_record.MEMBERSHIP_GOAL_KEY,			--MEMBERSHIP_GOAL_KEY
 						v_membership_record.MEMBERSHIP_KEY,			--MEMBERSHIP_KEY
 						v_membership_record.PROGRAM_KEY,			--PROGRAM_KEY
 						v_membership_record.INTERVENTIONS_KEY,			--INTERVENTIONS_KEY
 						v_membership_record.STUDENT_KEY,			--STUDENT_KEY
 						v_membership_record.STUDENT_ANNUAL_ATTRIBS_KEY,			--STUDENT_ANNUAL_ATTRIBS_KEY
 						v_membership_record.FACILITIES_KEY,			--FACILITIES_KEY
 						v_membership_record.SCHOOL_KEY,			--SCHOOL_KEY
 						v_membership_record.SCHOOL_ANNUAL_ATTRIBS_KEY,			--SCHOOL_ANNUAL_ATTRIBS_KEY
 						v_membership_record.STAFF_KEY,			--STAFF_KEY
 						v_membership_record.START_DATE,			--START_DATE
 						v_membership_record.END_DATE,			--END_DATE
 						v_membership_record.GOAL_NAME,			--GOAL_NAME
 						v_membership_record.TARGET_VALUE,			--TARGET_VALUE
 						v_membership_record.BASELINE_VALUE,			--BASELINE_VALUE
 						v_membership_record.DISTRICT_CODE,			--DISTRICT_CODE
 						v_membership_record.SYS_ETL_SOURCE,			--SYS_ETL_SOURCE
 						SYSDATE,			-- SYS_CREATED
 						SYSDATE,			-- SYS_UPDATED
 						v_membership_record.SYS_AUDIT_IND,			--SYS_AUDIT_IND
 						v_membership_record.SYS_PARTITION_VALUE			--SYS_PARTITION_VALUE
 					);
 					COMMIT;

                    v_STAT_ROWS_INSERTED := v_STAT_ROWS_INSERTED + 1;
dbms_output.put_line('After Insert');
 				EXCEPTION
 					WHEN OTHERS THEN
dbms_output.put_line('Error');
dbms_output.put_line(sqlerrm);
 						ROLLBACK;
 						v_WAREHOUSE_KEY := 0;
 						v_AUDIT_BASE_SEVERITY := 0;
 						v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 						v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 						K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
 				v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 				v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

 				K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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

        v_rowcnt := K12INTEL_AUTOMATION_PKG.INC_ROWS_PROCESSED(1);
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
 					v_AUDIT_NATURAL_KEY := '';

 					K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
 			v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 			v_AUDIT_NATURAL_KEY := '';

 			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
 			v_AUDIT_NATURAL_KEY := '';

 			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
 		v_STAT_DBERR_COUNT := v_STAT_DBERR_COUNT + 1;
 		v_AUDIT_NATURAL_KEY := '';

 		K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
 			v_AUDIT_NATURAL_KEY := '';

 			K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
