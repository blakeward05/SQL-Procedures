SET SERVEROUTPUT ON
SET ECHO OFF
SPOOL fix_perd_attend.log
;
SELECT COUNT(*) FROM K12INTEL_KEYMAP.KM_PERIOD_ABS_IC
;
TRUNCATE TABLE K12INTEL_KEYMAP.KM_PERIOD_ABS_IC
;
SELECT COUNT(*) FROM K12INTEL_KEYMAP.KM_PERIOD_ABS_IC  --SHOULD BE 0
;
SELECT COUNT(*) FROM K12INTEL_DW.FTBL_PERIOD_ABSENCES
;
TRUNCATE TABLE K12INTEL_DW.FTBL_PERIOD_ABSENCES 
;
SELECT COUNT(*) FROM K12INTEL_DW.FTBL_PERIOD_ABSENCES --SHOULD BE 0
;
COMMIT;

SPOOL OFF

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_F_PERIOD_ABS_IC"
 (
     p_PARAM_BUILD_ID            IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
     p_PARAM_PACKAGE_ID            IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
     p_PARAM_TASK_ID                IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
     p_PARAM_USE_FULL_REFRESH    IN NUMBER,
     p_PARAM_STAGE_SOURCE        IN VARCHAR2,
     p_PARAM_MISC_PARAMS            IN VARCHAR2,
     p_PARAM_EXECUTION_STATUS    OUT NUMBER
 ) IS
     PRAGMA AUTONOMOUS_TRANSACTION;
     v_initialize NUMBER(10) := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INITIALIZE(p_PARAM_PACKAGE_ID,p_PARAM_BUILD_ID,p_PARAM_TASK_ID,p_PARAM_USE_FULL_REFRESH);
 
     v_SYS_ETL_SOURCE VARCHAR2(50) := 'BLD_F_PERIOD_ABS_IC';
     v_WAREHOUSE_KEY NUMBER(10,0) := 0;
     v_AUDIT_BASE_SEVERITY NUMBER(10,0) := 0;
     v_STAT_ROWS_PROCESSED NUMBER(10,0) := 0;
     v_AUDIT_NATURAL_KEY VARCHAR2(512) := '';
     v_BASE_NATURALKEY_TXT VARCHAR(512) := '';
    v_LOCAL_CURRENT_SCHOOL_YEAR NUMBER := K12INTEL_METADATA.GET_SIS_SCHOOL_YEAR_IC(p_PARAM_STAGE_SOURCE);
    v_NETCHANGE_CUTOFF DATE := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.GET_NETCHANGE_CUTOFF();
    v_LOCAL_SCHOOL_YEAR VARCHAR2(10);
    v_LOCAL_DATA_DATE DATE := K12INTEL_METADATA.GET_LASTDATA_DATE();
    
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
         <TARGET SCHEMA="K12INTEL_METADATA" NAME="FTBL_PERIOD_ABSENCE"/>
     </TARGETS>
     <POLICIES>
         <POLICY NAME="REFRESH" APPLICABLE="Y" NOTES=""/>
         <POLICY NAME="NETCHANGE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="DELETE" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_BUILD_CONTROL" APPLICABLE="N" NOTES=""/>
         <POLICY NAME="XTBL_SCHOOL_CONTROL" APPLICABLE="N" NOTES=""/>
     </POLICIES>
     <CHANGE_LOG>
         <CHANGE DATE="08/16/2012" USER="Versifit" VERSION="10.6.0"  DESC="Procedure Created"/>
         <CHANGE DATE="07/10/2015" USER="Versifit" VERSION="10.6.0"  DESC="Changes to excused_absence and excused_autorized to properly handle exempt absences, so these 2 fields agree"/>
         <CHANGE DATE="07/10/2015" USER="Versifit" VERSION="10.6.0"  DESC="Changes to handle STATUS = P, these will be treated as exempt absences"/>
         <CHANGE DATE="09/24/2015" USER="Wardb" VERSION="10.6.0"  DESC="Modified cursor to include section and handle skinnied courses in same period w same attendanceid"/>
         <CHANGE DATE="10/02/2015" USER="Versifit" VERSION="10.6.0"  DESC="Add SECTIONID to keymap"/>
     </CHANGE_LOG>
 </ETL_COMMENTS>
 */
 /*
 HELPFUL COMMANDS:
     SELECT TOP 1000 * FROM K12INTEL_METADATA.FTBL_PERIOD_ABSENCE WHERE SYS_ETL_SOURCE = 'BLD_F_PERIOD_ABS_IC'
     DELETE K12INTEL_METADATA.FTBL_PERIOD_ABSENCE WHERE SYS_ETL_SOURCE = 'BLD_F_PERIOD_ABS_IC'
 */

 -- Auditing realted variables
 v_table_id NUMBER(10) := 0;
 
 -- Local variables and cursors
 v_start_time     DATE := sysdate;
 v_rowcnt     NUMBER;
 v_existing_PERIOD_ABSENCE_K     NUMBER(10);
 v_period_record    K12INTEL_DW.FTBL_PERIOD_ABSENCES%ROWTYPE;
 
 CURSOR c_some_cursor IS
     SELECT 
          att.STAGE_SOURCE
        , att.ATTENDANCEID
        , att.CALENDARID
        , att.PERSONID
        , att.PERIODID
        , att."DATE"
        , att.STATUS
        , att.EXCUSE
        , att.PRESENTMINUTES
        , att.COMMENTS
        , att.EXCUSEID
        , cal.ENDYEAR
        , sch."NUMBER" SCHOOL_CODE
        , dis."NUMBER" DISTRICT_CODE
        , per.STUDENTNUMBER
        , prd."NAME"
        , prd.PERIODMINUTES
        , prd.STARTTIME
        , g.SECTIONID
        , g.CNT
    FROM K12INTEL_STAGING_IC.ATTENDANCE att
    INNER JOIN K12INTEL_STAGING_IC.CALENDAR cal
        ON att.CALENDARID = cal.CALENDARID AND att.STAGE_SOURCE = cal.STAGE_SOURCE
    INNER JOIN K12INTEL_STAGING_IC.SCHOOL sch
        ON cal.SCHOOLID = sch.SCHOOLID AND cal.STAGE_SOURCE = sch.STAGE_SOURCE AND sch.STAGE_SIS_SCHOOL_YEAR = v_LOCAL_CURRENT_SCHOOL_YEAR
    INNER JOIN K12INTEL_STAGING_IC.PERSON per
        ON att.PERSONID = per.PERSONID AND att.STAGE_SOURCE = per.STAGE_SOURCE
    INNER JOIN K12INTEL_STAGING_IC.PERIOD prd
        ON att.PERIODID = prd.PERIODID AND att.STAGE_SOURCE = prd.STAGE_SOURCE    
    INNER JOIN K12INTEL_STAGING_IC.DISTRICT dis
        ON sch.DISTRICTID = dis.DISTRICTID AND sch.STAGE_SOURCE = dis.STAGE_SOURCE
    LEFT JOIN K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS g
        ON att.ATTENDANCEID = g.ATTENDANCEID AND att.STAGE_SOURCE = g.STAGE_SOURCE
 
/*Section below may be needed to add in section id to main cursor (BW 9/25/15)       
    INNER JOIN K12INTEL_STAGING_IC.SECTIONPLACEMENT secp
        ON SECP.PERIODID = prd.periodid and secp.stage_source = prd.stage_source
    INNER JOIN K12INTEL_STAGING_IC.TERM term 
        on secp.TERMID = term.TERMID and att."DATE" between term.STARTDATE and term.ENDDATE and term.STAGE_SOURCE = secp.STAGE_SOURCE
    INNER JOIN K12INTEL_STAGING_IC.TRIAL tr
        on tr.TRIALID = secp.TRIALID and tr.ACTIVE = 1 and tr.STAGE_SOURCE = secp.STAGE_SOURCE
    INNER JOIN K12INTEL_STAGING_IC.ROSTER ros
        on ros.SECTIONID = secp.SECTIONID and att.PERSONID = ros.PERSONID and ros.STAGE_SOURCE = att.STAGE_SOURCE and ros.STAGE_DELETEFLAG = 0
*/

    WHERE 1 = 1
        /*AND EXISTS(select null 
                FROM K12INTEL_STAGING_IC.ENROLLMENT enr
                WHERE att.personID = enr.personID AND att.calendarID = enr.calendarID AND enr.serviceType = 'P' AND att.STAGE_SOURCE =
                    enr.STAGE_SOURCE                
                )*/
        AND cal.EXCLUDE <> 1
        --AND prd.NONINSTRUCTIONAL = 0
        AND att.STAGE_DELETEFLAG = 0
--        AND per.STUDENTNUMBER = '8559415'
        AND att."DATE" >= TO_DATE('07/01/2014','mm/dd/yyyy')   
        AND att.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
        AND att.STAGE_MODIFYDATE >= v_NETCHANGE_CUTOFF
--        and att.ATTENDANCEID = 152926
        AND att."DATE" <= sysdate
;
 BEGIN
    BEGIN
        ---------------------------------------------------------------
        -- INITIALIZE TEMP TABLES  --Not sure this will be needed with expanded cursor
        ---------------------------------------------------------------
        --Delete temp table information
        execute immediate 'Delete K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS WHERE STAGE_SOURCE = ''' || p_PARAM_STAGE_SOURCE || '''';

        --Only create records when 1 record per period
        insert into K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS
        with TEMP_ROSTER as 
        (
            SELECT a.STAGE_SOURCE,a.SECTIONID,a.PERIODID,b.PERSONID,COALESCE(b.STARTDATE,c.STARTDATE) STARTDATE,COALESCE(b.ENDDATE,c.ENDDATE) ENDDATE,g.STARTDATE CAL_STARTDATE,g.ENDDATE CAL_ENDDATE
            FROM K12INTEL_STAGING_IC.SECTIONPLACEMENT a
            inner join K12INTEL_STAGING_IC.ROSTER b
            on a.SECTIONID = b.SECTIONID and a.STAGE_SOURCE = b.STAGE_SOURCE and b.STAGE_DELETEFLAG = 0
            inner join K12INTEL_STAGING_IC.TERM c
            on a.TERMID = c.TERMID and ((COALESCE(b.STARTDATE,c.STARTDATE) between c.STARTDATE and c.ENDDATE) and (COALESCE(b.ENDDATE,c.ENDDATE) BETWEEN c.STARTDATE and c.ENDDATE)) and a.STAGE_SOURCE = c.STAGE_SOURCE
            inner join K12INTEL_STAGING_IC.TRIAL d
            on a.TRIALID = d.TRIALID and d.ACTIVE = 1 and a.STAGE_SOURCE = d.STAGE_SOURCE
            inner join K12INTEL_STAGING_IC.SECTION e
            on a.SECTIONID = e.SECTIONID and a.STAGE_SOURCE = e.STAGE_SOURCE and e.STAGE_DELETEFLAG = 0
            inner join K12INTEL_STAGING_IC.COURSE f
            on e.COURSEID = f.COURSEID and e.STAGE_SOURCE = f.STAGE_SOURCE    
            inner join K12INTEL_STAGING_IC.CALENDAR g
            on f.CALENDARID = g.CALENDARID and f.STAGE_SOURCE = g.STAGE_SOURCE
            WHERE a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
                and g.EXCLUDE <> 1
                and f.ATTENDANCE = 1
        )
        select a.STAGE_SOURCE, a.ATTENDANCEID,b.SECTIONID, count(*) OVER(PARTITION BY a.STAGE_SOURCE, a.ATTENDANCEID) CNT
        FROM K12INTEL_STAGING_IC.ATTENDANCE a
        left join TEMP_ROSTER b
        on a.PERSONID = b.PERSONID and a.PERIODID = b.PERIODID and a."DATE" BETWEEN b.STARTDATE and b.ENDDATE and a.STAGE_SOURCE = b.STAGE_SOURCE
        where a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
            and a.STAGE_DELETEFLAG = 0
--            and rownum <= 1000
        --      and A.PERSONID = 239874
        --    and a.ATTENDANCEID = 109874
        group by a.STAGE_SOURCE, a.ATTENDANCEID, b.SECTIONID;
--        INSERT INTO K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS
--        select att.STAGE_SOURCE, att.ATTENDANCEID, b.sectionid, COUNT(*) OVER (PARTITION BY att.STAGE_SOURCE, att.ATTENDANCEID) AS CNT
--        FROM K12INTEL_STAGING_IC.ATTENDANCE att
--        inner join K12INTEL_STAGING_IC.SECTIONPLACEMENT a
--        on att.PERIODID = a.PERIODID and att.STAGE_SOURCE = a.STAGE_SOURCE
--        left join K12INTEL_STAGING_IC.ROSTER b
--        on a.SECTIONID = b.SECTIONID and att.PERSONID = b.PERSONID and a.STAGE_SOURCE = b.STAGE_SOURCE and b.STAGE_DELETEFLAG = 0
--        inner join K12INTEL_STAGING_IC.TERM c
--        on a.TERMID = c.TERMID and att."DATE" between c.STARTDATE and c.ENDDATE and a.STAGE_SOURCE = c.STAGE_SOURCE
--        inner join K12INTEL_STAGING_IC.TRIAL d
--        on a.TRIALID = d.TRIALID and d.ACTIVE = 1 and a.STAGE_SOURCE = d.STAGE_SOURCE
--        inner join K12INTEL_STAGING_IC.SECTION e
--        on a.SECTIONID = e.SECTIONID and a.STAGE_SOURCE = e.STAGE_SOURCE and e.STAGE_DELETEFLAG = 0
--        inner join K12INTEL_STAGING_IC.COURSE f
--        on e.COURSEID = f.COURSEID and e.STAGE_SOURCE = f.STAGE_SOURCE
--        inner join K12INTEL_STAGING_IC.CALENDAR g
--        on f.CALENDARID = g.CALENDARID and f.STAGE_SOURCE = g.STAGE_SOURCE
--        inner join K12INTEL_STAGING_IC.PERIOD h
--        on att.PERIODID = h.PERIODID /*and h.NONINSTRUCTIONAL = 0*/ and att.STAGE_SOURCE = h.STAGE_SOURCE
--        where att."DATE" >= TO_DATE('07/01/2012','mm/dd/yyyy')
--            and att."DATE" <= sysdate
--            and att.STAGE_MODIFYDATE >= v_NETCHANGE_CUTOFF  
--            and att.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
--            and g.EXCLUDE <> 1
--            and f.ATTENDANCE = 1
--            and att."DATE" between coalesce(b.STARTDATE,g.STARTDATE) and coalesce(b.ENDDATE,g.ENDDATE)
--            and att.STAGE_DELETEFLAG = 0
--            /*and EXISTS(select null 
--                        from K12INTEL_STAGING_IC.ENROLLMENT enr
--                        where att.personID = enr.personID and att.calendarID = enr.calendarID and enr.serviceType = 'P' and att.STAGE_SOURCE = enr.STAGE_SOURCE                

--                        )    */
--        group by att.STAGE_SOURCE,att.ATTENDANCEID, b.SECTIONID

--        ;
       -- having COUNT(*) = 1;  BW 9/16 removed having to include courses that are skinned with attid applies to two sections
     EXCEPTION
        WHEN OTHERS THEN
            v_WAREHOUSE_KEY := 0;
            v_AUDIT_BASE_SEVERITY := 0;
            v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

            K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                v_SYS_ETL_SOURCE,
                'TEMP_COURSE_SECTIONS',
                v_WAREHOUSE_KEY,
                v_AUDIT_NATURAL_KEY,
                'Untrapped Error',
                sqlerrm,
                'Y',
                v_AUDIT_BASE_SEVERITY
            );
     END;

     FOR v_source_data IN c_some_cursor LOOP
     BEGIN
        ---------------------------------------------------------------
        -- SYSTEM VARIABLES
        ---------------------------------------------------------------
        v_BASE_NATURALKEY_TXT := 'STAGE_SOURCE=' || v_source_data.STAGE_SOURCE || ';ATTENDANCEID=' || TO_CHAR(v_source_data.ATTENDANCEID) || ';DATE=' || TO_CHAR(v_source_data."DATE", 'mm/dd/yyyy') || ';SECTIONID=' || COALESCE(TO_CHAR(v_source_data.SECTIONID),'NULL');
        v_period_record.SYS_CREATED := SYSDATE;
        v_period_record.SYS_UPDATED := SYSDATE;
        v_period_record.SYS_AUDIT_IND := 'N';
        v_period_record.SYS_PARTITION_VALUE := 0;
        v_period_record.SYS_ETL_SOURCE := v_SYS_ETL_SOURCE;
        v_LOCAL_SCHOOL_YEAR := TO_CHAR(v_source_data.ENDYEAR-1) || '-' || TO_CHAR(v_source_data.ENDYEAR);

         ---------------------------------------------------------------
         -- Replace IC School Code with "Standard School Code"
         ---------------------------------------------------------------
        v_source_data.SCHOOL_CODE := K12INTEL_METADATA.REPLACE_IC_SCHOOL_NUMBER(v_source_data.SCHOOL_CODE);


         ---------------------------------------------------------------
         -- PERIOD_ABSENCE_KEY
         ---------------------------------------------------------------
         BEGIN
             K12INTEL_METADATA.GEN_PERIOD_ABS_KEY_IC(   
                v_period_record.PERIOD_ABSENCE_KEY,
                v_source_data.ATTENDANCEID,
                v_source_data.SECTIONID,
                v_source_data.STAGE_SOURCE                
            );

            IF v_period_record.PERIOD_ABSENCE_KEY = 0 THEN
                v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.PERIOD_ABSENCE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'PERIOD_ABSENCE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'ERROR GENERATING KEY',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );

                RAISE_APPLICATION_ERROR(-20000, 'FAILED TO GENERATE PERIOD_ABSENCE_KEY');
            END IF;
         END;
 
         ---------------------------------------------------------------
         -- CALENDAR_DATE_KEY
         ---------------------------------------------------------------
         BEGIN
             K12INTEL_METADATA.LOOKUP_CALENDAR_DATE_KEY(
                v_source_data."DATE",
                v_period_record.CALENDAR_DATE_KEY
            );

            IF v_period_record.CALENDAR_DATE_KEY = 0 THEN
                RAISE NO_DATA_FOUND;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CALENDAR_DATE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy');
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CALENDAR_DATE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CALENDAR_DATE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy');
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CALENDAR_DATE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CALENDAR_DATE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CALENDAR_DATE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 

         ---------------------------------------------------------------
         -- SCHOOL_KEY, FACILITIES_KEY
         ---------------------------------------------------------------
         BEGIN
             SELECT SCHOOL_KEY, FACILITIES_KEY 
            INTO v_period_record.SCHOOL_KEY, v_period_record.FACILITIES_KEY
            FROM K12INTEL_DW.DTBL_SCHOOLS
            WHERE SCHOOL_CODE = v_source_data.SCHOOL_CODE
                and DISTRICT_CODE = v_source_data.DISTRICT_CODE;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_KEY := 0;
                 v_period_record.FACILITIES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_CODE=' || v_source_data.SCHOOL_CODE || ';DISTRICT_CODE=' || v_source_data.DISTRICT_CODE;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_KEY := 0;
                 v_period_record.FACILITIES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_CODE=' || v_source_data.SCHOOL_CODE || ';DISTRICT_CODE=' || v_source_data.DISTRICT_CODE;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_KEY := 0;
                 v_period_record.FACILITIES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;

 
         ---------------------------------------------------------------
         -- SCHOOL_DATES_KEY
         ---------------------------------------------------------------
         BEGIN
             IF v_period_record.SCHOOL_KEY = 0 THEN
                v_period_record.SCHOOL_DATES_KEY := 0;
            ELSE
                SELECT SCHOOL_DATES_KEY INTO v_period_record.SCHOOL_DATES_KEY 
                FROM K12INTEL_DW.DTBL_SCHOOL_DATES 
                WHERE SCHOOL_KEY = v_period_record.SCHOOL_KEY
                    and DATE_VALUE = v_source_data."DATE";
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_DATES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_KEY=' || TO_CHAR(v_period_record.SCHOOL_KEY) || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy'); 
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_DATES_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_DATES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_KEY=' || TO_CHAR(v_period_record.SCHOOL_KEY) || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy'); 
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_DATES_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_DATES_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_DATES_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
 
         ---------------------------------------------------------------
         -- TIME_KEY
         ---------------------------------------------------------------
         BEGIN
             --v_period_record.TIME_KEY := 0;

            IF v_source_data.STARTTIME IS NULL THEN
                v_period_record.TIME_KEY := 0;
            ELSE
                SELECT TIME_KEY INTO v_period_record.TIME_KEY
                FROM K12INTEL_DW.DTBL_TIME
                WHERE TIME_VALUE_24HOURS =  REPLACE(SUBSTR(to_char(v_source_data.STARTTIME, 'hh24:mi:ss'), 0,5),':',''); 
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.TIME_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STARTTIME=' || REPLACE(SUBSTR(to_char(v_source_data.STARTTIME, 'hh24:mi:ss'), 0,5),':',''); 
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'TIME_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.TIME_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STARTTIME=' || REPLACE(SUBSTR(to_char(v_source_data.STARTTIME, 'hh24:mi:ss'), 0,5),':',''); 
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'TIME_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.TIME_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'TIME_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
 
         ---------------------------------------------------------------
         -- STUDENT_KEY
         ---------------------------------------------------------------
         BEGIN
             K12INTEL_METADATA.LOOKUP_STUDENT_KEY(
                v_period_record.STUDENT_KEY,
                v_source_data.STUDENTNUMBER,
                v_source_data.STAGE_SOURCE                
            );

            IF v_period_record.STUDENT_KEY = 0 THEN 
                RAISE NO_DATA_FOUND;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENTNUMBER=' || v_source_data.STUDENTNUMBER;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENTNUMBER=' || v_source_data.STUDENTNUMBER;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
         -- STUDENT_EVOLVE_KEY
         ---------------------------------------------------------------
         BEGIN
             IF v_period_record.STUDENT_KEY = 0 THEN
                v_period_record.STUDENT_EVOLVE_KEY := 0;
            ELSE
                SELECT STUDENT_EVOLVE_KEY INTO v_period_record.STUDENT_EVOLVE_KEY
                FROM K12INTEL_DW.DTBL_STUDENTS_EVOLVED
                WHERE STUDENT_KEY = v_period_record.STUDENT_KEY
                    and v_source_data."DATE" BETWEEN SYS_BEGIN_DATE AND SYS_END_DATE;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_EVOLVE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENT_KEY=' || TO_CHAR(v_period_record.STUDENT_KEY) || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy');
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'STUDENT_EVOLVE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_EVOLVE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENT_KEY=' || TO_CHAR(v_period_record.STUDENT_KEY) || ';DATE=' || TO_CHAR(v_source_data."DATE",'mm/dd/yyyy');
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'STUDENT_EVOLVE_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_EVOLVE_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'STUDENT_EVOLVE_KEY',
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
             IF v_period_record.STUDENT_KEY = 0 THEN
                v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
            ELSE
               K12INTEL_METADATA.LOOKUP_STU_ANNUAL_ATTRIBS_KEY(
                   v_period_record.STUDENT_KEY,
                   v_LOCAL_SCHOOL_YEAR,
                   v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY
               );

               IF v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY = 0 THEN
                 RAISE NO_DATA_FOUND;   
               END IF;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENT_KEY=' || TO_CHAR(v_period_record.STUDENT_KEY) || ';SCHOOL_YEAR=' || v_LOCAL_SCHOOL_YEAR;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';STUDENT_KEY=' || TO_CHAR(v_period_record.STUDENT_KEY) || ';SCHOOL_YEAR=' || v_LOCAL_SCHOOL_YEAR;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ANNUAL_ATTRIBS_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ANNUAL_ATTRIBS_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
         ---------------------------------------------------------------
         -- SCHOOL_ANNUAL_ATTRIBS_KEY
         ---------------------------------------------------------------
         BEGIN
             --v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;

            IF v_period_record.SCHOOL_KEY = 0 THEN
                v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
            ELSE
                SELECT SCHOOL_ANNUAL_ATTRIBS_KEY INTO v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY
                FROM K12INTEL_DW.DTBL_SCHOOL_ANNUAL_ATTRIBS
                WHERE SCHOOL_KEY = v_period_record.SCHOOL_KEY
                    and SCHOOL_YEAR = v_LOCAL_SCHOOL_YEAR;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_KEY=' || TO_CHAR(v_period_record.SCHOOL_KEY) || ';SCHOOL_YEAR=' || v_LOCAL_SCHOOL_YEAR;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_ANNUAL_ATTRIBS_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';SCHOOL_KEY=' || TO_CHAR(v_period_record.SCHOOL_KEY) || ';SCHOOL_YEAR=' || v_LOCAL_SCHOOL_YEAR;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_ANNUAL_ATTRIBS_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'SCHOOL_ANNUAL_ATTRIBS_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 

         
         --------------------------------------------------------------------------------------------------------------------------
         -- COURSE_OFFERINGS_KEY, COURSE_KEY, STAFF_EVOLVE_KEY, STAFF_ANNUAL_ATTRIBS_KEY, STAFF_KEY, STAFF_ASSIGNMENT_KEY, ROOM_KEY
         --------------------------------------------------------------------------------------------------------------------------
         BEGIN
            IF v_source_data.SECTIONID IS NULL THEN
                    v_period_record.COURSE_OFFERINGS_KEY := 0;
                    v_period_record.COURSE_KEY := 0;
                    v_period_record.STAFF_EVOLVE_KEY := 0;
                    v_period_record.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                    v_period_record.STAFF_KEY := 0;
                    v_period_record.STAFF_ASSIGNMENT_KEY := 0;
                    v_period_record.ROOM_KEY := 0;
            ELSE
             SELECT a.COURSE_OFFERINGS_KEY, 
                    a.COURSE_KEY,
                    a.STAFF_EVOLVE_KEY,
                    a.STAFF_ANNUAL_ATTRIBS_KEY,
                    a.STAFF_KEY,
                    a.STAFF_ASSIGNMENT_KEY,
                    a.ROOM_KEY
                    INTO
                    v_period_record.COURSE_OFFERINGS_KEY, 
                    v_period_record.COURSE_KEY,
                    v_period_record.STAFF_EVOLVE_KEY,
                    v_period_record.STAFF_ANNUAL_ATTRIBS_KEY,
                    v_period_record.STAFF_KEY,
                    v_period_record.STAFF_ASSIGNMENT_KEY,
                    v_period_record.ROOM_KEY
                FROM K12INTEL_DW.DTBL_COURSE_OFFERINGS a
                inner join K12INTEL_KEYMAP.KM_CRS_OFFER_IC b
                on a.COURSE_OFFERINGS_KEY = b.COURSE_OFFERINGS_KEY
                WHERE b.SECTIONID = v_source_data.SECTIONID
                    AND b.STAGE_SOURCE = v_source_data.STAGE_SOURCE;
                --WHERE exists(select null from K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS tcs where b.SECTIONID = tcs.SECTIONID and b.STAGE_SOURCE = tcs.STAGE_SOURCE and tcs.ATTENDANCEID = v_source_data.ATTENDANCEID and tcs.STAGE_SOURCE = v_source_data.STAGE_SOURCE);
           END IF;
        EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.COURSE_OFFERINGS_KEY := 0;
                v_period_record.COURSE_KEY := 0;
                v_period_record.STAFF_EVOLVE_KEY := 0;
                v_period_record.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                v_period_record.STAFF_KEY := 0;
                v_period_record.STAFF_ASSIGNMENT_KEY := 0;
                v_period_record.ROOM_KEY := 0;
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.COURSE_OFFERINGS_KEY := 0;
                v_period_record.COURSE_KEY := 0;
                v_period_record.STAFF_EVOLVE_KEY := 0;
                v_period_record.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                v_period_record.STAFF_KEY := 0;
                v_period_record.STAFF_ASSIGNMENT_KEY := 0;
                v_period_record.ROOM_KEY := 0;
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
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.COURSE_OFFERINGS_KEY := 0;
                v_period_record.COURSE_KEY := 0;
                v_period_record.STAFF_EVOLVE_KEY := 0;
                v_period_record.STAFF_ANNUAL_ATTRIBS_KEY := 0;
                v_period_record.STAFF_KEY := 0;
                v_period_record.STAFF_ASSIGNMENT_KEY := 0;
                v_period_record.ROOM_KEY := 0;
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

         --------------------------------------------------------------------------------------------
         -- ATTENDANCE_TYPE, EXCUSED_AUTHORIZED, EXCUSED_ABSENCE, ABSENCE_REASON_CODE, ABSENCE_REASON
         --------------------------------------------------------------------------------------------
         BEGIN
             IF v_source_data.EXCUSEID IS NULL THEN
                v_period_record.ATTENDANCE_TYPE := CASE v_source_data.STATUS WHEN 'T' THEN 'Late' WHEN 'P' THEN 'Present' ELSE 'Absent' END;
                v_period_record.EXCUSED_AUTHORIZED := CASE WHEN v_source_data.EXCUSE IN('X','P') THEN 'Yes' ELSE 'No' END;
                v_period_record.EXCUSED_ABSENCE := CASE v_source_data.EXCUSE WHEN 'X' THEN 'Authorized' WHEN 'P' THEN 'Authorized' WHEN 'E' THEN 'Excused' WHEN 'U' THEN 'Unexcused' ELSE 'Unknown' END;
                v_period_record.ABSENCE_REASON_CODE := 'NR';
                v_period_record.ABSENCE_REASON := 'No Reason';
            ELSE
                SELECT
                    CASE COALESCE(STATUS,v_source_data.STATUS) WHEN 'T' THEN 'Late' WHEN 'P' THEN 'Present' ELSE 'Absent' END,
                    CASE WHEN COALESCE(EXCUSE,v_source_data.EXCUSE) IN('X','P') THEN 'Yes' ELSE 'No' END,
                    CASE COALESCE(EXCUSE,v_source_data.EXCUSE) WHEN 'X' THEN 'Exempt' WHEN 'P' THEN 'Authorized' WHEN 'E' THEN 'Excused' WHEN 'U' THEN 'Unexcused' ELSE 'Unknown' END,
                    CODE,
                    SUBSTR(DESCRIPTION,1,20)
                    INTO
                    v_period_record.ATTENDANCE_TYPE,
                    v_period_record.EXCUSED_AUTHORIZED,
                    v_period_record.EXCUSED_ABSENCE,
                    v_period_record.ABSENCE_REASON_CODE,
                    v_period_record.ABSENCE_REASON
                from K12INTEL_STAGING_IC.ATTENDANCEEXCUSE                
                WHERE EXCUSEID = v_source_data.EXCUSEID
                    and STAGE_SOURCE = v_source_data.STAGE_SOURCE;
            END IF;
         EXCEPTION
             WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_TYPE := '@ERR';
                v_period_record.EXCUSED_AUTHORIZED := '@ERR';
                v_period_record.EXCUSED_ABSENCE := '@ERR';
                v_period_record.ABSENCE_REASON_CODE := '@ERR';
                v_period_record.ABSENCE_REASON := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';EXCUSEID=' || TO_CHAR(v_source_data.EXCUSEID);
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_TYPE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_TYPE := '@ERR';
                v_period_record.EXCUSED_AUTHORIZED := '@ERR';
                v_period_record.EXCUSED_ABSENCE := '@ERR';
                v_period_record.ABSENCE_REASON_CODE := '@ERR';
                v_period_record.ABSENCE_REASON := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT || ';EXCUSEID=' || TO_CHAR(v_source_data.EXCUSEID);
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_TYPE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_TYPE := '@ERR';
                v_period_record.EXCUSED_AUTHORIZED := '@ERR';
                v_period_record.EXCUSED_ABSENCE := '@ERR';
                v_period_record.ABSENCE_REASON_CODE := '@ERR';
                v_period_record.ABSENCE_REASON := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_TYPE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
        ---------------------------------------------------------------
         -- ATTENDANCE_PERIOD
         ---------------------------------------------------------------
         BEGIN
             v_period_record.ATTENDANCE_PERIOD := COALESCE(v_source_data."NAME",'@ERR');
         EXCEPTION
             /*WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_PERIOD := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_PERIOD',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_PERIOD := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_PERIOD',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );*/
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_PERIOD := '@ERR';
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_PERIOD',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
 
         ---------------------------------------------------------------
         -- INSTRUCTIONAL_MINUTES
         ---------------------------------------------------------------
         BEGIN
             v_period_record.INSTRUCTIONAL_MINUTES := NVL(v_source_data.PERIODMINUTES,0);
         EXCEPTION
             /*WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.INSTRUCTIONAL_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'INSTRUCTIONAL_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.INSTRUCTIONAL_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'INSTRUCTIONAL_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );*/
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.INSTRUCTIONAL_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'INSTRUCTIONAL_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
 
         ---------------------------------------------------------------
         -- ATTENDANCE_MINUTES
         ---------------------------------------------------------------
         BEGIN
             v_period_record.ATTENDANCE_MINUTES := CASE WHEN v_period_record.ATTENDANCE_TYPE IN ('Late','Present') THEN NVL(v_source_data.PERIODMINUTES,0)                                            
                                                        WHEN v_period_record.EXCUSED_ABSENCE IN ('Excused','Exempt') THEN NVL(v_source_data.PERIODMINUTES,0)
                                                        ELSE 0 
                                                    END;
         EXCEPTION
             /*WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );*/
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'ATTENDANCE_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     sqlerrm,
                     'Y',
                     v_AUDIT_BASE_SEVERITY
                 );
         END;
 
 
         ---------------------------------------------------------------
         -- CLASSROOM_ATTENDANCE_MINUTES
         ---------------------------------------------------------------
         BEGIN
             v_period_record.CLASSROOM_ATTENDANCE_MINUTES := CASE WHEN v_period_record.ATTENDANCE_TYPE = 'Absent' THEN 0 ELSE NVL(v_source_data.PERIODMINUTES,0) END;
         EXCEPTION
             /*WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CLASSROOM_ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CLASSROOM_ATTENDANCE_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );
             WHEN TOO_MANY_ROWS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CLASSROOM_ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CLASSROOM_ATTENDANCE_MINUTES',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     sqlerrm,
                     'N',
                     v_AUDIT_BASE_SEVERITY
                 );*/
             WHEN OTHERS THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.CLASSROOM_ATTENDANCE_MINUTES := 0;
                 v_WAREHOUSE_KEY := 0;
                 v_AUDIT_BASE_SEVERITY := 0;
                 v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
                 K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                     v_SYS_ETL_SOURCE,
                     'CLASSROOM_ATTENDANCE_MINUTES',
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
             v_period_record.DISTRICT_CODE := v_source_data.DISTRICT_CODE;
         EXCEPTION
             /*WHEN NO_DATA_FOUND THEN
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.DISTRICT_CODE := '@ERR';
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.DISTRICT_CODE := '@ERR';
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
                 v_period_record.SYS_AUDIT_IND := 'Y';
                 v_period_record.DISTRICT_CODE := '@ERR';
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
             SELECT PERIOD_ABSENCE_KEY INTO v_existing_PERIOD_ABSENCE_K
             FROM K12INTEL_DW.FTBL_PERIOD_ABSENCES
             WHERE PERIOD_ABSENCE_KEY = v_period_record.PERIOD_ABSENCE_KEY;
             BEGIN
                 UPDATE K12INTEL_DW.FTBL_PERIOD_ABSENCES
                 SET
                     CALENDAR_DATE_KEY=            v_period_record.CALENDAR_DATE_KEY,
                     SCHOOL_DATES_KEY=            v_period_record.SCHOOL_DATES_KEY,
                     TIME_KEY=            v_period_record.TIME_KEY,
                     STUDENT_KEY=            v_period_record.STUDENT_KEY,
                     STUDENT_EVOLVE_KEY=            v_period_record.STUDENT_EVOLVE_KEY,
                     STUDENT_ANNUAL_ATTRIBS_KEY=            v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY,
                     SCHOOL_KEY=            v_period_record.SCHOOL_KEY,
                     SCHOOL_ANNUAL_ATTRIBS_KEY=            v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY,
                     FACILITIES_KEY=            v_period_record.FACILITIES_KEY,
                     ROOM_KEY=            v_period_record.ROOM_KEY,
                     STAFF_ASSIGNMENT_KEY=            v_period_record.STAFF_ASSIGNMENT_KEY,
                     STAFF_KEY=            v_period_record.STAFF_KEY,
                     STAFF_ANNUAL_ATTRIBS_KEY=            v_period_record.STAFF_ANNUAL_ATTRIBS_KEY,
                     STAFF_EVOLVE_KEY=            v_period_record.STAFF_EVOLVE_KEY,
                     COURSE_KEY=            v_period_record.COURSE_KEY,
                     COURSE_OFFERINGS_KEY=            v_period_record.COURSE_OFFERINGS_KEY,
                     ATTENDANCE_TYPE=            v_period_record.ATTENDANCE_TYPE,
                     EXCUSED_AUTHORIZED=            v_period_record.EXCUSED_AUTHORIZED,
                     EXCUSED_ABSENCE=            v_period_record.EXCUSED_ABSENCE,
                     ABSENCE_REASON_CODE=            v_period_record.ABSENCE_REASON_CODE,
                     ABSENCE_REASON=            v_period_record.ABSENCE_REASON,
                     ATTENDANCE_PERIOD=            v_period_record.ATTENDANCE_PERIOD,
                     INSTRUCTIONAL_MINUTES=            v_period_record.INSTRUCTIONAL_MINUTES,
                     ATTENDANCE_MINUTES=            v_period_record.ATTENDANCE_MINUTES,
                     CLASSROOM_ATTENDANCE_MINUTES=            v_period_record.CLASSROOM_ATTENDANCE_MINUTES,
                     DISTRICT_CODE=            v_period_record.DISTRICT_CODE,
                     SYS_ETL_SOURCE=            v_period_record.SYS_ETL_SOURCE,
                     SYS_UPDATED=            v_period_record.SYS_UPDATED,
                     SYS_AUDIT_IND=            v_period_record.SYS_AUDIT_IND,
                     SYS_PARTITION_VALUE=            v_period_record.SYS_PARTITION_VALUE
                 WHERE
                     PERIOD_ABSENCE_KEY = v_existing_PERIOD_ABSENCE_K AND
                     (
                         (
                             (v_period_record.CALENDAR_DATE_KEY <> CALENDAR_DATE_KEY) OR
                             (v_period_record.CALENDAR_DATE_KEY IS NOT NULL AND CALENDAR_DATE_KEY IS NULL) OR
                             (v_period_record.CALENDAR_DATE_KEY IS NULL AND CALENDAR_DATE_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SCHOOL_DATES_KEY <> SCHOOL_DATES_KEY) OR
                             (v_period_record.SCHOOL_DATES_KEY IS NOT NULL AND SCHOOL_DATES_KEY IS NULL) OR
                             (v_period_record.SCHOOL_DATES_KEY IS NULL AND SCHOOL_DATES_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.TIME_KEY <> TIME_KEY) OR
                             (v_period_record.TIME_KEY IS NOT NULL AND TIME_KEY IS NULL) OR
                             (v_period_record.TIME_KEY IS NULL AND TIME_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STUDENT_KEY <> STUDENT_KEY) OR
                             (v_period_record.STUDENT_KEY IS NOT NULL AND STUDENT_KEY IS NULL) OR
                             (v_period_record.STUDENT_KEY IS NULL AND STUDENT_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STUDENT_EVOLVE_KEY <> STUDENT_EVOLVE_KEY) OR
                             (v_period_record.STUDENT_EVOLVE_KEY IS NOT NULL AND STUDENT_EVOLVE_KEY IS NULL) OR
                             (v_period_record.STUDENT_EVOLVE_KEY IS NULL AND STUDENT_EVOLVE_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY <> STUDENT_ANNUAL_ATTRIBS_KEY) OR
                             (v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL AND STUDENT_ANNUAL_ATTRIBS_KEY IS NULL) OR
                             (v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY IS NULL AND STUDENT_ANNUAL_ATTRIBS_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SCHOOL_KEY <> SCHOOL_KEY) OR
                             (v_period_record.SCHOOL_KEY IS NOT NULL AND SCHOOL_KEY IS NULL) OR
                             (v_period_record.SCHOOL_KEY IS NULL AND SCHOOL_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY <> SCHOOL_ANNUAL_ATTRIBS_KEY) OR
                             (v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL AND SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL) OR
                             (v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY IS NULL AND SCHOOL_ANNUAL_ATTRIBS_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.FACILITIES_KEY <> FACILITIES_KEY) OR
                             (v_period_record.FACILITIES_KEY IS NOT NULL AND FACILITIES_KEY IS NULL) OR
                             (v_period_record.FACILITIES_KEY IS NULL AND FACILITIES_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ROOM_KEY <> ROOM_KEY) OR
                             (v_period_record.ROOM_KEY IS NOT NULL AND ROOM_KEY IS NULL) OR
                             (v_period_record.ROOM_KEY IS NULL AND ROOM_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STAFF_ASSIGNMENT_KEY <> STAFF_ASSIGNMENT_KEY) OR
                             (v_period_record.STAFF_ASSIGNMENT_KEY IS NOT NULL AND STAFF_ASSIGNMENT_KEY IS NULL) OR
                             (v_period_record.STAFF_ASSIGNMENT_KEY IS NULL AND STAFF_ASSIGNMENT_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STAFF_KEY <> STAFF_KEY) OR
                             (v_period_record.STAFF_KEY IS NOT NULL AND STAFF_KEY IS NULL) OR
                             (v_period_record.STAFF_KEY IS NULL AND STAFF_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STAFF_ANNUAL_ATTRIBS_KEY <> STAFF_ANNUAL_ATTRIBS_KEY) OR
                             (v_period_record.STAFF_ANNUAL_ATTRIBS_KEY IS NOT NULL AND STAFF_ANNUAL_ATTRIBS_KEY IS NULL) OR
                             (v_period_record.STAFF_ANNUAL_ATTRIBS_KEY IS NULL AND STAFF_ANNUAL_ATTRIBS_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.STAFF_EVOLVE_KEY <> STAFF_EVOLVE_KEY) OR
                             (v_period_record.STAFF_EVOLVE_KEY IS NOT NULL AND STAFF_EVOLVE_KEY IS NULL) OR
                             (v_period_record.STAFF_EVOLVE_KEY IS NULL AND STAFF_EVOLVE_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.COURSE_KEY <> COURSE_KEY) OR
                             (v_period_record.COURSE_KEY IS NOT NULL AND COURSE_KEY IS NULL) OR
                             (v_period_record.COURSE_KEY IS NULL AND COURSE_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.COURSE_OFFERINGS_KEY <> COURSE_OFFERINGS_KEY) OR
                             (v_period_record.COURSE_OFFERINGS_KEY IS NOT NULL AND COURSE_OFFERINGS_KEY IS NULL) OR
                             (v_period_record.COURSE_OFFERINGS_KEY IS NULL AND COURSE_OFFERINGS_KEY IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ATTENDANCE_TYPE <> ATTENDANCE_TYPE) OR
                             (v_period_record.ATTENDANCE_TYPE IS NOT NULL AND ATTENDANCE_TYPE IS NULL) OR
                             (v_period_record.ATTENDANCE_TYPE IS NULL AND ATTENDANCE_TYPE IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.EXCUSED_AUTHORIZED <> EXCUSED_AUTHORIZED) OR
                             (v_period_record.EXCUSED_AUTHORIZED IS NOT NULL AND EXCUSED_AUTHORIZED IS NULL) OR
                             (v_period_record.EXCUSED_AUTHORIZED IS NULL AND EXCUSED_AUTHORIZED IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.EXCUSED_ABSENCE <> EXCUSED_ABSENCE) OR
                             (v_period_record.EXCUSED_ABSENCE IS NOT NULL AND EXCUSED_ABSENCE IS NULL) OR
                             (v_period_record.EXCUSED_ABSENCE IS NULL AND EXCUSED_ABSENCE IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ABSENCE_REASON_CODE <> ABSENCE_REASON_CODE) OR
                             (v_period_record.ABSENCE_REASON_CODE IS NOT NULL AND ABSENCE_REASON_CODE IS NULL) OR
                             (v_period_record.ABSENCE_REASON_CODE IS NULL AND ABSENCE_REASON_CODE IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ABSENCE_REASON <> ABSENCE_REASON) OR
                             (v_period_record.ABSENCE_REASON IS NOT NULL AND ABSENCE_REASON IS NULL) OR
                             (v_period_record.ABSENCE_REASON IS NULL AND ABSENCE_REASON IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ATTENDANCE_PERIOD <> ATTENDANCE_PERIOD) OR
                             (v_period_record.ATTENDANCE_PERIOD IS NOT NULL AND ATTENDANCE_PERIOD IS NULL) OR
                             (v_period_record.ATTENDANCE_PERIOD IS NULL AND ATTENDANCE_PERIOD IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.INSTRUCTIONAL_MINUTES <> INSTRUCTIONAL_MINUTES) OR
                             (v_period_record.INSTRUCTIONAL_MINUTES IS NOT NULL AND INSTRUCTIONAL_MINUTES IS NULL) OR
                             (v_period_record.INSTRUCTIONAL_MINUTES IS NULL AND INSTRUCTIONAL_MINUTES IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.ATTENDANCE_MINUTES <> ATTENDANCE_MINUTES) OR
                             (v_period_record.ATTENDANCE_MINUTES IS NOT NULL AND ATTENDANCE_MINUTES IS NULL) OR
                             (v_period_record.ATTENDANCE_MINUTES IS NULL AND ATTENDANCE_MINUTES IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.CLASSROOM_ATTENDANCE_MINUTES <> CLASSROOM_ATTENDANCE_MINUTES) OR
                             (v_period_record.CLASSROOM_ATTENDANCE_MINUTES IS NOT NULL AND CLASSROOM_ATTENDANCE_MINUTES IS NULL) OR
                             (v_period_record.CLASSROOM_ATTENDANCE_MINUTES IS NULL AND CLASSROOM_ATTENDANCE_MINUTES IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.DISTRICT_CODE <> DISTRICT_CODE) OR
                             (v_period_record.DISTRICT_CODE IS NOT NULL AND DISTRICT_CODE IS NULL) OR
                             (v_period_record.DISTRICT_CODE IS NULL AND DISTRICT_CODE IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SYS_ETL_SOURCE <> SYS_ETL_SOURCE) OR
                             (v_period_record.SYS_ETL_SOURCE IS NOT NULL AND SYS_ETL_SOURCE IS NULL) OR
                             (v_period_record.SYS_ETL_SOURCE IS NULL AND SYS_ETL_SOURCE IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SYS_AUDIT_IND <> SYS_AUDIT_IND) OR
                             (v_period_record.SYS_AUDIT_IND IS NOT NULL AND SYS_AUDIT_IND IS NULL) OR
                             (v_period_record.SYS_AUDIT_IND IS NULL AND SYS_AUDIT_IND IS NOT NULL)
                         ) OR
                         (
                             (v_period_record.SYS_PARTITION_VALUE <> SYS_PARTITION_VALUE) OR
                             (v_period_record.SYS_PARTITION_VALUE IS NOT NULL AND SYS_PARTITION_VALUE IS NULL) OR
                             (v_period_record.SYS_PARTITION_VALUE IS NULL AND SYS_PARTITION_VALUE IS NOT NULL)
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
                     INSERT INTO K12INTEL_DW.FTBL_PERIOD_ABSENCES
                     VALUES (
                         v_period_record.PERIOD_ABSENCE_KEY,            --PERIOD_ABSENCE_KEY
                         v_period_record.CALENDAR_DATE_KEY,            --CALENDAR_DATE_KEY
                         v_period_record.SCHOOL_DATES_KEY,            --SCHOOL_DATES_KEY
                         v_period_record.TIME_KEY,            --TIME_KEY
                         v_period_record.STUDENT_KEY,            --STUDENT_KEY
                         v_period_record.STUDENT_EVOLVE_KEY,            --STUDENT_EVOLVE_KEY
                         v_period_record.STUDENT_ANNUAL_ATTRIBS_KEY,            --STUDENT_ANNUAL_ATTRIBS_KEY
                         v_period_record.SCHOOL_KEY,            --SCHOOL_KEY
                         v_period_record.SCHOOL_ANNUAL_ATTRIBS_KEY,            --SCHOOL_ANNUAL_ATTRIBS_KEY
                         v_period_record.FACILITIES_KEY,            --FACILITIES_KEY
                         v_period_record.ROOM_KEY,            --ROOM_KEY
                         v_period_record.STAFF_ASSIGNMENT_KEY,            --STAFF_ASSIGNMENT_KEY
                         v_period_record.STAFF_KEY,            --STAFF_KEY
                         v_period_record.STAFF_ANNUAL_ATTRIBS_KEY,            --STAFF_ANNUAL_ATTRIBS_KEY
                         v_period_record.STAFF_EVOLVE_KEY,            --STAFF_EVOLVE_KEY
                         v_period_record.COURSE_KEY,            --COURSE_KEY
                         v_period_record.COURSE_OFFERINGS_KEY,            --COURSE_OFFERINGS_KEY
                         v_period_record.ATTENDANCE_TYPE,            --ATTENDANCE_TYPE
                         v_period_record.EXCUSED_AUTHORIZED,            --EXCUSED_AUTHORIZED
                         v_period_record.EXCUSED_ABSENCE,            --EXCUSED_ABSENCE
                         v_period_record.ABSENCE_REASON_CODE,            --ABSENCE_REASON_CODE
                         v_period_record.ABSENCE_REASON,            --ABSENCE_REASON
                         v_period_record.ATTENDANCE_PERIOD,            --ATTENDANCE_PERIOD
                         v_period_record.INSTRUCTIONAL_MINUTES,            --INSTRUCTIONAL_MINUTES
                         v_period_record.ATTENDANCE_MINUTES,            --ATTENDANCE_MINUTES
                         v_period_record.CLASSROOM_ATTENDANCE_MINUTES,            --CLASSROOM_ATTENDANCE_MINUTES
                         v_period_record.DISTRICT_CODE,            --DISTRICT_CODE
                         v_period_record.SYS_ETL_SOURCE,            --SYS_ETL_SOURCE
                         SYSDATE,            -- SYS_CREATED
                         SYSDATE,            -- SYS_UPDATED
                         v_period_record.SYS_AUDIT_IND,            --SYS_AUDIT_IND
                         v_period_record.SYS_PARTITION_VALUE            --SYS_PARTITION_VALUE
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
             v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
 
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

    BEGIN
        delete k12intel_dw.FTBL_PERIOD_ABSENCES a
        where exists(select null from k12intel_keymap.KM_PERIOD_ABS_IC b where a.PERIOD_ABSENCE_KEY = b.PERIOD_ABSENCE_KEY and b.STAGE_SOURCE = p_PARAM_STAGE_SOURCE)
            and not exists(select null
                           from k12intel_keymap.KM_PERIOD_ABS_IC b 
                           inner join K12INTEL_STAGING_IC.TEMP_COURSE_SECTIONS c
                           on b.ATTENDANCEID = c.ATTENDANCEID and (b.STAGE_SOURCE = c.STAGE_SOURCE or (b.SECTIONID is null and c.SECTIONID is null)) and b.STAGE_SOURCE = c.STAGE_SOURCE -- c.STAGE_DELETEFLAG = 0
                           where a.PERIOD_ABSENCE_KEY = b.PERIOD_ABSENCE_KEY and b.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
                        );

        v_rowcnt := SQL%ROWCOUNT;        
        v_rowcnt := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_DELETED(v_rowcnt);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
             v_WAREHOUSE_KEY := 0;
             v_AUDIT_BASE_SEVERITY := 0;
             v_AUDIT_NATURAL_KEY := '';
 
             K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                 v_SYS_ETL_SOURCE,
                 'DELETE',
                 v_WAREHOUSE_KEY,
                 v_AUDIT_NATURAL_KEY,
                 'Untrapped Error',
                 sqlerrm,
                 'Y',
                 v_AUDIT_BASE_SEVERITY
             );
    END;

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
                 'WRITE TASK STATS',
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

DROP PROCEDURE K12INTEL_METADATA.BLD_F_PERIOD_ATTENDANCE_IC;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA.BLD_F_PERIOD_ATTENDANCE_IC (
   p_PARAM_BUILD_ID           IN     K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
   p_PARAM_PACKAGE_ID         IN     K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
   p_PARAM_TASK_ID            IN     K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
   p_PARAM_USE_FULL_REFRESH   IN     NUMBER,
   p_PARAM_STAGE_SOURCE       IN     VARCHAR2,
   p_PARAM_MISC_PARAMS        IN     VARCHAR2,
   p_PARAM_EXECUTION_STATUS      OUT NUMBER)
IS
   PRAGMA AUTONOMOUS_TRANSACTION;
   v_initialize                  NUMBER (10)
                                    := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INITIALIZE (
                                          p_PARAM_PACKAGE_ID,
                                          p_PARAM_BUILD_ID,
                                          p_PARAM_TASK_ID,
                                          p_PARAM_USE_FULL_REFRESH);

   v_SYS_ETL_SOURCE              VARCHAR2 (50) := 'BLD_F_PERIOD_ATTENDANCE_IC';
   v_WAREHOUSE_KEY               NUMBER (10, 0) := 0;
   v_STAT_ROWS_PROCESSED         NUMBER (10, 0) := 0;
   v_AUDIT_BASE_SEVERITY         NUMBER (10, 0) := 0;
   v_AUDIT_NATURAL_KEY           VARCHAR2 (512) := '';
   v_BASE_NATURALKEY_TXT         VARCHAR (512) := '';

   -- Auditing realted variables
   v_table_id                    NUMBER (10) := 0;

   -- Local variables and cursors
   v_rowcnt                      NUMBER;
   v_existing_ATTENDANCE_KEY     NUMBER (10);
   v_f_attend_crt_record         K12INTEL_DW.FTBL_ATTENDANCE%ROWTYPE;

   ------------------------------------------------------------------
   -- setting vars here!
   ------------------------------------------------------------------
   v_start_time                  DATE := K12INTEL_METADATA.GET_LASTDATA_DATE ();
   v_LOCAL_CURRENT_SCHOOL_YEAR   NUMBER
      := K12INTEL_METADATA.GET_SIS_SCHOOL_YEAR_IC (p_PARAM_STAGE_SOURCE);
   v_NETCHANGE_CUTOFF            DATE
      := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.GET_NETCHANGE_CUTOFF ();
   v_LOCAL_DATA_DATE             DATE
                                    := K12INTEL_METADATA.GET_LASTDATA_DATE ();
   --   v_local_school_year           VARCHAR2 (10)
   --      :=    TO_CHAR (v_LOCAL_CURRENT_SCHOOL_YEAR)
   --         || '-'
   --         || TO_CHAR (v_LOCAL_CURRENT_SCHOOL_YEAR + 1);
   v_create_date                 DATE := NULL;
BEGIN
   --v_local_school_year := '2014-2015';
   FOR yearrec
      IN ( SELECT
                 scope_year
           FROM
                 K12INTEL_USERDATA.XTBL_BUILD_CONTROL
           WHERE
                 build_name = 'BLD_F_PERIOD_ATTENDANCE_IC'
          AND    process_ind = 'Y'
          AND    build_method =
                    CASE
                       WHEN p_PARAM_USE_FULL_REFRESH = 1 THEN 'REFRESH'
                       ELSE 'NETCHANGE'
                    END)
   LOOP
      BEGIN
         
         FOR inrec
            IN (
                SELECT distinct c.calendarid, c.name, cs.value, sch.school_key
                FROM
                    k12intel_staging_ic.day d
                    JOIN k12intel_staging_ic.periodschedule ps ON d.periodScheduleID = ps.periodScheduleID and ps.stage_source = d.stage_source
                    JOIN k12intel_staging_ic.period p on p.periodscheduleid = ps.periodscheduleid and ps.stage_source = p.stage_source
                    --ADDING IN ENDYEAR ON CALENDAR JOIN TO JUST GET CALENDARS FOR SCOPE YEAR BW 12/17/15
                    JOIN k12intel_staging_ic.calendar c ON c.calendarid = d.calendarid and c.stage_source = p.stage_source and to_char(c.endyear) = SUBSTR(yearrec.scope_year, 6,4)
                    JOIN k12intel_staging_ic.school s on s.schoolid = c.schoolid and s.stage_deleteflag = 0 and s.stage_source = c.stage_source
                    JOIN K12intel_STAGING_IC.CustomSchool CS  on cs.attributeid = 634 AND CS.STAGE_SOURCE= s.STAGE_SOURCE
                        AND CS.STAGE_DELETEFLAG = 0 and CS.SchoolId = S.SchoolID  AND CS.STAGE_SIS_SCHOOL_YEAR = S.STAGE_SIS_SCHOOL_YEAR
                    join k12intel_dw.dtbl_schools sch on sch.school_code = cs.value 
                WHERE 1=1
                    and p.name in ('01', '02', '03', '04', '05', '06', '07')
                --    and c.calendarid IN (3414)
                    )
         LOOP
            BEGIN
            
            DELETE FROM k12intel_dw.mpsf_period_attendance
            WHERE school_dates_key IN ( SELECT
                                            school_Dates_key
                                        FROM
                                              k12intel_dw.dtbl_school_dates
                                        WHERE
                                              local_school_year =
                                                 yearrec.scope_year
                                              and 
                                                dtbl_school_dates.school_key =
                                                inrec.school_key
                                                -- One student for testing
                                              --  and mpsf_period_attendance.student_key
                                              --  = 63660
                                                 );

            COMMIT;    
            
               v_create_date := SYSDATE;

               INSERT INTO
                      k12intel_dw.mpsf_period_attendance
                  ( SELECT
                          k12intel_dw.mpsf_period_attendance_seq.NEXTVAL
                             AS period_attendance_key,
                          v.student_key,
                          --    dst.student_id, --FOR TESTING PURPOSES
                          v.school_key,
                          v.school_dates_key,
                          v.calendar_dates_key,
                          v.course_key,
                          v.course_offerings_key,
                          se.student_evolve_key,
                          v.student_annual_attribs_key,
                          v.school_annual_attribs_key,
                          v.staff_key,
                          v.staff_assignment_key,
                          v.student_class_minutes,
                          v.course_period,
                          v.local_day_cycle,
                          v.course_section_name,
                          NVL (pa.attendance_type, 'Present')
                             AS attendance_type,
                          NVL (pa.excused_absence, '--') AS excused_absence,
                          NVL (pa.absence_reason_code, '--')
                             AS absence_reason_code,
                          NVL (pa.absence_reason, '--') AS absence_reason,
                          CASE
                             WHEN (NVL (pa.attendance_type, 'Present') IN ('Present',
                                                                           'Late')
                   OR              NVL (pa.excused_absence, '--') IN ('Authorized',
                                                                      'Exempt'))
                             THEN
                                1
                             WHEN NVL (pa.absence_reason_code, '--') IN ('TAUN',
                                                                         'TAEX')
                             THEN
                                1
                             ELSE
                                0
                          END
                             AS attendance_value,
                          v.date_value,
                          v_create_date AS create_date
                    FROM
                          ( SELECT
                                  sd.date_value,
                                  sd.school_dates_key,
                                  sd.calendar_dates_key,
                                  sd.local_day_cycle,
                                  sd.school_key,
                                  co.course_section_days,
                                  co.course_section_name,
                                  co.course_offerings_key
                                     AS course_offerings_key,
                                  ss.student_class_minutes,
                                  co.course_key,
                                  co.staff_key,
                                  co.staff_assignment_key,
                                  co.course_period,
                                  ss.student_key,
                                  -- se.student_evolve_key,
                                  ss.student_annual_attribs_key,
                                  ss.school_annual_attribs_key
                            FROM
                                  k12intel_dw.ftbl_student_schedules ss
                                  INNER JOIN
                                  k12intel_dw.dtbl_course_offerings co
                                     ON ss.course_offerings_key =
                                           co.course_offerings_key
                                  INNER JOIN k12intel_dw.dtbl_schools ds
                                     ON ds.school_key = ss.school_key
                                     AND co.school_key = ds.school_key
                                  INNER JOIN
                                  ( SELECT
                                          d."DATE" AS DATE_VALUE,
                                          d.calendarid,
                                          ps.name AS local_day_cycle,
                                          c.name,
                                          c.schoolid,
                                          s."NUMBER",
                                          sd.school_code,
                                          sd.local_enroll_Day,
                                          sd.local_school_year,
                                          sd.calendar_dates_key,
                                          sd.school_Dates_key,
                                          sd.school_key
                                    FROM
                                          k12intel_staging_ic.day d
                                          JOIN
                                          k12intel_staging_ic.periodschedule ps
                                             ON d.periodScheduleID =
                                                   ps.periodScheduleID
                                          JOIN k12intel_staging_ic.calendar c
                                             ON c.calendarid = d.calendarid
                                             --ADDED JOIN TO CALENDARID BW 12/17
                                             AND c.calendarid = inrec.calendarid  
                                          JOIN k12intel_staging_ic.school s
                                             ON s.schoolid = c.schoolid
                                             AND s.STAGE_SIS_SCHOOL_YEAR =
                                                   TO_NUMBER (
                                                      SUBSTR (
                                                         yearrec.scope_year,
                                                         1,
                                                         4))
                                             AND S.STAGE_DELETEFLAG = 0
                                          JOIN
                                          K12intel_STAGING_IC.CustomSchool cs
                                             ON cs.schoolid = s.schoolid
                                             AND cs.attributeid = 634
                                             AND cs.STAGE_SIS_SCHOOL_YEAR =
                                                   TO_NUMBER (
                                                      SUBSTR (
                                                         yearrec.scope_year,
                                                         1,
                                                         4))
                                             AND cs.stage_deleteflag = 0
                                          JOIN
                                          k12intel_dw.dtbl_school_dates sd
                                             ON sd.date_value = d."DATE"
                                             AND sd.school_code = cs."VALUE"
                                    WHERE 1=1
                                                            ) sd
                                     ON ds.school_key = sd.school_key
                            WHERE
                                  1 = 1
                           AND    sd.local_school_year = yearrec.scope_year
                           AND    sd.local_enroll_day = 1
                           AND    sd.date_value < v_create_date
                           AND    sd.date_value >= ss.schedule_start_date
                           AND    sd.date_value < ss.schedule_end_date
                           AND    (UPPER(sd.local_day_cycle) =
                                      UPPER(co.course_section_days)
                           OR      (co.course_section_days = 'A,B'
                           AND      UPPER(sd.local_day_cycle) IN ('A DAY',
                                                           'B DAY'))
                           OR      (co.course_section_days = 'A,C'
                           AND      UPPER(sd.local_day_cycle) IN ('A DAY',
                                                           'C DAY'))
                           OR      (co.course_section_days = 'B,D'
                           AND      UPPER(sd.local_day_cycle) IN ('B DAY',
                                                           'D DAY'))
                           OR      (co.course_section_days = 'A,B,C,D'
                           AND      UPPER(sd.local_day_cycle) IN ('A DAY',
                                                           'B DAY',
                                                           'C DAY',
                                                           'D DAY')))) v
                          LEFT JOIN k12intel_dw.ftbl_period_absences pa
                             ON pa.school_dates_key = v.school_dates_key
                             AND pa.student_key = v.student_key
                             AND pa.course_offerings_key =
                                   v.course_offerings_key
                          INNER JOIN k12intel_dw.dtbl_students dst
                             ON dst.student_key = v.student_key
                          INNER JOIN k12intel_dw.dtbl_students_evolved se
                             ON se.student_key = v.student_key
                             AND se.sys_begin_date <= v.date_value
                             AND se.sys_end_date > v.date_value
                    WHERE
                          1 = 1 --AND v.school_key = 139 
                        --  AND dst.student_id = '8559415'
                               );
            END;

            v_STAT_ROWS_PROCESSED := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED(SQL%ROWCOUNT);
            P_PARAM_EXECUTION_STATUS := 0;

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
         END LOOP;
      END;
   END LOOP;
END;
/
