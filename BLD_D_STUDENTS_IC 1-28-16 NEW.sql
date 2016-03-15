DROP PROCEDURE K12INTEL_METADATA.BLD_D_STUDENTS_IC;

CREATE OR REPLACE PROCEDURE K12INTEL_METADATA."BLD_D_STUDENTS_IC" (
   p_PARAM_BUILD_ID IN   K12INTEL_METADATA.WORKFLOW_TASK_STATS.BUILD_NUMBER%TYPE,
   p_PARAM_PACKAGE_ID IN K12INTEL_METADATA.WORKFLOW_TASK_STATS.PACKAGE_ID%TYPE,
   p_PARAM_TASK_ID IN    K12INTEL_METADATA.WORKFLOW_TASK_STATS.TASK_ID%TYPE,
   p_PARAM_USE_FULL_REFRESH IN NUMBER,
   p_PARAM_STAGE_SOURCE IN VARCHAR2,
   p_PARAM_MISC_PARAMS IN VARCHAR2,
   p_PARAM_EXECUTION_STATUS   OUT NUMBER)
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
      <CONNECTOR>IC</CONNECTOR>
      <QUALIFIER></QUALIFIER>
      <TARGETS>
          <TARGET SCHEMA="K12INTEL_METADATA" NAME="DTBL_STUDENTS"/>
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
          <CHANGE DATE="08/13/2014" USER="Max Janairo" VERSION="10.6.0"  DESC="Changed inner join to left join on enrollment to include students without enrollments in IC"/>
          <CHANGE DATE="01/20/2016" USER="Blake Ward" VERSION="10.6.0" DESC="Trimmed student state id to remove spaces"/>
          <CHANGE DATE="01/28/2016" USER="Blake Ward" VERSION="10.6.0" DESC="Extended ESL Indicator field to add Former option"/>
      </CHANGE_LOG>
  </ETL_COMMENTS>
  */
   /*
   HELPFUL COMMANDS:
       SELECT TOP 1000 * FROM K12INTEL_METADATA.DTBL_STUDENTS WHERE SYS_ETL_SOURCE = 'BLD_D_STUDENTS_IC'
       DELETE K12INTEL_METADATA.DTBL_STUDENTS WHERE SYS_ETL_SOURCE = 'BLD_D_STUDENTS_IC'
   */

   /*
   HELPFUL COMMANDS:
       TRUNCATE TABLE K12INTEL_DW.DTBL_STUDENTS
       TRUNCATE TABLE K12INTEL_METADATA.META_TASK_AUDIT_LOG


       EXEC BLD_D_STUDENTS_IC


       SELECT TOP 1000 * FROM K12INTEL_DW.DTBL_STUDENTS


       SELECT *
       FROM K12INTEL_METADATA.META_TASK_AUDIT_LOG
       WHERE AUDIT_SOURCE_LOCATION = ''
       ORDER BY DATETIME_STAMP DESC
   */

   PRAGMA AUTONOMOUS_TRANSACTION;
   v_initialize                  NUMBER (10)
                                    := K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.INITIALIZE (
                                          p_PARAM_PACKAGE_ID,
                                          p_PARAM_BUILD_ID,
                                          p_PARAM_TASK_ID,
                                          p_PARAM_USE_FULL_REFRESH);

   v_SYS_ETL_SOURCE              VARCHAR2 (50) := 'BLD_D_STUDENTS_IC';
   v_WAREHOUSE_KEY               NUMBER (10, 0) := 0;
   v_AUDIT_BASE_SEVERITY         NUMBER (10, 0) := 0;
   v_AUDIT_NATURAL_KEY           VARCHAR2 (512) := '';
   v_BASE_NATURALKEY_TXT         VARCHAR (512) := '';


   /*    Procedure    BLD_D_STUDENTS_IC
       Remarks        Build DTBL_STUDENTS
       Developer    Philip Devine

       06/14/2012 VersiFit Technologies LLC
   */

   -- Auditing realted variables
   v_table_id                    NUMBER (10) := 0;

   -- Local variables and cursors
   v_start_time                  DATE := K12INTEL_METADATA.GET_LASTDATA_DATE ();
   v_rowcnt                      NUMBER;
   v_existing_STUDENT_KEY        NUMBER (10);
   v_student_record              K12INTEL_DW.DTBL_STUDENTS%ROWTYPE;
   v_details_record              K12INTEL_DW.DTBL_STUDENT_DETAILS%ROWTYPE;
   v_local_data_date             DATE := K12INTEL_METADATA.GET_LASTDATA_DATE ();

   v_last_activity_date          DATE;
   v_ETHNIC_DECODE               VARCHAR (4000);
   v_DIPLOMADATE                 DATE;
   v_DIPLOMATYPE                 VARCHAR2 (3);
   v_MIN_START_DATE              DATE;
   v_address_record              K12INTEL_STAGING_IC.TEMP_ADDRESS%ROWTYPE;

   v_LOCAL_CURRENT_SCHOOL_YEAR   NUMBER (10)
      := K12INTEL_METADATA.GET_SIS_SCHOOL_YEAR_IC (p_PARAM_STAGE_SOURCE);
   v_LOCAL_STARTDATE             DATE;
   v_LOCAL_ENDDATE               DATE;
   v_LOCAL_GRADE                 VARCHAR2 (4);
   v_LOCAL_DISTRICTID            NUMBER (10);
   v_LOCAL_SCHOOLID              NUMBER (10);
   v_LOCAL_CALENDARID            NUMBER (10);

   v_DEFAULT_DISTRICT_CODE       VARCHAR2 (20);

   CURSOR c_some_cursor
   IS
      SELECT per.STAGE_SOURCE,
             per.PERSONID,
             per.STATEID,
             per.STUDENTNUMBER,
             id.IDENTITYID,
             id.LASTNAME,
             id.FIRSTNAME,
             id.MIDDLENAME,
             id.SUFFIX,
             id."ALIAS",
             id.GENDER,
             id.BIRTHDATE,
             id.RACEETHNICITY,
             id.BIRTHCOUNTRY,
             id.HISPANICETHNICITY,
             id.RACEETHNICITYFED,
             id.HOMEPRIMARYLANGUAGE,
             last_enr.ENROLLMENTID,
             last_enr.CALENDARID,
             last_enr.GRADE,
             last_enr.SERVICETYPE,
             last_enr.NOSHOW,
             last_enr.STARTDATE,
             last_enr.STARTSTATUS,
             last_enr.ENDDATE,
             last_enr.ENDSTATUS,
             last_enr.NEXTCALENDAR,
             last_enr.NEXTGRADE,
             last_enr.RESIDENTDISTRICT,
             last_enr.MEALSTATUS,
             last_enr.ENGLISHPROFICIENCY,
             last_enr."LANGUAGE",
             last_enr.CITIZENSHIP,
             last_enr.MIGRANT,
             last_enr.HOMELESS,
             last_enr.giftedTalented,
             last_enr.SECTION504,
             last_enr.SPECIALEDSETTING,
             last_enr.WITHDRAWDATE,
             last_enr.PERCENTENROLLED,
             last_enr.SCHOOLID,
             last_enr.DISTRICTID,
             last_enr.ENDYEAR,
             last_enr.CALENDAR_NAME,
             last_enr.CALENDAR_EXCLUDE,
             last_enr.SCHOOL_SUB_TYPE,
             last_enr.SPECIALEDSTATUS,
             last_enr.DISABILITY1,
             vr.Am_Ind_Ak,
             vr.Asian,
             vr.black,
             vr.hispanic,
             vr.pac_is,
             vr.White
        FROM K12INTEL_STAGING_IC.PERSON per
             INNER JOIN K12INTEL_STAGING_IC."IDENTITY" id
                ON     per.CURRENTIDENTITYID = id.IDENTITYID
                   AND per.STAGE_SOURCE = id.STAGE_SOURCE
                   AND id.STAGE_SIS_SCHOOL_YEAR = v_LOCAL_CURRENT_SCHOOL_YEAR
             LEFT JOIN
             (SELECT SUM (
                        CASE
                           WHEN re.name = 'American Indian or Alaska Native'
                           THEN
                              1
                           ELSE
                              0
                        END)
                        AS Am_Ind_Ak,
                     SUM (CASE
                             WHEN re.name = 'Asian' THEN 1
                             ELSE 0
                          END)
                        AS Asian,
                     SUM (
                        CASE
                           WHEN re.name = 'Black or African American' THEN 1
                           ELSE 0
                        END)
                        AS black,
                     SUM (CASE
                             WHEN re.name = 'Hispanic/Latino' THEN 1
                             ELSE 0
                          END)
                        AS hispanic,
                     SUM (
                        CASE
                           WHEN re.name =
                                   'Native Hawaiian or Other Pacific Islander'
                           THEN
                              1
                           ELSE
                              0
                        END)
                        AS pac_is,
                     SUM (CASE
                             WHEN re.name = 'White' THEN 1
                             ELSE 0
                          END)
                        AS White,
                     p.studentnumber AS studentnumber
                FROM k12intel_staging_ic.person p
                     JOIN k12intel_staging_ic.identity i
                        ON i.identityid = p.currentidentityid
                     JOIN k12intel_staging_ic.identityraceethnicity ire
                        ON ire.identityid = i.identityid
                     JOIN k12intel_staging_ic.raceethnicity re
                        ON re.raceid = ire.raceid
              GROUP BY p.studentnumber) vr
                ON vr.studentnumber = per.studentnumber
             LEFT JOIN k12intel_staging_ic.student st ON st.personid = per.personid
             LEFT JOIN
             ( /* Select the last enrollment for the student (if one exists) */
              SELECT enr.STAGE_SOURCE,
                     enr.personID,
                     enr.ENROLLMENTID,
                     enr.CALENDARID,
                     enr.GRADE,
                     enr.SERVICETYPE,
                     enr.ACTIVE,
                     enr.NOSHOW,
                     enr.STARTDATE,
                     enr.STARTSTATUS,
                     enr.ENDDATE,
                     enr.ENDSTATUS,
                     enr.NEXTCALENDAR,
                     enr.NEXTGRADE,
                     enr.DIPLOMADATE,
                     enr.DIPLOMATYPE,
                     enr.GRADYEAR,
                     enr.SERVINGDISTRICT,
                     enr.RESIDENTDISTRICT,
                     enr.RESIDENTSCHOOL,
                     enr.MEALSTATUS,
                     enr.ENGLISHPROFICIENCY,
                     enr.ENGLISHPROFICIENCYDATE,
                     enr.LEP,
                     enr.ESL,
                     enr."LANGUAGE",
                     enr.CITIZENSHIP,
                     enr.TITLE1,
                     enr.TRANSPORTATION,
                     enr.MIGRANT,
                     enr.HOMELESS,
                     enr.GIFTEDTALENTED,
                     enr.SECTION504,
                     enr.SPECIALEDSTATUS,
                     enr.SPECIALEDSETTING,
                     enr.DISABILITY1,
                     enr.DISABILITY2,
                     enr.WITHDRAWDATE,
                     enr.COHORTYEAR,
                     enr.PERCENTENROLLED,
                     cal.SCHOOLID,
                     cal.DISTRICTID,
                     enr.ENDYEAR,
                     cal.NAME CALENDAR_NAME,
                     cal.EXCLUDE CALENDAR_EXCLUDE,
                     school_type.SCHOOL_SUB_TYPE,
                     ROW_NUMBER ()
                        OVER (PARTITION BY enr.STAGE_SOURCE,
                                           enr.personID
                              ORDER BY
                                 cal.endyear DESC,
                                 --CASE WHEN enr.SERVICETYPE = 'P' THEN 0 ELSE 1 END,
                                 enr.startdate DESC,
                                 ENR.ENDDATE DESC)
                        r
                FROM K12INTEL_STAGING_IC.Enrollment enr
                     INNER JOIN K12INTEL_STAGING_IC.calendar cal
                        ON     enr.calendarID = cal.calendarID
                           AND enr.STAGE_SOURCE = cal.STAGE_SOURCE
                     INNER JOIN K12INTEL_STAGING_IC.School sch
                        ON     cal.schoolID = sch.schoolID
                           AND cal.STAGE_SOURCE = sch.STAGE_SOURCE
                           AND sch.STAGE_SIS_SCHOOL_YEAR =
                                  v_LOCAL_CURRENT_SCHOOL_YEAR
                     LEFT JOIN
                     (SELECT csch.STAGE_SOURCE,
                             csch.SCHOOLID,
                             csch.VALUE SCHOOL_SUB_TYPE
                        FROM K12INTEL_STAGING_IC.CUSTOMSCHOOL csch
                             INNER JOIN
                             K12INTEL_STAGING_IC.CAMPUSATTRIBUTE cattr
                                ON     csch.ATTRIBUTEID = cattr.ATTRIBUTEID
                                   AND cattr.OBJECT = 'School'
                                   AND cattr.ELEMENT = 'subtype'
                                   AND csch.STAGE_SOURCE = cattr.STAGE_SOURCE
                       WHERE     csch.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
                             AND csch.STAGE_SIS_SCHOOL_YEAR =
                                    v_LOCAL_CURRENT_SCHOOL_YEAR
                             AND csch.STAGE_DELETEFLAG = 0) school_type
                        ON     sch.SCHOOLID = school_type.SCHOOLID
                           AND sch.STAGE_SOURCE = school_type.STAGE_SOURCE
               WHERE     1 = 1
                     AND enr.STAGE_DELETEFLAG = 0
                     --ignore future enrollments
                     --AND enr.stateExclude = 0
                     AND enr.startDate <= v_local_data_date
                     AND enr.STAGE_SOURCE = p_PARAM_STAGE_SOURCE) last_enr
                ON     per.PERSONID = last_enr.PERSONID
                   AND per.STAGE_SOURCE = last_enr.STAGE_SOURCE
                   AND last_enr.r = 1
       WHERE 1 = 1 AND per.STUDENTNUMBER IS NOT NULL                        --
                                                     -- AND a.studentnumber = '9027071'
                                                     --AND a.studentnumber in ('1000057','1000138')
                                                     --AND a.personid= 142
--                                                     and a.studentnumber in (263029,296936,296894,299593,297311,309315,308353,312742,286637,286641)
--                                                     AND rownum < 10000
             AND per.STAGE_SOURCE = p_PARAM_STAGE_SOURCE;
BEGIN
   DBMS_OUTPUT.ENABLE (999999);

   BEGIN
      EXECUTE IMMEDIATE
         (   'delete K12INTEL_STAGING_IC.TEMP_HOMEROOMS where stage_source='''
          || p_PARAM_STAGE_SOURCE
          || '''');

      INSERT INTO K12INTEL_STAGING_IC.TEMP_HOMEROOMS
         SELECT STAGE_SOURCE,
                CALENDARID,
                PERSONID,
                TEACHERDISPLAY,
                ROOM_NAME
           FROM (SELECT a.STAGE_SOURCE,
                        a.CALENDARID,
                        b.PERSONID,
                        c.TEACHERDISPLAY,
                        d.NAME ROOM_NAME,
                        RANK ()
                           OVER (PARTITION BY a.STAGE_SOURCE,
                                              a.CALENDARID,
                                              b.PERSONID
                                 ORDER BY
                                    COALESCE (b.ENDDATE,
                                              TO_DATE ('12/31/9999',
                                                       'mm/dd/yyyy')) DESC,
                                    b.STARTDATE DESC,
                                    b.SECTIONID DESC)
                           r
                   FROM K12INTEL_STAGING_IC.TRIAL a
                        INNER JOIN K12INTEL_STAGING_IC.ROSTER b
                           ON     a.TRIALID = b.TRIALID
                              AND a.STAGE_SOURCE = b.STAGE_SOURCE
                              AND b.STAGE_DELETEFLAG = 0
                        INNER JOIN K12INTEL_STAGING_IC.SECTION c
                           ON     b.SECTIONID = c.SECTIONID
                              AND b.STAGE_SOURCE = c.STAGE_SOURCE
                              AND c.STAGE_DELETEFLAG = 0
                        LEFT JOIN K12INTEL_STAGING_IC.ROOM d
                           ON     c.ROOMID = d.ROOMID
                              AND c.STAGE_SOURCE = d.STAGE_SOURCE
                        INNER JOIN k12intel_Staging_IC.COURSE e
                           ON     c.COURSEID = e.COURSEID
                              AND c.STAGE_SOURCE = e.STAGE_SOURCE
                        INNER JOIN k12intel_staging_ic.CALENDAR f
                           ON     e.CALENDARID = f.CALENDARID
                              AND e.STAGE_SOURCE = f.STAGE_SOURCE
                  WHERE     COALESCE (b.STARTDATE,
                                      f.STARTDATE) <= v_local_data_date
                        AND a.ACTIVE = 1
                        AND a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
                        AND (c.HOMEROOMSECTION = 1 OR e.HOMEROOM = 1)) x
          WHERE x.r = 1;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;
         v_student_record.SYS_AUDIT_IND := 'Y';
         v_AUDIT_NATURAL_KEY := NULL;

         K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                              'POPULATE HOMEROOMS TABLE',
                                              v_WAREHOUSE_KEY,
                                              v_AUDIT_NATURAL_KEY,
                                              'insert into homerooms table',
                                              SQLERRM,
                                              'Y',
                                              v_AUDIT_BASE_SEVERITY);
   END;

   BEGIN
      EXECUTE IMMEDIATE
         (   'delete K12INTEL_STAGING_IC.TEMP_ADDRESS where stage_source='''
          || p_PARAM_STAGE_SOURCE
          || '''');

      /*
            INSERT INTO K12INTEL_STAGING_IC.TEMP_ADDRESS
               SELECT a.STAGE_SOURCE,
                      a.HOUSEHOLDID,
                      a.PERSONID,
                      a.PHONE,
                      a.PHONEPRIVATE,
                      physical.ADDRESSID PHYSICAL_ADDRESSID,
                      physical."NUMBER" PHYSICAL_NUMBER,
                      physical.STREET PHYSICAL_STREET,
                      physical.TAG PHYSICAL_TAG,
                      physical."PREFIX" PHYSICAL_PREFIX,
                      physical.DIR PHYSICAL_DIR,
                      physical.APT PHYSICAL_APT,
                      physical.CITY PHYSICAL_CITY,
                      physical."STATE" PHYSICAL_STATE,
                      physical.ZIP PHYSICAL_ZIP,
                      physical.COUNTY,
                      physical.POSTOFFICEBOX PHYSICAL_POSTOFFICEBOX,
                      NULL PHYSICAL_XCOORD,
                      NULL PHYSICAL_YCOORD,
                      mailing.PRIVATE,
                      mailing.ADDRESSID MAILING_ADDRESSID,
                      mailing."NUMBER" MAILING_NUMBER,
                      mailing.STREET MAILING_STREET,
                      mailing.TAG MAILING_TAG,
                      mailing."PREFIX" MAILING_PREFIX,
                      mailing.DIR MAILING_DIR,
                      mailing.APT MAILING_APT,
                      mailing.CITY MAILING_CITY,
                      mailing."STATE" MAILING_STATE,
                      mailing.ZIP MAILING_ZIP,
                      mailing.POSTOFFICEBOX MAILING_POSTOFFICEBOX,
                      physical.CATCHMENT_CODE
                 FROM (SELECT a.STAGE_SOURCE,
                              a.HOUSEHOLDID,
                              a.PERSONID,
                              c.PHONE,
                              c.PHONEPRIVATE,
                              ROW_NUMBER ()
                              OVER (
                                 PARTITION BY a.STAGE_SOURCE, a.PERSONID
                                 ORDER BY
                                    COALESCE (a.ENDDATE,
                                              TO_DATE ('12/31/9999', 'mm/dd/yyyy')) DESC,
                                    a.STARTDATE DESC,
                                    a.MEMBERID DESC)
                                 r
                         FROM K12INTEL_STAGING_IC.HOUSEHOLDMEMBER a
                              INNER JOIN K12INTEL_STAGING_IC.PERSON b
                                 ON     a.PERSONID = b.PERSONID
                                    AND a.STAGE_SOURCE = b.STAGE_SOURCE
                              INNER JOIN K12INTEL_STAGING_IC.HOUSEHOLD c
                                 ON     a.HOUSEHOLDID = c.HOUSEHOLDID
                                    AND a.STAGE_SOURCE = c.STAGE_SOURCE
                        WHERE     b.STUDENTNUMBER IS NOT NULL
                              AND a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE) a
                      LEFT JOIN
                      (                                                    --MAILING
                       SELECT a.STAGE_SOURCE,
                              a.HOUSEHOLDID,
                              a.PRIVATE,
                              a.SECONDARY,
                              a.MAILING,
                              a.ADDRESSID,
                              b."NUMBER",
                              b.STREET,
                              b.TAG,
                              b."PREFIX",
                              b.DIR,
                              b.APT,
                              b.CITY,
                              b."STATE",
                              b.ZIP,
                              b.POSTOFFICEBOX,
                              ROW_NUMBER ()
                              OVER (
                                 PARTITION BY a.STAGE_SOURCE, a.HOUSEHOLDID
                                 ORDER BY
                                    CASE WHEN a.ENDDATE IS NULL THEN 0 ELSE 1 END,
                                    CASE WHEN a.MAILING = 1 THEN 0 ELSE 1 END,
                                    COALESCE (a.ENDDATE,
                                              TO_DATE ('12/31/9999', 'mm/dd/yyyy')) DESC,
                                    CASE WHEN a.SECONDARY = 1 THEN 1 ELSE 0 END,
                                    a.STARTDATE DESC,
                                    a.LOCATIONID DESC)
                                 r
                         FROM K12INTEL_STAGING_IC.HOUSEHOLDLOCATION a
                              INNER JOIN K12INTEL_STAGING_IC.ADDRESS b
                                 ON     a.STAGE_SOURCE = b.STAGE_SOURCE
                                    AND a.ADDRESSID = b.ADDRESSID) mailing
                         ON     a.HOUSEHOLDID = mailing.HOUSEHOLDID
                            AND a.STAGE_SOURCE = mailing.STAGE_SOURCE
                            AND mailing.r = 1
                      LEFT JOIN
                      (                                                   --PHYSICAL
                       SELECT a.STAGE_SOURCE,
                              a.HOUSEHOLDID,
                              a.PRIVATE,
                              a.SECONDARY,
                              a.MAILING,
                              a.ADDRESSID,
                              b."NUMBER",
                              b.STREET,
                              b.TAG,
                              b."PREFIX",
                              b.DIR,
                              b.APT,
                              b.CITY,
                              b."STATE",
                              b.ZIP,
                              b.COUNTY,
                              b.POSTOFFICEBOX,
                              c.CATCHMENT_CODE,
                              ROW_NUMBER ()
                              OVER (
                                 PARTITION BY a.STAGE_SOURCE, a.HOUSEHOLDID
                                 ORDER BY
                                    CASE WHEN a.ENDDATE IS NULL THEN 0 ELSE 1 END,
                                    CASE WHEN a.MAILING = 1 THEN 1 ELSE 0 END,
                                    COALESCE (a.ENDDATE,
                                              TO_DATE ('12/31/9999', 'mm/dd/yyyy')) DESC,
                                    CASE WHEN a.SECONDARY = 1 THEN 1 ELSE 0 END,
                                    a.STARTDATE DESC,
                                    a.LOCATIONID DESC)
                                 r
                         FROM K12INTEL_STAGING_IC.HOUSEHOLDLOCATION a
                              INNER JOIN K12INTEL_STAGING_IC.ADDRESS b
                                 ON     a.STAGE_SOURCE = b.STAGE_SOURCE
                                    AND a.ADDRESSID = b.ADDRESSID
                              LEFT JOIN
                              (SELECT a.STAGE_SOURCE,
                                      a.ADDRESSID,
                                      a.VALUE CATCHMENT_CODE
                                 FROM k12intel_staging_ic.customaddress a
                                      INNER JOIN
                                      k12intel_staging_ic.CAMPUSATTRIBUTE b
                                         ON     a.ATTRIBUTEID = b.ATTRIBUTEID
                                            AND a.STAGE_SOURCE = b.STAGE_SOURCE
                                WHERE OBJECT = 'Address' AND ELEMENT = 'catchment')
                              c
                                 ON     b.ADDRESSID = c.ADDRESSID
                                    AND b.STAGE_SOURCE = c.STAGE_SOURCE) physical
                         ON     a.HOUSEHOLDID = physical.HOUSEHOLDID
                            AND a.STAGE_SOURCE = physical.STAGE_SOURCE
                            AND physical.r = 1
                WHERE a.r = 1;
      */
      INSERT INTO K12INTEL_STAGING_IC.TEMP_ADDRESS
         SELECT STAGE_SOURCE,
                COALESCE (HOUSEHOLDID,
                          0),
                PERSONID,
                PHONE,
                PHONEPRIVATE,
                PHYSICAL_ADDRESSID,
                PHYSICAL_NUMBER,
                PHYSICAL_STREET,
                PHYSICAL_TAG,
                PHYSICAL_PREFIX,
                PHYSICAL_DIR,
                PHYSICAL_APT,
                PHYSICAL_CITY,
                PHYSICAL_STATE,
                PHYSICAL_ZIP,
                COUNTY,
                PHYSICAL_POSTOFFICEBOX,
                PHYSICAL_XCOORD,
                PHYSICAL_YCOORD,
                MAILING_PRIVATE,
                MAILING_ADDRESSID,
                MAILING_NUMBER,
                MAILING_STREET,
                MAILING_TAG,
                MAILING_PREFIX,
                MAILING_DIR,
                MAILING_APT,
                MAILING_CITY,
                MAILING_STATE,
                MAILING_ZIP,
                MAILING_POSTOFFICEBOX,
                CATCHMENT_CODE
           FROM (SELECT DISTINCT
                        P.STAGE_SOURCE,
                        HM.HOUSEHOLDID,
                        P.PERSONID,
                        H.PHONE,
                        H.PHONEPRIVATE,
                        A.ADDRESSID PHYSICAL_ADDRESSID,
                        A."NUMBER" AS PHYSICAL_NUMBER,
                        A.STREET AS PHYSICAL_STREET,
                        A.TAG AS PHYSICAL_TAG,
                        A.PREFIX AS PHYSICAL_PREFIX,
                        A.DIR AS PHYSICAL_DIR,
                        A.APT PHYSICAL_APT,
                        A.CITY AS PHYSICAL_CITY,
                        A.STATE AS PHYSICAL_STATE,
                        A.ZIP AS PHYSICAL_ZIP,
                        A.COUNTY,
                        A.POSTOFFICEBOX AS PHYSICAL_POSTOFFICEBOX,
                        NULL PHYSICAL_XCOORD,
                        NULL PHYSICAL_YCOORD,
                        HL.PRIVATE MAILING_PRIVATE,
                        A.ADDRESSID MAILING_ADDRESSID,
                        A."NUMBER" AS MAILING_NUMBER,
                        A.STREET AS MAILING_STREET,
                        A.TAG AS MAILING_TAG,
                        A.PREFIX AS MAILING_PREFIX,
                        A.DIR AS MAILING_DIR,
                        A.APT MAILING_APT,
                        A.CITY AS MAILING_CITY,
                        A.STATE AS MAILING_STATE,
                        A.ZIP AS MAILING_ZIP,
                        A.POSTOFFICEBOX AS MAILING_POSTOFFICEBOX,
                        C.CATCHMENT_CODE,
                        /*
                                                ROW_NUMBER() OVER (PARTITION BY P.PERSONID
                                                ORDER BY
                                                        -- THIS IS THE BEST PRIORITY SORT ORDER FOR KIDS WITH MULTIPLE ADDRESSES (SOME KIDS HAVE AS MANY AS 6 ADDRESSES):
                                                        CASE WHEN MAI.SAA_ELEM IS NULL THEN 0 ELSE 1 END DESC,      -- MAI SAA DESC SORT SO NULLS COME LAST  (A NULL MEANS ITS AN INVALID ADDRESS OR NON-MILW ADDRESS)
                                                        NVL(HM.SECONDARY,0) ,             -- SECONDARY ASC SORT SO 0'S COME FIRST
                                                        NVL(RP.SEQ,255),                  -- SEQ ASC SORT - THIS IS THE FREEFORM "EMERGENCY PRIORITY" ENTRY FIELD WHICH IS A TINYINT WITH MAX VALUE = 255 - SCHOOLS ARE TRAINED TO ENTER 1 FOR THE PERSON TO BE NOTIFIED FIRST, THEN 2 NEXT, ETC... AND IF NULL THEN PUT LAST ...
                                                        NVL(RP.GUARDIAN,0) DESC,          -- RELATED PAIR GUARDIAN DESC SORT SO 1 COMES BEFORE 0
                                                        RP.MAILING DESC,                  -- MAILING DESC SORT SO 1 COMES BEFORE 0
                                                        A."NUMBER" DESC                  -- NUMBER COLUMN FROM ADDRESS TABLE DESC SORT SO NULLS COME LAST
                                                ) AS VADDR_SORTNUM
                                    FROM
                                        K12INTEL_STAGING_IC.PERSON P
                                            LEFT OUTER JOIN K12INTEL_STAGING_IC.RELATEDPAIR RP
                                                ON RP.PERSONID1 = P.PERSONID
                                                        AND (RP.ENDDATE IS NULL OR RP.ENDDATE>=TRUNC(SYSDATE) )
                                            LEFT OUTER JOIN K12INTEL_STAGING_IC.HOUSEHOLDMEMBER HM
                                                ON (HM.STARTDATE IS NULL OR TRUNC(HM.STARTDATE) <= TRUNC(SYSDATE))
                                                    AND (HM.ENDDATE IS NULL OR TRUNC(HM.ENDDATE) >= TRUNC(SYSDATE))
                                                    AND HM.PERSONID = P.PERSONID
                                            LEFT OUTER JOIN K12INTEL_STAGING_IC.HOUSEHOLDLOCATION HL
                                                ON HL.HOUSEHOLDID = H.HOUSEHOLDID
                                                    AND (HL.ENDDATE IS NULL OR TRUNC(HL.ENDDATE) >= TRUNC(SYSDATE) )
                                            LEFT OUTER JOIN K12INTEL_STAGING_IC.ADDRESS A
                                                ON HL.ADDRESSID = A.ADDRESSID
                                            LEFT OUTER JOIN K12INTEL_STAGING_IC.MAI_ADDRESS MAI           -- LEFT JOIN TO MAI SO GET VALID MILWAUKEE ADDRESSES SORTED AT TOP OF THE LIST AND SO DOESN'T EXCLUDE ANY NON-MILW OR INVALID ADDRESSES
                                                ON (         (A.MAI_RCD_NBR = MAI.MAI_RCD_NBR)                 -- 1ST MATCH ON MAI_RCD_NBR IN MAI TABLE
                                                            OR
                                                            ( UPPER(TRIM(A.CITY)) = 'MILWAUKEE' AND
                                                              UPPER(TRIM(A."NUMBER")) = MAI.HSE_NBR  AND
                                                              UPPER(TRIM(A.STREET)) = MAI.STREET  AND
                                                              UPPER(TRIM(A.PREFIX)) = MAI.DIR AND
                                                              UPPER(TRIM(A.TAG)) = MAI.STTYPE
                                                            )       -- IF MAI_RCD_NBR NOT EXISTS, THEN MATCH ON ADDRESS STREET #, NAME, DIRECTION, AND TYPE TO GET ATTENDANCE AREA SCHOOLS (SAA_ELEM/MID/HIGH).  NOTE THAT DON'T NEED TO MATCH ON ZIP BECAUSE THESE 4 COLS ALWAYS HAVE THE SAME SAA VALUES
                                                      )
                                                      AND
                                                      ( NVL(MAI.SAA_ELEM,0) <> 0 AND NVL(MAI.SAA_HIGH,0) <> 0 AND NVL(MAI.SAA_MID,0) <> 0 ) -- NOTE THAT ONLY ABOUT 40 RECORDS OUT OF 254380 TOTAL HAVE A 0 IN THESE 3 COLS, AND WANT ALL 3 FILLED IN
                        */

                        ROW_NUMBER ()
                           OVER (PARTITION BY p.personid
                                 ORDER BY
                                    -- this is the best priority sort order for kids with multiple addresses (some kids have as many as 6 addresses):
                                    CASE
                                       WHEN a.addressid IS NULL THEN 0
                                       ELSE 1
                                    END DESC, -- put records with an addressid first so missing/nulls come last
                                    CASE
                                       WHEN hm.personid = rp.personid1 THEN 0
                                       ELSE 1
                                    END, -- this puts the enrolled students household records before the relationships
                                    NVL (hm.secondary, 0), -- HM secondary asc sort so 0's come first
                                    NVL (hl.secondary, 0), -- HL secondary asc sort so 0's come first   (secondary here is usually used for po boxes)
                                    NVL (rp.seq, 255), -- RP SEQ asc sort - this is the freeform "Emergency Priority" entry field which is a tinyint with max value = 255 - Schools are trained to enter 1 for the person to be notified first, then 2 next, etc. and if null then put last
                                    NVL (rp.guardian, 0) DESC, -- RP guardian desc sort so 1 comes before 0
                                    rp.mailing DESC, -- RP mailing desc sort so 1 comes before 0
                                    hl.mailing DESC, -- HL mailing desc sort so 1 comes before 0
                                    a.postofficebox, -- po box checkbox asc sort so 0s (no po box) come first
                                    a."NUMBER" DESC -- number column from address table desc sort so nulls come last
                                                   )
                           AS vaddr_sortnum
                   FROM k12intel_staging_ic.Person p
                        LEFT OUTER JOIN k12intel_staging_ic.relatedpair rp
                           ON     rp.personid1 = p.personid
                              AND (   rp.enddate IS NULL
                                   OR rp.endDate >= TRUNC (SYSDATE))
                        LEFT OUTER JOIN
                        k12intel_staging_ic.HouseholdMember hm
                           ON     (   hm.startDate IS NULL
                                   OR TRUNC (hm.startDate) <= TRUNC (SYSDATE))
                              AND (   hm.endDate IS NULL
                                   OR TRUNC (hm.endDate) >= TRUNC (SYSDATE))
                              AND (   hm.personID = p.personID
                                   OR hm.personid = rp.personid2) -- join the enrolled student's personid to HM and the Related-Pair's personid2 to HM  - to get all possible addresses
                        LEFT OUTER JOIN K12INTEL_STAGING_IC.HOUSEHOLD H
                           ON H.HOUSEHOLDID = HM.HOUSEHOLDID
                        LEFT OUTER JOIN
                        k12intel_staging_ic.HouseholdLocation hl
                           ON     hl.householdID = hm.householdID
                              AND (   hl.enddate IS NULL
                                   OR TRUNC (hl.endDate) >= TRUNC (SYSDATE))
                        LEFT OUTER JOIN k12intel_staging_ic.address a
                           ON hl.addressid = a.addressid
                        LEFT JOIN
                        (SELECT a.STAGE_SOURCE,
                                a.ADDRESSID,
                                a.VALUE CATCHMENT_CODE
                           FROM k12intel_staging_ic.customaddress a
                                INNER JOIN
                                k12intel_staging_ic.CAMPUSATTRIBUTE b
                                   ON     a.ATTRIBUTEID = b.ATTRIBUTEID
                                      AND a.STAGE_SOURCE = b.STAGE_SOURCE
                          WHERE OBJECT = 'Address' AND ELEMENT = 'catchment')
                        c
                           ON     a.ADDRESSID = c.ADDRESSID
                              AND a.STAGE_SOURCE = c.STAGE_SOURCE)
          WHERE VADDR_SORTNUM = 1;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;
         v_student_record.SYS_AUDIT_IND := 'Y';
         v_AUDIT_NATURAL_KEY := NULL;

         K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                              'POPULATE ADDRESS TABLE',
                                              v_WAREHOUSE_KEY,
                                              v_AUDIT_NATURAL_KEY,
                                              'insert into address table',
                                              SQLERRM,
                                              'Y',
                                              v_AUDIT_BASE_SEVERITY);
   END;

   BEGIN
      EXECUTE IMMEDIATE
         (   'delete K12INTEL_STAGING_IC.TEMP_CHANGE_COUNT where stage_source='''
          || p_PARAM_STAGE_SOURCE
          || '''');

      INSERT INTO K12INTEL_STAGING_IC.TEMP_CHANGE_COUNT
         SELECT a.stage_source,
                a.personid,
                'R' change_type,
                COUNT (*) - 1 change_count
           FROM k12intel_staging_ic.householdmember hm
                JOIN K12INTEL_STAGING_IC.TEMP_ADDRESS a
                   ON     hm.householdid = a.householdid
                      AND hm.personid = a.personid
                      AND hm.stage_source = a.stage_source
                JOIN k12intel_staging_ic.householdlocation hl
                   ON     a.householdid = hl.householdid
                      AND a.stage_source = hl.stage_source
         GROUP BY a.stage_source,
                  a.personid
         HAVING COUNT (*) > 1
         UNION
         SELECT a.stage_source,
                a.personid,
                'S' change_type,
                COUNT (*) - 1 change_count
           FROM k12intel_staging_ic.enrollment a
                JOIN k12intel_staging_ic.calendar b
                   ON     a.calendarid = b.calendarid
                      AND a.stage_source = b.stage_source
                      AND v_local_data_date BETWEEN b.startdate AND b.enddate
          WHERE     a.stage_source = p_PARAM_STAGE_SOURCE
                AND a.stage_deleteflag = 0
                AND a.servicetype = 'P'
                AND a.startdate < v_local_data_date
         GROUP BY a.stage_source,
                  a.personid
         HAVING COUNT (*) > 1;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;
         v_student_record.SYS_AUDIT_IND := 'Y';
         v_AUDIT_NATURAL_KEY := NULL;

         K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
            v_SYS_ETL_SOURCE,
            'POPULATE CHANGE_COUNT TABLE',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'insert into change count table',
            SQLERRM,
            'Y',
            v_AUDIT_BASE_SEVERITY);
   END;

   BEGIN
      EXECUTE IMMEDIATE
         (   'delete K12INTEL_STAGING_IC.TEMP_ENROLLMENTS where stage_source='''
          || p_PARAM_STAGE_SOURCE
          || '''');

      INSERT INTO K12INTEL_STAGING_IC.TEMP_ENROLLMENTS
         SELECT E.STAGE_SOURCE,
                E.PERSONID,
                P.STUDENTNUMBER,
                E.STARTDATE,
                COALESCE (E.ENDDATE,
                          C.ENDDATE)
                   ENDDATE,
                E.GRADE,
                C.DISTRICTID,
                C.SCHOOLID,
                C.NAME CALENDARNAME,
                C.ENDYEAR - 1 CALENDARYEAR,
                C.CALENDARID,
                E.STARTSTATUS,
                E.ENDSTATUS
           FROM K12INTEL_STAGING_IC.ENROLLMENT E
                JOIN K12INTEL_STAGING_IC.PERSON P
                   ON     E.PERSONID = P.PERSONID
                      AND E.STAGE_SOURCE = P.STAGE_SOURCE
                JOIN K12INTEL_STAGING_IC.CALENDAR C
                   ON     E.CALENDARID = C.CALENDARID
                      AND E.STAGE_SOURCE = C.STAGE_SOURCE
          WHERE E.STAGE_DELETEFLAG = 0 AND P.STUDENTNUMBER IS NOT NULL;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;
         v_student_record.SYS_AUDIT_IND := 'Y';
         v_AUDIT_NATURAL_KEY := NULL;

         K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
            v_SYS_ETL_SOURCE,
            'POPULATE ENROLLMENTS TABLE',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'insert into enrollments table',
            SQLERRM,
            'Y',
            v_AUDIT_BASE_SEVERITY);
   END;

   BEGIN
      v_DEFAULT_DISTRICT_CODE :=
         K12INTEL_METADATA.GET_DISTRICT_CODE_IC (p_PARAM_STAGE_SOURCE);

      IF v_DEFAULT_DISTRICT_CODE IS NULL
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;
         v_student_record.SYS_AUDIT_IND := 'Y';
         v_AUDIT_NATURAL_KEY := NULL;

         K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
            v_SYS_ETL_SOURCE,
            'DEFAULT_DISTRICT_CODE',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'FAILED TO GET DEFAULT DISTRICT_CODE',
            NULL,
            'Y',
            v_AUDIT_BASE_SEVERITY);

         RAISE NO_DATA_FOUND;
      END IF;
   END;

   p_PARAM_EXECUTION_STATUS := 0;

   BEGIN
      FOR v_some_data IN c_some_cursor
      LOOP
         BEGIN
            v_student_record.sys_etl_source := v_SYS_ETL_SOURCE;
            v_details_record.sys_etl_source := v_SYS_ETL_SOURCE;
            v_details_record.sys_audit_ind := 'N';
            v_student_record.sys_audit_ind := 'N';
            v_student_record.SYS_PARTITION_VALUE := 0;

            CASE
               WHEN v_some_data.WITHDRAWDATE < v_local_data_date
               THEN
                  v_last_activity_date := v_some_data.WITHDRAWDATE;
               WHEN v_some_data.ENDDATE < v_local_data_date
               THEN
                  v_last_activity_date := v_some_data.ENDDATE;
               ELSE
                  v_last_activity_date := v_local_data_date;
            END CASE;


            v_BASE_NATURALKEY_TXT :=
                  'STAGE_SOURCE='
               || v_some_data.STAGE_SOURCE
               || ';PERSONID='
               || TO_CHAR (v_some_data.PERSONID)
               || ';STUDENTNUMBER='
               || v_some_data.STUDENTNUMBER;

            ---------------------------------------------------------------
            -- STUDENT_KEY
            ---------------------------------------------------------------
            BEGIN
               K12INTEL_METADATA.GEN_STUDENT_KEY (
                  v_some_data.STUDENTNUMBER,
                  v_some_data.STAGE_SOURCE,
                  v_student_record.STUDENT_KEY);

               IF v_student_record.STUDENT_KEY = 0
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_KEY := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_KEY',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'ERROR GENERATING KEY',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);

                  RAISE_APPLICATION_ERROR (-20000,
                                           'FAILED TO GENERATE STUDENT_KEY');
               END IF;
            END;

            --------------------------------------------------------------
            -- STUDENT_ATTRIB_KEY
            ---------------------------------------------------------------
            BEGIN
               v_student_record.STUDENT_ATTRIB_KEY := -1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ATTRIB_KEY := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ATTRIB_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ATTRIB_KEY := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ATTRIB_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ATTRIB_KEY := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ATTRIB_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- FIND FUTURE ENROLLMENT (IF ANY)
            ---------------------------------------------------------------
            BEGIN
               SELECT STARTDATE,
                      ENDDATE,
                      SCHOOLID,
                      GRADE,
                      DISTRICTID,
                      CALENDARID
                 INTO v_LOCAL_STARTDATE,
                      v_LOCAL_ENDDATE,
                      v_LOCAL_SCHOOLID,
                      v_LOCAL_GRADE,
                      v_LOCAL_DISTRICTID,
                      v_LOCAL_CALENDARID
                 FROM (SELECT STARTDATE,
                              ENDDATE,
                              SCHOOLID,
                              GRADE,
                              DISTRICTID,
                              CALENDARID,
                              RANK ()
                                 OVER (PARTITION BY STUDENTNUMBER,
                                                    STAGE_SOURCE
                                       ORDER BY
                                          STARTDATE DESC,
                                          ENDDATE DESC)
                                 R
                         FROM K12INTEL_STAGING_IC.TEMP_ENROLLMENTS
                        WHERE     STUDENTNUMBER = v_some_data.STUDENTNUMBER
                              AND v_local_data_date < STARTDATE
                              AND STAGE_SOURCE = v_some_data.STAGE_SOURCE) X
                WHERE R = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_LOCAL_STARTDATE := v_some_data.STARTDATE;
                  v_LOCAL_ENDDATE := v_some_data.ENDDATE;
                  v_LOCAL_GRADE := '--';
                  v_LOCAL_DISTRICTID := v_some_data.DISTRICTID;
                  v_LOCAL_SCHOOLID := 0;
                  v_LOCAL_CALENDARID := v_some_data.CALENDARID;
               WHEN TOO_MANY_ROWS
               THEN
                  v_LOCAL_STARTDATE := v_some_data.STARTDATE;
                  v_LOCAL_ENDDATE := v_some_data.ENDDATE;
                  v_LOCAL_GRADE := '--';
                  v_LOCAL_DISTRICTID := v_some_data.DISTRICTID;
                  v_LOCAL_SCHOOLID := 0;
                  v_LOCAL_CALENDARID := v_some_data.CALENDARID;
               WHEN OTHERS
               THEN
                  v_LOCAL_STARTDATE := v_some_data.STARTDATE;
                  v_LOCAL_ENDDATE := v_some_data.ENDDATE;
                  v_LOCAL_GRADE := '--';
                  v_LOCAL_DISTRICTID := v_some_data.DISTRICTID;
                  v_LOCAL_SCHOOLID := 0;
                  v_LOCAL_CALENDARID := v_some_data.CALENDARID;
            END;

            ---------------------------------------------------------------
            -- STUDENT_ACTIVITY_INDICATOR
            ---------------------------------------------------------------
            BEGIN
               CASE
                  WHEN    v_local_data_date BETWEEN v_some_data.STARTDATE
                                                AND COALESCE (
                                                       v_some_data.WITHDRAWDATE,
                                                       v_some_data.ENDDATE,
                                                       TO_DATE ('12/31/9999',
                                                                'mm/dd/yyyy'))
                       OR v_local_data_date < v_LOCAL_STARTDATE
                  THEN
                     v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Active';
                  ELSE
                     v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Inactive';
               /*
                              WHEN v_some_data.CALENDAR_EXCLUDE = 1
                              THEN
                                 v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Exclude';
                              WHEN v_some_data.NOSHOW = 1
                              THEN
                                 v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Active';
                              WHEN
                                  v_some_data.SERVICETYPE = 'P'
                                   AND v_local_data_date BETWEEN v_some_data.STARTDATE
                                                             AND COALESCE (
                                                                    v_some_data.WITHDRAWDATE,
                                                                    v_some_data.ENDDATE,
                                                                    TO_DATE ('12/31/9999',
                                                                             'mm/dd/yyyy'))
                                   AND v_some_data.ENDYEAR - 1 = v_LOCAL_CURRENT_SCHOOL_YEAR
                              THEN
                                 v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Active';
                              ELSE
                                 v_student_record.STUDENT_ACTIVITY_INDICATOR := 'Inactive';
               */
               END CASE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ACTIVITY_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ACTIVITY_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ACTIVITY_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ACTIVITY_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ACTIVITY_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ACTIVITY_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_STATUS
            ---------------------------------------------------------------
            BEGIN
               IF     v_some_data.CALENDAR_EXCLUDE = 1
                  AND (   v_local_data_date BETWEEN v_some_data.STARTDATE
                                                AND COALESCE (
                                                       v_some_data.WITHDRAWDATE,
                                                       v_some_data.ENDDATE,
                                                       TO_DATE ('12/31/9999',
                                                                'mm/dd/yyyy'))
                       OR v_local_data_date < v_LOCAL_STARTDATE)
               THEN
                  v_student_record.STUDENT_STATUS := 'Exclude';
               ELSIF     v_some_data.NOSHOW = 1
                     AND v_local_data_date BETWEEN v_some_data.STARTDATE
                                               AND COALESCE (
                                                      v_some_data.WITHDRAWDATE,
                                                      v_some_data.ENDDATE,
                                                      TO_DATE ('12/31/9999',
                                                               'mm/dd/yyyy'))
               THEN
                  v_student_record.STUDENT_STATUS := 'No Show';
               ELSIF     v_some_data.ENDSTATUS = 'HSC'
                     AND v_local_data_date > v_LOCAL_ENDDATE
               THEN
                  v_student_record.STUDENT_STATUS := 'Graduate';
               ELSIF v_local_data_date BETWEEN v_some_data.STARTDATE
                                           AND COALESCE (
                                                  v_some_data.WITHDRAWDATE,
                                                  v_some_data.ENDDATE,
                                                  TO_DATE ('12/31/9999',
                                                           'mm/dd/yyyy'))
               THEN
                  v_student_record.STUDENT_STATUS := 'Enrolled';
               ELSIF v_local_data_date < v_LOCAL_STARTDATE
               THEN
                  v_student_record.STUDENT_STATUS := 'Assigned';
               ELSE
                  v_student_record.STUDENT_STATUS := 'Withdrawn';
               /*
                           ELSIF     v_some_data.ENDSTATUS IS NOT NULL
                                 AND COALESCE (v_some_data.WITHDRAWDATE,
                                               v_some_data.ENDDATE,
                                               TO_DATE ('12/31/9999', 'mm/dd/yyyy')) <
                                        v_local_data_date
                           THEN
                              SELECT SUBSTR (a.NAME, 1, 30)
                                INTO v_student_record.STUDENT_STATUS
                                FROM k12intel_staging_ic.CampusDictionary a
                                     INNER JOIN k12intel_staging_ic.CampusAttribute b
                                        ON     a.attributeID = b.attributeID
                                           AND a.STAGE_SOURCE = b.STAGE_SOURCE
                               WHERE     b.object = 'Enrollment'
                                     AND b.element = 'endStatus'
                                     AND a.CODE = v_some_data.ENDSTATUS
                                     AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                           ELSIF v_some_data.WITHDRAWDATE < v_local_data_date
                           THEN
                              v_student_record.STUDENT_STATUS := 'Inactive';
                           ELSIF
                                v_some_data.SERVICETYPE = 'P'
                                 AND v_local_data_date BETWEEN v_some_data.STARTDATE
                                                           AND COALESCE (
                                                                  v_some_data.WITHDRAWDATE,
                                                                  v_some_data.ENDDATE,
                                                                  TO_DATE ('12/31/9999',
                                                                           'mm/dd/yyyy'))
                           THEN
                              v_student_record.STUDENT_STATUS := 'Active';
                           --Do on break check
                           ELSE
                              v_MIN_START_DATE := NULL;

                              SELECT MIN ("DATE")
                                INTO v_MIN_START_DATE
                                FROM K12INTEL_STAGING_IC."DAY"
                               WHERE     calendarid = v_some_data.calendarid
                                     AND ATTENDANCE = 1
                                     AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;

                              IF     v_some_data.SCHOOL_SUB_TYPE = '1'
                                 AND v_some_data.SERVICETYPE = 'P'
                                 AND v_some_data.ENDYEAR - 1 = v_LOCAL_CURRENT_SCHOOL_YEAR
                                 AND v_local_data_date BETWEEN v_some_data.STARTDATE
                                                           AND v_MIN_START_DATE
                              THEN
                                 v_student_record.STUDENT_STATUS := 'On Break';
                              ELSE
                                 --v_student_record.SYS_AUDIT_IND := 'Y';
                                 v_student_record.STUDENT_STATUS := 'Inactive';
               */

               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_STATUS := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';ENDSTATUS='
                     || v_some_data.ENDSTATUS;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_STATUS',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_STATUS := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';ENDSTATUS='
                     || v_some_data.ENDSTATUS;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_STATUS',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_STATUS := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_STATUS',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_CURRENT_SCHOOL_CODE
            ---------------------------------------------------------------
            BEGIN
               SELECT a.VALUE
                 INTO v_student_record.STUDENT_CURRENT_SCHOOL_CODE
                 FROM k12intel_staging_ic.customschool a
                      INNER JOIN k12intel_staging_ic.CAMPUSATTRIBUTE b
                         ON     a.ATTRIBUTEID = b.ATTRIBUTEID
                            AND a.stage_source = b.stage_source
                WHERE     a.SCHOOLID = v_some_data.SCHOOLID --v_LOCAL_SCHOOLID S.Schnelz 01-29-2015
                      --v_some_data.SCHOOLID
                      AND b.object = 'School'
                      AND b.ELEMENT = 'LOCALSCHOOLNUM'
                      AND a.STAGE_SIS_SCHOOL_YEAR =
                             v_LOCAL_CURRENT_SCHOOL_YEAR
                      AND a.stage_deleteflag = 0
                      AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_CURRENT_SCHOOL_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_CURRENT_SCHOOL_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ID',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);

                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_CURRENT_SCHOOL_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- Replace IC School Code with "Standard School Code"
            ---------------------------------------------------------------
            v_student_record.STUDENT_CURRENT_SCHOOL_CODE :=
               K12INTEL_METADATA.REPLACE_IC_SCHOOL_NUMBER (
                  v_student_record.STUDENT_CURRENT_SCHOOL_CODE);


            ---------------------------------------------------------------
            -- SCHOOL_KEY
            ---------------------------------------------------------------
            BEGIN
               /*
                               SELECT c.SCHOOL_KEY, c.SCHOOL_NAME
                               into v_student_record.SCHOOL_KEY, v_student_record.STUDENT_CURRENT_SCHOOL
                               FROM K12INTEL_STAGING_IC.SCHOOL a
                               inner join K12INTEL_STAGING_IC.DISTRICT b
                               on a.DISTRICTID = b.DISTRICTID and a.STAGE_SOURCE = b.STAGE_SOURCE
                               inner join K12INTEL_DW.DTBL_SCHOOLS c
                               on a."NUMBER" = c.SCHOOL_CODE and b."NUMBER" = c.DISTRICT_CODE
                               WHERE a.DISTRICTID = v_some_data.DISTRICTID
                                   and a.SCHOOLID = v_some_data.SCHOOLID
                                   and a.STAGE_SIS_SCHOOL_YEAR = v_LOCAL_CURRENT_SCHOOL_YEAR
                                   and a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               */
               SELECT SCHOOL_KEY,
                      SCHOOL_NAME
                 INTO v_student_record.SCHOOL_KEY,
                      v_student_record.STUDENT_CURRENT_SCHOOL
                 FROM K12INTEL_DW.DTBL_SCHOOLS
                WHERE SCHOOL_CODE =
                         v_student_record.STUDENT_CURRENT_SCHOOL_CODE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.SCHOOL_KEY := 0;
                  --v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_student_record.STUDENT_CURRENT_SCHOOL := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'SCHOOL_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.SCHOOL_KEY := 0;
                  --v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_student_record.STUDENT_CURRENT_SCHOOL := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'SCHOOL_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.SCHOOL_KEY := 0;
                  --v_student_record.STUDENT_CURRENT_SCHOOL_CODE := '@ERR';
                  v_student_record.STUDENT_CURRENT_SCHOOL := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'SCHOOL_KEY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_ID
            ---------------------------------------------------------------
            BEGIN
               v_student_record.STUDENT_ID := v_some_data.STUDENTNUMBER;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ID := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ID',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ID := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;
                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ID',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ID',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);

                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ID := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ID',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_NAME
            ---------------------------------------------------------------
            BEGIN
               IF RTRIM (v_some_data.LASTNAME) IS NOT NULL
               THEN
                  v_student_record.STUDENT_NAME :=
                     RTRIM (v_some_data.LASTNAME) || ', ';
               END IF;

               IF RTRIM (v_some_data.FIRSTNAME) IS NOT NULL
               THEN
                  v_student_record.STUDENT_NAME :=
                        v_student_record.STUDENT_NAME
                     || RTRIM (v_some_data.FIRSTNAME)
                     || ' ';
               END IF;

               IF RTRIM (v_some_data.MIDDLENAME) IS NOT NULL
               THEN
                  v_student_record.STUDENT_NAME :=
                        v_student_record.STUDENT_NAME
                     || SUBSTR (v_some_data.MIDDLENAME,
                                1,
                                1)
                     || '. ';
               END IF;

               IF RTRIM (v_some_data.SUFFIX) IS NOT NULL
               THEN
                  v_student_record.STUDENT_NAME :=
                        v_student_record.STUDENT_NAME
                     || RTRIM (v_some_data.SUFFIX);
               END IF;

               v_student_record.STUDENT_NAME :=
                  RTRIM (v_student_record.STUDENT_NAME);
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_NAME := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_NAME',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_NAME := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_NAME',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_NAME := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_NAME',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_FTE_GROUP
            ---------------------------------------------------------------
            BEGIN
               v_student_record.STUDENT_FTE_GROUP := '--';
            /*
               CASE
                  WHEN v_some_data.PERCENTENROLLED = 100
                  THEN
                     'Full-Time'
                  WHEN v_some_data.PERCENTENROLLED = 50
                  THEN
                     'Part-Time'
                  WHEN v_some_data.PERCENTENROLLED = 0
                  THEN
                     'Educated Outside MPS'
                  ELSE
                     '@ERR'
               END;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_student_record.SYS_AUDIT_IND := 'Y';
               v_student_record.STUDENT_FTE_GROUP := '@ERR';
               v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;

               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                    'STUDENT_FTE_GROUP',
                                                    v_WAREHOUSE_KEY,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'NO_DATA_FOUND',
                                                    SQLERRM,
                                                    'N',
                                                    v_AUDIT_BASE_SEVERITY);
            WHEN TOO_MANY_ROWS
            THEN
               v_student_record.SYS_AUDIT_IND := 'Y';
               v_student_record.STUDENT_FTE_GROUP := '@ERR';
               v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;
               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                    'STUDENT_FTE_GROUP',
                                                    v_WAREHOUSE_KEY,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'TOO_MANY_ROWS',
                                                    SQLERRM,
                                                    'N',
                                                    v_AUDIT_BASE_SEVERITY);
            WHEN OTHERS
            THEN
               v_student_record.SYS_AUDIT_IND := 'Y';
               v_student_record.STUDENT_FTE_GROUP := '@ERR';
               v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;
               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                    'STUDENT_FTE_GROUP',
                                                    v_WAREHOUSE_KEY,
                                                    v_AUDIT_NATURAL_KEY,
                                                    'Untrapped Error',
                                                    SQLERRM,
                                                    'Y',
                                                    v_AUDIT_BASE_SEVERITY);
*/
            END;


            ---------------------------------------------------------------
            -- STUDENT_GENDER_CODE
            ---------------------------------------------------------------
            BEGIN
               v_student_record.STUDENT_GENDER_CODE :=
                  NVL (v_some_data.GENDER, 'U');
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_GENDER
            ---------------------------------------------------------------
            BEGIN
               IF v_student_record.STUDENT_GENDER_CODE = 'M'
               THEN
                  v_student_record.STUDENT_GENDER := 'Male';
               ELSIF v_student_record.STUDENT_GENDER_CODE = 'F'
               THEN
                  v_student_record.STUDENT_GENDER := 'Female';
               ELSE
                  v_student_record.STUDENT_GENDER := 'Unknown';
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_GENDER := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_GENDER',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_AGE
            ---------------------------------------------------------------
            BEGIN
               v_student_record.STUDENT_AGE :=
                  FLOOR (
                     (v_last_activity_date - v_some_data.BIRTHDATE) / 365);
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_AGE := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_AGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_AGE := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_AGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_AGE := 0;
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_AGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_RACE_CODE
            ---------------------------------------------------------------
            BEGIN
               IF v_some_data.RACEETHNICITYFED IS NULL
               THEN
                  v_student_record.STUDENT_RACE_CODE := 'Unknown';
               ELSE
                  SELECT DOMAIN_DECODE
                    INTO v_student_record.STUDENT_RACE_CODE
                    FROM k12intel_userdata.xtbl_domain_decodes
                   WHERE     domain_name = 'STUDENT_ORIGIN_IND_FIELDS'
                         AND domain_code = v_some_data.RACEETHNICITYFED;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';RACEETHNICITYFED='
                     || v_some_data.RACEETHNICITYFED;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';RACEETHNICITYFED='
                     || v_some_data.RACEETHNICITYFED;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE_CODE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_RACE
            ---------------------------------------------------------------
            BEGIN
               IF v_some_data.RACEETHNICITYFED IS NULL
               THEN
                  v_student_record.STUDENT_RACE := 'Unknown';
               ELSE
                  IF v_some_data.HISPANICETHNICITY = 'Y'
                  THEN
                     v_student_record.STUDENT_RACE := 'Hispanic';
                  ELSIF v_some_data.RACEETHNICITYFED = '7'
                  THEN
                     v_student_record.STUDENT_RACE := 'Multi';
                  ELSE
                     SELECT a."NAME"
                       INTO v_student_record.STUDENT_RACE
                       FROM k12intel_Staging_ic.RACEETHNICITY a
                      WHERE     a.FEDERALCODE = v_some_data.RACEETHNICITYFED
                            AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';RACEETHNICITYFED='
                     || v_some_data.RACEETHNICITYFED;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';RACEETHNICITYFED='
                     || v_some_data.RACEETHNICITYFED;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_RACE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_RACE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_ETHNICITY_CODE
            ---------------------------------------------------------------
            BEGIN
               --v_student_record.STUDENT_ETHNICITY_CODE :=
               CASE
                  WHEN v_some_data.HISPANICETHNICITY = 'Y'
                  THEN
                     v_student_record.STUDENT_ETHNICITY_CODE := 'HISP';
                  WHEN v_some_data.HISPANICETHNICITY = 'N'
                  THEN
                     v_student_record.STUDENT_ETHNICITY_CODE := 'NOTHISP';
                  ELSE
                     v_student_record.STUDENT_ETHNICITY_CODE := '--';
               END CASE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ETHNICITY_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ETHNICITY_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ETHNICITY_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_ETHNICITY
            ---------------------------------------------------------------
            BEGIN
               -- v_student_record.STUDENT_ETHNICITY :=
               CASE
                  WHEN v_some_data.HISPANICETHNICITY = 'Y'
                  THEN
                     v_student_record.STUDENT_ETHNICITY := 'Hispanic';
                  WHEN v_some_data.HISPANICETHNICITY = 'N'
                  THEN
                     v_student_record.STUDENT_ETHNICITY := 'Not Hispanic';
                  ELSE
                     v_student_record.STUDENT_ETHNICITY := '--';
               END CASE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ETHNICITY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ETHNICITY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ETHNICITY := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_ETHNICITY',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_LANGUAGE
            ---------------------------------------------------------------
            BEGIN
               IF v_some_data.LANGUAGE IS NULL
               THEN
                  v_student_record.STUDENT_LANGUAGE := 'English';
               ELSE
                  SELECT a.NAME
                    INTO v_student_record.STUDENT_LANGUAGE
                    FROM k12intel_staging_ic.CampusDictionary a
                         INNER JOIN k12intel_staging_ic.CampusAttribute b
                            ON     a.attributeID = b.attributeID
                               AND a.STAGE_SOURCE = b.STAGE_SOURCE
                   WHERE     b.object = 'Enrollment'
                         AND b.element = 'language'
                         AND a.CODE = v_some_data.LANGUAGE
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_LANGUAGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'NO_DATA_FOUND',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_LANGUAGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'TOO_MANY_ROWS',
                                                       SQLERRM,
                                                       'N',
                                                       v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                                                       'STUDENT_LANGUAGE',
                                                       v_WAREHOUSE_KEY,
                                                       v_AUDIT_NATURAL_KEY,
                                                       'Untrapped Error',
                                                       SQLERRM,
                                                       'Y',
                                                       v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_HOME_LANGUAGE
            ---------------------------------------------------------------
            BEGIN
               /*SELECT NVL (c.NAME, 'English')
                 INTO v_student_record.STUDENT_HOME_LANGUAGE
                 FROM k12intel_staging_ic.enrollment a
                      LEFT JOIN k12intel_staging_ic.CampusAttribute b
                         ON     b.object = 'Enrollment'
                            AND b.element = 'language'
                            AND a.stage_source = b.stage_source
                      LEFT JOIN k12intel_staging_ic.CampusDictionary c
                         ON     a.LANGUAGE = c.CODE
                            AND b.attributeID = c.attributeID
                            AND b.STAGE_SOURCE = c.STAGE_SOURCE
                WHERE     a.ENROLLMENTID = v_some_data.ENROLLMENTID
                      AND a.stage_source = v_some_data.stage_source;*/
               IF v_some_data.HOMEPRIMARYLANGUAGE IS NULL
               THEN
                  v_student_record.STUDENT_HOME_LANGUAGE := '--';
               ELSE
                  SELECT a.NAME
                    INTO v_student_record.STUDENT_HOME_LANGUAGE
                    FROM k12intel_staging_ic.CampusDictionary a
                         INNER JOIN k12intel_staging_ic.CampusAttribute b
                            ON     a.attributeID = b.attributeID
                               AND a.STAGE_SOURCE = b.STAGE_SOURCE
                   WHERE     b.object = 'Definition'
                         AND b.element = 'iso639-2Language'
                         AND a.CODE = v_some_data.HOMEPRIMARYLANGUAGE
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_HOME_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_HOME_LANGUAGE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_HOME_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_HOME_LANGUAGE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_HOME_LANGUAGE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_HOME_LANGUAGE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_COUNTRY_OF_CITIZENSHIP
            ---------------------------------------------------------------
            BEGIN
               IF v_some_data.CITIZENSHIP IS NULL
               THEN
                  v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP := '--';
               ELSE
                  SELECT a.NAME
                    INTO v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP
                    FROM k12intel_staging_ic.CampusDictionary a
                         INNER JOIN k12intel_staging_ic.CampusAttribute b
                            ON     a.attributeID = b.attributeID
                               AND a.STAGE_SOURCE = b.STAGE_SOURCE
                   WHERE     b.object = 'Enrollment'
                         AND b.element = 'citizenship'
                         AND a.CODE = v_some_data.CITIZENSHIP
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_CITIZENSHIP',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_CITIZENSHIP',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_CITIZENSHIP',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_COUNTRY_OF_BIRTH
            ---------------------------------------------------------------
            BEGIN
               IF v_some_data.BIRTHCOUNTRY IS NULL
               THEN
                  v_student_record.STUDENT_COUNTRY_OF_BIRTH := '--';
               ELSE
                  SELECT SUBSTR (A.NAME,
                                 1,
                                 30)
                    INTO v_student_record.STUDENT_COUNTRY_OF_BIRTH
                    FROM k12intel_staging_ic.CampusDictionary a
                         INNER JOIN k12intel_staging_ic.CampusAttribute b
                            ON     a.attributeID = b.attributeID
                               AND a.STAGE_SOURCE = b.STAGE_SOURCE
                   WHERE     b.object = 'Identity'
                         AND b.element = 'birthCountry'
                         AND a.CODE = v_some_data.BIRTHCOUNTRY
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_BIRTH := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_BIRTH',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_BIRTH := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_BIRTH',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_COUNTRY_OF_BIRTH := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_COUNTRY_OF_BIRTH',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_FOODSERVICE_ELIG_CODE
            ---------------------------------------------------------------
            BEGIN
               SELECT ELIGIBILITY
                 INTO v_student_record.STUDENT_FOODSERVICE_ELIG_CODE
                 FROM (SELECT ELIGIBILITY,
                              RANK ()
                              OVER (
                                 PARTITION BY stage_source,
                                              personid
                                 ORDER BY
                                    endyear DESC,
                                    COALESCE (enddate,
                                              TO_DATE ('12/31/9999',
                                                       'mm/dd/yyyy')) DESC,
                                    startdate DESC)
                                 r
                         FROM k12intel_staging_ic.poseligibility
                        WHERE     personid = v_some_data.personid
                              AND v_last_activity_date BETWEEN startdate
                                                           AND COALESCE (
                                                                  enddate,
                                                                  TO_DATE (
                                                                     '12/31/9999',
                                                                     'mm/dd/yyyy'))
                              AND stage_source = v_some_data.stage_source) x
                WHERE x.r = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.STUDENT_FOODSERVICE_ELIG_CODE := 'NA';
               /*v_student_record.SYS_AUDIT_IND := 'Y';
               v_student_record.STUDENT_FOODSERVICE_ELIG_CODE := '@ERR';
               v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;

               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                   v_SYS_ETL_SOURCE,
                   'STUDENT_FOODSERVICE_ELIG_CODE',
                   v_WAREHOUSE_KEY,
                   v_AUDIT_NATURAL_KEY,
                   'NO_DATA_FOUND',
                   sqlerrm,
                   'N',
                   v_AUDIT_BASE_SEVERITY
               );*/
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_ELIG_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_ELIG_CODE := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG_CODE',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_FOODSERVICE_INDICATOR
            ---------------------------------------------------------------
            BEGIN
               --  v_student_record.STUDENT_FOODSERVICE_INDICATOR :=
               CASE
                  WHEN v_student_record.STUDENT_FOODSERVICE_ELIG_CODE IN ('F',
                                                                          'R')
                  THEN
                     v_student_record.STUDENT_FOODSERVICE_INDICATOR := 'Yes';
                  ELSE
                     v_student_record.STUDENT_FOODSERVICE_INDICATOR := 'No';
               END CASE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_FOODSERVICE_ELIG
            ---------------------------------------------------------------
            BEGIN
               IF v_student_record.STUDENT_FOODSERVICE_ELIG_CODE IN ('NA',
                                                                     'P',
                                                                     'S')
               THEN
                  v_student_record.STUDENT_FOODSERVICE_ELIG := 'Full Price';
               ELSIF v_student_record.STUDENT_FOODSERVICE_ELIG_CODE = 'F'
               THEN
                  v_student_record.STUDENT_FOODSERVICE_ELIG := 'Free lunch';
               ELSIF v_student_record.STUDENT_FOODSERVICE_ELIG_CODE = 'R'
               THEN
                  v_student_record.STUDENT_FOODSERVICE_ELIG := 'Reduced lunch';
               ELSIF v_student_record.STUDENT_FOODSERVICE_ELIG_CODE = 'D'
               THEN
                  v_student_record.STUDENT_FOODSERVICE_ELIG := 'Denied';
               ELSE
                  v_student_record.STUDENT_FOODSERVICE_ELIG := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY :=
                        v_BASE_NATURALKEY_TXT
                     || ';STUDENT_FOODSERVICE_ELIG='
                     || v_student_record.STUDENT_FOODSERVICE_ELIG_CODE;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Unknown eligibility code',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_ELIG := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'NO_DATA_FOUND ' || v_some_data.MEALSTATUS,
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_ELIG := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_FOODSERVICE_ELIG := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_FOODSERVICE_ELIG',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;
--v_student_record.STUDENT_SPECIAL_ED_INDICATOR,
--                      v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP,
--                      v_student_record.STUDENT_SPECIAL_ED_CLASS

            ---------------------------------------------------------------
            -- STUDENT_SPECIAL_ED_INDICATOR
            ---------------------------------------------------------------
            BEGIN
                IF v_some_data.SPECIALEDSTATUS IS NULL
                    THEN v_student_record.STUDENT_SPECIAL_ED_INDICATOR := 'No';
                ELSE 
                    v_student_record.STUDENT_SPECIAL_ED_INDICATOR := case when v_some_data.SPECIALEDSTATUS = 'Y' THEN 'Yes' ELSE 'No' END;
/*
                SELECT STUDENT_SPECIAL_ED_INDICATOR
                 INTO v_student_record.STUDENT_SPECIAL_ED_INDICATOR
                 FROM (SELECT CASE
                                 WHEN specialEdStatus = 'Y' THEN 'Yes'
                                 ELSE 'NO'
                              END
                                 AS STUDENT_SPECIAL_ED_INDICATOR
                         FROM K12INTEL_STAGING_IC.ENROLLMENT e
                        WHERE     1 = 1
                              AND STAGE_DELETEFLAG = 0
                              AND SPECIALEDSTATUS = 'Y') last_enr
                           
                WHERE 1=1
                    and rownum = 1;   */
                END IF;


--                    SELECT COALESCE(DOMAIN_ALTERNATE_DECODE,'No')
--                    INTO v_student_record.STUDENT_SPECIAL_ED_INDICATOR
--                    FROM K12INTEL_USERDATA.XTBL_DOMAIN_DECODES
--                    WHERE DOMAIN_NAME = 'STUDENT_SPECIAL_ED_INDICATOR'
--                        and DOMAIN_CODE = v_some_data.SPECIALEDSTATUS
--                        and DOMAIN_SCOPE = p_PARAM_STAGE_SOURCE;
--                END IF;
                
               /*               SELECT b.NAME
                              into v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
                              from k12intel_staging_ic.customstudent a
                              inner join k12intel_Staging_ic.CAMPUSDICTIONARY b
                              on a.value = b.code and a.attributeid = b.attributeid and a.stage_source = b.stage_source
                              inner join k12intel_Staging_ic.CampusAttribute c
                              on a.attributeid = c.attributeid and a.stage_source = c.stage_source
                              where a.personid = v_some_data.personid
                                  and c.object = 'Special Ed Codes'
                                  and c.element = 'Eligibility'
                                  AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE
                                  and a.stage_sis_school_year = v_LOCAL_CURRENT_SCHOOL_YEAR
                                  and a.STAGE_DELETEFLAG = 0
                                  and not exists(
                                          SELECT null
                                          from k12intel_staging_ic.customstudent cs
                                          inner join k12intel_Staging_ic.CampusAttribute ca
                                          on cs.attributeid = ca.attributeid and cs.stage_source = ca.stage_source
                                          where 1=1
                                              and ca.object = 'Special Ed Codes'
                                              and ca.element = 'endDate'
                                              AND a.personID = cs.personid
                                              and a."DATE" = cs."DATE"
                                              and a.STAGE_SOURCE = cs.STAGE_SOURCE
                                              and a.stage_sis_school_year = cs.stage_sis_school_year
                                              and cs.STAGE_DELETEFLAG = 0
                              );

                              v_student_record.STUDENT_SPECIAL_ED_INDICATOR := 'Yes';
              */
        /*       SELECT STUDENT_SPECIAL_ED_INDICATOR,
                      STUDENT_EDUCATIONAL_EXCEPT_TYP,
                      STUDENT_SPECIAL_ED_CLASS
                 INTO v_student_record.STUDENT_SPECIAL_ED_INDICATOR,
                      v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP,
                      v_student_record.STUDENT_SPECIAL_ED_CLASS
                 FROM (SELECT CASE
                                 WHEN specialEdStatus = 'Y' THEN 'Yes'
                                 ELSE 'NO'
                              END
                                 AS STUDENT_SPECIAL_ED_INDICATOR,
                              COALESCE (disability1,
                                        '--')
                                 AS STUDENT_EDUCATIONAL_EXCEPT_TYP,
                              COALESCE (specialEdSetting,
                                        '--')
                                 AS STUDENT_SPECIAL_ED_CLASS,
                              ROW_NUMBER ()
                                 OVER (PARTITION BY PERSONID
                                       ORDER BY STARTDATE)
                                 AS RN
                         FROM K12INTEL_STAGING_IC.ENROLLMENT e
                        WHERE     1 = 1
                              AND personid = v_some_data.personid
                              AND STAGE_DELETEFLAG = 0
                              AND SPECIALEDSTATUS = 'Y') last_enr
                WHERE last_enr.rn = 1; */
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_SPECIAL_ED_INDICATOR := 'No';
                  v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP :=
                     'Not Special Education';
                  v_student_record.STUDENT_SPECIAL_ED_CLASS :=
                     'Not Special Education';
               /*v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;

               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                   v_SYS_ETL_SOURCE,
                   'STUDENT_SPECIAL_ED_INDICATOR',
                   v_WAREHOUSE_KEY,
                   v_AUDIT_NATURAL_KEY,
                   'NO_DATA_FOUND',
                   sqlerrm,
                   'N',
                   v_AUDIT_BASE_SEVERITY
               );*/
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_SPECIAL_ED_INDICATOR := '@ERR';
                  v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := '@ERR';
                  v_student_record.STUDENT_SPECIAL_ED_CLASS := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_SPECIAL_ED_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_SPECIAL_ED_INDICATOR := '@ERR';
                  v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := '@ERR';
                  v_student_record.STUDENT_SPECIAL_ED_CLASS := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_SPECIAL_ED_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            ---------------------------------------------------------------
            -- STUDENT_SPECIAL_ED_CLASS
            ---------------------------------------------------------------
                       BEGIN
                           IF v_student_record.STUDENT_SPECIAL_ED_INDICATOR = 'No' THEN
                               v_student_record.STUDENT_SPECIAL_ED_CLASS := 'Not Special Education';
                           ELSIF v_some_data.SPECIALEDSETTING IS NULL THEN
                               v_student_record.STUDENT_SPECIAL_ED_CLASS := 'Unknown';
                           ELSE
                              select substr(a.code || ': ' || a.name,1,99)
                                 into v_student_record.STUDENT_SPECIAL_ED_CLASS
                               from k12intel_staging_ic.CampusDictionary a
                                 inner join k12intel_staging_ic.CampusAttribute b
                                on a.attributeID = b.attributeID and a.STAGE_SOURCE = b.STAGE_SOURCE
                                where b.object = 'Enrollment'
                                    and b.element = 'specialEdSetting'	
                                    and a.CODE = v_some_data.SPECIALEDSETTING	
                                    and a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
                                        and rownum = 1;


--                                from k12intel_staging_ic.enrollment
--                              where personid = v_some_data.personid
--                              AND STAGE_DELETEFLAG = 0
--                              AND SPECIALEDSTATUS = 'Y';
                           END IF;
                       EXCEPTION
                           WHEN NO_DATA_FOUND THEN
                               v_student_record.SYS_AUDIT_IND := 'Y';
                               v_student_record.STUDENT_SPECIAL_ED_CLASS := '@ERR';
                               v_WAREHOUSE_KEY := 0;
                               v_AUDIT_BASE_SEVERITY := 0;

                               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                   v_SYS_ETL_SOURCE,
                                   'STUDENT_SPECIAL_ED_CLASS',
                                   v_WAREHOUSE_KEY,
                                   v_AUDIT_NATURAL_KEY || ';SPECIALEDSETTING=' || v_some_data.SPECIALEDSETTING,
                                   'NO_DATA_FOUND',
                                   sqlerrm,
                                   'N',
                                   v_AUDIT_BASE_SEVERITY
                               );
                           WHEN TOO_MANY_ROWS THEN
                               v_student_record.SYS_AUDIT_IND := 'Y';
                               v_student_record.STUDENT_SPECIAL_ED_CLASS := '@ERR';
                               v_WAREHOUSE_KEY := 0;
                               v_AUDIT_BASE_SEVERITY := 0;

                               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                   v_SYS_ETL_SOURCE,
                                   'STUDENT_SPECIAL_ED_CLASS',
                                   v_WAREHOUSE_KEY,
                                   v_AUDIT_NATURAL_KEY || ';SPECIALEDSETTING=' || v_some_data.SPECIALEDSETTING,
                                   'TOO_MANY_ROWS',
                                   sqlerrm,
                                   'N',
                                   v_AUDIT_BASE_SEVERITY
                               );
                           WHEN OTHERS THEN
                               v_student_record.SYS_AUDIT_IND := 'Y';
                               v_student_record.STUDENT_SPECIAL_ED_CLASS := '@ERR';
                               v_WAREHOUSE_KEY := 0;
                               v_AUDIT_BASE_SEVERITY := 0;

                               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                   v_SYS_ETL_SOURCE,
                                   'STUDENT_SPECIAL_ED_CLASS',
                                   v_WAREHOUSE_KEY,
                                   v_AUDIT_NATURAL_KEY || ';SPECIALEDSETTING=' || v_some_data.SPECIALEDSETTING,
                                   'Untrapped Error',
                                   sqlerrm,
                                   'Y',
                                   v_AUDIT_BASE_SEVERITY
                               );
                       END;
           
            
                        ---------------------------------------------------------------
                        -- STUDENT_EDUCATIONAL_EXCEPT_TYP
                        ---------------------------------------------------------------
                        BEGIN
                            IF v_student_record.STUDENT_SPECIAL_ED_INDICATOR = 'No' THEN
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := 'Not Applicable';
                            ELSIF v_some_data.DISABILITY1 IS NULL THEN
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := 'Unknown';
                            ELSE
                                select a.NAME
                                    into v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
                                    from k12intel_staging_ic.CampusDictionary a
                                    inner join k12intel_staging_ic.CampusAttribute b
                                    on a.attributeID = b.attributeID and a.STAGE_SOURCE = b.STAGE_SOURCE
                                    where b.object = 'Enrollment'
                                        and b.element = 'disability1'	
                                        and a.CODE = v_some_data.DISABILITY1	 
                                        and a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE;
--                                SELECT NVL(b.value,'Unknown')
--                                into v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
--                                from k12intel_staging_ic.customstudent a
--                                inner join k12intel_Staging_ic.CAMPUSDICTIONARY b
--                                on a.value = b.code and a.attributeid = b.attributeid and a.stage_source = b.stage_source
--                                inner join k12intel_Staging_ic.CampusAttribute c
--                                on a.attributeid = c.attributeid and a.stage_source = c.stage_source
--                                where a.personid = v_some_data.personid
--                                    and c.object = 'Special Ed Codes'
--                                    and c.element = 'Eligibility'
--                                    and a.stage_sis_school_year = v_LOCAL_CURRENT_SCHOOL_YEAR
--                                    and a.stage_source = v_some_data.stage_source;  
                            END IF;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                v_student_record.SYS_AUDIT_IND := 'Y';
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := '@ERR';
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_EDUCATIONAL_EXCEPT_TYP',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY || ';DISABILITY1=' || v_some_data.DISABILITY1,
                                    'NO_DATA_FOUND',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN TOO_MANY_ROWS THEN
                                v_student_record.SYS_AUDIT_IND := 'Y';
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := '@ERR';
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_EDUCATIONAL_EXCEPT_TYP',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY || ';DISABILITY1=' || v_some_data.DISABILITY1,
                                    'TOO_MANY_ROWS',
                                    sqlerrm,
                                    'N',
                                    v_AUDIT_BASE_SEVERITY
                                );
                            WHEN OTHERS THEN
                                v_student_record.SYS_AUDIT_IND := 'Y';
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP := '@ERR';
                                v_WAREHOUSE_KEY := 0;
                                v_AUDIT_BASE_SEVERITY := 0;

                                v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                                    v_SYS_ETL_SOURCE,
                                    'STUDENT_EDUCATIONAL_EXCEPT_TYP',
                                    v_WAREHOUSE_KEY,
                                    v_AUDIT_NATURAL_KEY || ';DISABILITY1=' || v_some_data.DISABILITY1,
                                    'Untrapped Error',
                                    sqlerrm,
                                    'Y',
                                    v_AUDIT_BASE_SEVERITY
                                );
                        END;
            

            ---------------------------------------------------------------
            -- STUDENT_LEP_INDICATOR
            ---------------------------------------------------------------
            BEGIN
               --v_student_record.STUDENT_LEP_INDICATOR :=
               --               CASE
               --                  WHEN v_some_data.englishProficiency IN ('L', 'M', 'Y')
               --                  THEN
               --                     v_student_record.STUDENT_LEP_INDICATOR := 'Yes';
               --                  ELSE
               --                     v_student_record.STUDENT_LEP_INDICATOR := 'No';
               --               END CASE;
               SELECT CASE
                         WHEN programstatus IN ('LEP',
                                                'Exited LEP')
                         THEN
                            'Yes'
                         ELSE
                            'No'
                      END
                 INTO v_student_record.STUDENT_LEP_INDICATOR
                 FROM k12intel_staging_ic.lep
                WHERE personid = v_some_data.personid;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_student_record.STUDENT_LEP_INDICATOR := 'No';
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_LEP_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_LEP_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_LEP_INDICATOR := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_LEP_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;


            ---------------------------------------------------------------
            -- STUDENT_ESL_INDICATOR, STUDENT_ESL_CLASSIFICATION
            ---------------------------------------------------------------
            BEGIN
               SELECT CASE
                         WHEN programstatus = 'LEP' THEN 'Yes'
                         WHEN programstatus = 'Exited LEP' THEN 'Former'
                         ELSE 'No'
                      END,
                      NVL (C.VALUE, 'Not Applicable')
                 INTO v_student_record.STUDENT_ESL_INDICATOR,
                      v_student_record.STUDENT_ESL_CLASSIFICATION
                 FROM k12intel_staging_ic.lep L
                      LEFT JOIN K12INTEL_STAGING_IC.CUSTOMLEP C
                         ON     L.LEPID = C.LEPID
                            AND L.STAGE_SOURCE = C.STAGE_SOURCE
                            AND C.ATTRIBUTEID = 1840
                WHERE personid = v_some_data.personid;
            --               SELECT "NAME"
            --                 INTO v_student_record.STUDENT_ESL_CLASSIFICATION
            --                 FROM (SELECT b."NAME",
            --                              RANK ()
            --                              OVER (
            --                                 PARTITION BY a.PERSONID, a.STAGE_SOURCE
            --                                 ORDER BY
            --                                    COALESCE (
            --                                       a.ENDDATE,
            --                                       TO_DATE ('12/31/9999', 'mm/dd/yyyy')) DESC,
            --                                    a.STARTDATE DESC,
            --                                    a.PARTICIPATIONID DESC)
            --                                 r
            --                         FROM k12intel_staging_ic.PROGRAMPARTICIPATION a
            --                              INNER JOIN k12intel_Staging_ic.PROGRAM b
            --                                 ON     a.programid = b.programid
            --                                    AND a.stage_source = b.stage_source
            --                        WHERE     personid = v_some_data.personid
            --                              AND a.programid IN (315,
            --                                                  316,
            --                                                  317,
            --                                                  318,
            --                                                  290)
            --                              AND v_last_activity_date BETWEEN a.STARTDATE
            --                                                           AND COALESCE (
            --                                                                  a.ENDDATE,
            --                                                                  TO_DATE (
            --                                                                     '12/31/9999',
            --                                                                     'mm/dd/yyyy'))
            --                              AND a.stage_source = v_some_data.stage_source)
            --                      x
            --                WHERE x.r = 1;
            --
            --               v_student_record.STUDENT_ESL_INDICATOR := 'Yes';
            /*
            IF v_student_record.STUDENT_ESL_INDICATOR = 'Yes' OR v_student_record.STUDENT_LEP_INDICATOR = 'Yes' THEN
                select x.VALUE
                into v_student_record.STUDENT_ESL_CLASSIFICATION
                FROM
                (
                    select a.VALUE
                    from k12intel_staging_ic.customstudent a
                    where a.attributeid = 184
                        and a.PERSONID = v_some_data.PERSONID
                        and a.value in ('E','P','M','2')
                        and stage_sis_school_year = v_LOCAL_CURRENT_SCHOOL_YEAR
                        and a.STAGE_SOURCE = v_some_data.STAGE_SOURCE
                        and exists(select null from k12intel_staging_ic.customstudent b where a.personid = b.personid and b.attributeid = 185 and a.STAGE_SIS_SCHOOL_YEAR = b.STAGE_SIS_SCHOOL_YEAR and a.stage_source = b.stage_source and b."DATE" < v_last_activity_date)
                        and not exists(select null from k12intel_staging_ic.customstudent b where a.personid = b.personid and b.attributeid = 349 and a.STAGE_SIS_SCHOOL_YEAR = b.STAGE_SIS_SCHOOL_YEAR and a.stage_source = b.stage_source and b."DATE" < v_last_activity_date)
                    order by "DATE" DESC, CUSTOMID DESC
                ) x
                where rownum < 2;

                select a."NAME" INTO v_student_record.STUDENT_ESL_CLASSIFICATION
                from k12intel_staging_ic.CampusDictionary a
                inner join k12intel_staging_ic.CampusAttribute b
                on a.attributeID = b.attributeID and a.STAGE_SOURCE = b.STAGE_SOURCE
                where b.object = 'ELL'
                    and b.element = 'ESL Status'
                    and a.CODE = v_student_record.STUDENT_ESL_CLASSIFICATION
                    and a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
            else
                v_student_record.STUDENT_ESL_CLASSIFICATION := 'NOT ESL';
            end if;*/
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ESL_INDICATOR := 'No';
                  v_student_record.STUDENT_ESL_CLASSIFICATION :=
                     'Not Applicable';
               /*v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;

               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                   v_SYS_ETL_SOURCE,
                   'STUDENT_ESL_CLASSIFICATION',
                   v_WAREHOUSE_KEY,
                   v_AUDIT_NATURAL_KEY,
                   'NO_DATA_FOUND',
                   sqlerrm,
                   'N',
                   v_AUDIT_BASE_SEVERITY
               );*/
               WHEN TOO_MANY_ROWS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ESL_INDICATOR := '@ERR';
                  v_student_record.STUDENT_ESL_CLASSIFICATION := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ESL_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'TOO_MANY_ROWS',
                     SQLERRM,
                     'N',
                     v_AUDIT_BASE_SEVERITY);
               WHEN OTHERS
               THEN
                  v_student_record.SYS_AUDIT_IND := 'Y';
                  v_student_record.STUDENT_ESL_INDICATOR := '@ERR';
                  v_student_record.STUDENT_ESL_CLASSIFICATION := '@ERR';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                     v_SYS_ETL_SOURCE,
                     'STUDENT_ESL_INDICATOR',
                     v_WAREHOUSE_KEY,
                     v_AUDIT_NATURAL_KEY,
                     'Untrapped Error',
                     SQLERRM,
                     'Y',
                     v_AUDIT_BASE_SEVERITY);
            END;

            /*
                        ---------------------------------------------------------------
                        -- STUDENT_ESL_CLASSIFICATION
                        ---------------------------------------------------------------
                        BEGIN
                            IF v_student_record.STUDENT_ESL_INDICATOR = 'No' THEN
                                v_student_record.STUDENT_ESL_CLASSIFICATION := 'Not Applicable';
                            ELSE
                                SELECT C.VALUE
                                INTO v_student_record.STUDENT_ESL_CLASSIFICATION
                                FROM K12INTEL_STAGING_IC.LEP L
                                    JOIN K12INTEL_STAGING_IC.CUSTOMLEP C ON L.LEPID = C.LEPID AND L.STAGE_SOURCE = C.STAGE_SOURCE AND C.ATTRIBUTEID = 1840
                                WHERE PERSONID = v_some_data.personid;
                            END IF;

                            EXCEPTION
                               WHEN NO_DATA_FOUND
                               THEN
                                  v_student_record.STUDENT_ESL_CLASSIFICATION := '@ERR';
                               WHEN TOO_MANY_ROWS
                               THEN
                                  v_student_record.SYS_AUDIT_IND := 'Y';
                                  v_student_record.STUDENT_ESL_CLASSIFICATION := '@ERR';
                                  v_WAREHOUSE_KEY := 0;
                                  v_AUDIT_BASE_SEVERITY := 0;

                                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                                     v_SYS_ETL_SOURCE,
                                     'STUDENT_ESL_CLASSIFICATION',
                                     v_WAREHOUSE_KEY,
                                     v_AUDIT_NATURAL_KEY,
                                     'TOO_MANY_ROWS',
                                     SQLERRM,
                                     'N',
                                     v_AUDIT_BASE_SEVERITY);
                               WHEN OTHERS
                               THEN
                                  v_student_record.SYS_AUDIT_IND := 'Y';
                                  v_student_record.STUDENT_ESL_CLASSIFICATION := '@ERR';
                                  v_WAREHOUSE_KEY := 0;
                                  v_AUDIT_BASE_SEVERITY := 0;

                                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                                     v_SYS_ETL_SOURCE,
                                     'STUDENT_ESL_CLASSIFICATION',
                                     v_WAREHOUSE_KEY,
                                     v_AUDIT_NATURAL_KEY,
                                     'Untrapped Error',
                                     SQLERRM,
                                     'Y',
                                     v_AUDIT_BASE_SEVERITY);
                        END;
            */


            ---------------------------------------------------------------
            -- STUDENT_GIFTED_INDICATOR
            -- STUDENT_GIFTED_YEAR
            ---------------------------------------------------------------
            BEGIN
               --            SELECT CASE WHEN COUNT (*) > 0 THEN 'Yes' ELSE 'No' END
               --              INTO v_student_record.STUDENT_GIFTED_INDICATOR
               --              FROM k12intel_staging_ic.customstudent
               --             WHERE     personid = v_some_data.personid
               --                   AND attributeid IN (230,
               --                                       231,
               --                                       238,
               --                                       240,
               --                                       241,
               --                                       242,
               --                                       243)
               --                   AND VALUE = '1'
               --                   AND stage_sis_school_year = v_LOCAL_CURRENT_SCHOOL_YEAR
               --                   AND stage_source = v_some_data.stage_source
               --                   AND STAGE_DELETEFLAG = 0;
               IF v_some_data.giftedTalented = 'Y'
               THEN
                  BEGIN
                     v_student_record.STUDENT_GIFTED_INDICATOR := 'Yes';
                     v_details_record.student_gifted_year :=
                        TO_CHAR (v_some_data.startDate,
                                 'YYYY');
                  END;
               ELSE
                  v_student_record.STUDENT_GIFTED_INDICATOR := 'No';
                  v_details_record.student_gifted_year := NULL;
               END IF;

               --            EXCEPTION
               --            WHEN NO_DATA_FOUND
               --            THEN
               --               v_student_record.SYS_AUDIT_IND := 'Y';
               --               v_student_record.STUDENT_GIFTED_INDICATOR := '@ERR';
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
               --                  v_SYS_ETL_SOURCE,
               --                  'STUDENT_GIFTED_INDICATOR',
               --                  v_WAREHOUSE_KEY,
               --                  v_AUDIT_NATURAL_KEY,
               --                  'NO_DATA_FOUND',
               --                  SQLERRM,
               --                  'N',
               --                  v_AUDIT_BASE_SEVERITY);
               --            WHEN TOO_MANY_ROWS
               --            THEN
               --               v_student_record.SYS_AUDIT_IND := 'Y';
               --               v_student_record.STUDENT_GIFTED_INDICATOR := '@ERR';
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
               --                  v_SYS_ETL_SOURCE,
               --                  'STUDENT_GIFTED_INDICATOR',
               --                  v_WAREHOUSE_KEY,
               --                  v_AUDIT_NATURAL_KEY,
               --                  'TOO_MANY_ROWS',
               --                  SQLERRM,
               --                  'N',
               --                  v_AUDIT_BASE_SEVERITY);
               --            WHEN OTHERS
               --            THEN
               --               v_student_record.SYS_AUDIT_IND := 'Y';
               --               v_student_record.STUDENT_GIFTED_INDICATOR := '@ERR';
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
               --                  v_SYS_ETL_SOURCE,
               --                  'STUDENT_GIFTED_INDICATOR',
               --                  v_WAREHOUSE_KEY,
               --                  v_AUDIT_NATURAL_KEY,
               --                  'Untrapped Error',
               --                  SQLERRM,
               --                  'Y',
               --                  v_AUDIT_BASE_SEVERITY);
               --         END;


               ---------------------------------------------------------------
               -- STUDENT_CUMULATIVE_GPA, STUDENT_CURRENT_GPA, WEIGHTED_CUMULATIVE_GPA, WEIGHTED_CURRENT_GPA
               ---------------------------------------------------------------
               BEGIN
                  SELECT CUMULATIVE_GPA,
                         CURRENT_GPA,
                         WEIGHTED_CUMULATIVE_GPA,
                         CURRENT_WEIGHTED_GPA
                    INTO v_student_record.STUDENT_CUMULATIVE_GPA,
                         v_student_record.STUDENT_CURRENT_GPA,
                         v_student_record.WEIGHTED_CUMULATIVE_GPA,
                         v_student_record.WEIGHTED_CURRENT_GPA
                    FROM K12INTEL_STAGING_IC.TEMP_GPA
                   WHERE     PERSONID = v_some_data.PERSONID
                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.STUDENT_CUMULATIVE_GPA := NULL;
                     v_student_record.STUDENT_CURRENT_GPA := NULL;
                     v_student_record.WEIGHTED_CUMULATIVE_GPA := NULL;
                     v_student_record.WEIGHTED_CURRENT_GPA := NULL;
                  /*v_student_record.SYS_AUDIT_IND := 'Y';
                  v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                      v_SYS_ETL_SOURCE,
                      'STUDENT_CUMULATIVE_GPA',
                      v_WAREHOUSE_KEY,
                      v_AUDIT_NATURAL_KEY,
                      'NO_DATA_FOUND',
                      sqlerrm,
                      'N',
                      v_AUDIT_BASE_SEVERITY
                  );*/
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CUMULATIVE_GPA := 0;
                     v_student_record.STUDENT_CURRENT_GPA := 0;
                     v_student_record.WEIGHTED_CUMULATIVE_GPA := 0;
                     v_student_record.WEIGHTED_CURRENT_GPA := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CUMULATIVE_GPA',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CUMULATIVE_GPA := 0;
                     v_student_record.STUDENT_CURRENT_GPA := 0;
                     v_student_record.WEIGHTED_CUMULATIVE_GPA := 0;
                     v_student_record.WEIGHTED_CURRENT_GPA := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CUMULATIVE_GPA',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_ADMISSION_TYPE
               ---------------------------------------------------------------
               BEGIN
                  SELECT SUBSTR (a.NAME,
                                 1,
                                 30)
                    INTO v_student_record.STUDENT_ADMISSION_TYPE
                    FROM k12intel_staging_ic.CampusDictionary a
                         INNER JOIN k12intel_staging_ic.CampusAttribute b
                            ON     a.attributeID = b.attributeID
                               AND a.STAGE_SOURCE = b.STAGE_SOURCE
                   WHERE     b.object = 'Enrollment'
                         AND b.element = 'startStatus'
                         AND a.CODE = v_some_data.STARTSTATUS
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_ADMISSION_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || 'STARTSTATUS='
                        || v_some_data.STARTSTATUS;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ADMISSION_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_ADMISSION_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || 'STARTSTATUS='
                        || v_some_data.STARTSTATUS;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ADMISSION_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_ADMISSION_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ADMISSION_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_LIVING_WITH
               ---------------------------------------------------------------
               BEGIN
                  DECLARE
                     v_Mother               NUMBER (1);
                     v_Father               NUMBER (1);
                     v_Stepfather           NUMBER (1);
                     v_Stepmother           NUMBER (1);
                     v_Guardian             NUMBER (1);
                     v_Grandmother          NUMBER (1);
                     v_Grandfather          NUMBER (1);
                     v_FosterMother         NUMBER (1);
                     v_FosterFather         NUMBER (1);
                     v_Aunt                 NUMBER (1);
                     v_Uncle                NUMBER (1);
                     v_living_with_buffer   VARCHAR2 (500);
                  BEGIN
                     v_rowcnt := 0;
                     v_living_with_buffer := NULL;

                     FOR v_parent_data
                        IN (SELECT DISTINCT a.NAME
                              FROM k12intel_staging_ic.RELATEDPAIR a
                                   INNER JOIN k12intel_staging_ic.PERSON p
                                      ON     a.PERSONID1 = p.PERSONID
                                         AND a.STAGE_SOURCE = p.STAGE_SOURCE
                             WHERE     p.STUDENTNUMBER IS NOT NULL
                                   AND a.GUARDIAN = 1
                                   AND a.personid1 = v_some_data.personid
                                   AND a.NAME IS NOT NULL
                                   AND a.stage_source =
                                          v_some_data.stage_source)
                     LOOP
                        v_rowcnt := v_rowcnt + 1;
                        v_living_with_buffer :=
                           CASE
                              WHEN v_living_with_buffer IS NULL
                              THEN
                                 v_parent_data.name
                              ELSE
                                    v_living_with_buffer
                                 || ','
                                 || v_parent_data.name
                           END;

                        IF v_parent_data.name = 'Mother'
                        THEN
                           v_Mother := 1;
                        ELSIF v_parent_data.name = 'Father'
                        THEN
                           v_Father := 1;
                        ELSIF v_parent_data.name = 'Stepfather'
                        THEN
                           v_Stepfather := 1;
                        ELSIF v_parent_data.name = 'Stepmother'
                        THEN
                           v_Stepmother := 1;
                        ELSIF v_parent_data.name = 'Guardian'
                        THEN
                           v_Guardian := 1;
                        ELSIF v_parent_data.name = 'Grandmother'
                        THEN
                           v_Grandmother := 1;
                        ELSIF v_parent_data.name = 'Grandfather'
                        THEN
                           v_Grandfather := 1;
                        ELSIF v_parent_data.name = 'Foster mother'
                        THEN
                           v_FosterMother := 1;
                        ELSIF v_parent_data.name = 'Foster father'
                        THEN
                           v_FosterFather := 1;
                        ELSIF v_parent_data.name = 'Aunt'
                        THEN
                           v_Aunt := 1;
                        ELSIF v_parent_data.name = 'Uncle'
                        THEN
                           v_Uncle := 1;
                        END IF;
                     END LOOP;

                     IF v_rowcnt = 0
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := '--';
                     ELSIF v_rowcnt = 1
                     THEN
                        --only 1 record, grab from buffer
                        v_student_record.STUDENT_LIVING_WITH :=
                           v_living_with_buffer;
                     ELSIF v_Mother = 1 AND v_Father = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Both Parents';
                     ELSIF v_Mother = 1 AND v_Stepfather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Both Parents';
                     ELSIF v_Father = 1 AND v_Stepmother = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Both Parents';
                     ELSIF v_Grandmother = 1 AND v_Grandfather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Grandparents';
                     ELSIF v_Mother = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Mother';
                     ELSIF v_Father = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Father';
                     ELSIF v_Stepmother = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Stepmother';
                     ELSIF v_Stepfather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Stepfather';
                     ELSIF v_Grandfather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Grandfather';
                     ELSIF v_Grandmother = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Grandmother';
                     ELSIF v_FosterMother = 1 AND v_FosterFather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH :=
                           'Foster Parents';
                     ELSIF v_FosterMother = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH :=
                           'Foster Mother';
                     ELSIF v_FosterFather = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH :=
                           'Foster Father';
                     ELSIF v_Aunt = 1 AND v_Uncle = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH :=
                           'Aunt and Uncle';
                     ELSIF v_Aunt = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Aunt';
                     ELSIF v_Uncle = 1
                     THEN
                        v_student_record.STUDENT_LIVING_WITH := 'Uncle';
                     ELSE
                        v_student_record.SYS_AUDIT_IND := 'Y';
                        v_student_record.STUDENT_LIVING_WITH := '@ERR';
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY :=
                              v_BASE_NATURALKEY_TXT
                           || ';STUDENT_LIVING_WITH_BUFFER='
                           || v_living_with_buffer;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                           v_SYS_ETL_SOURCE,
                           'STUDENT_LIVING_WITH',
                           v_WAREHOUSE_KEY,
                           v_AUDIT_NATURAL_KEY,
                           'UNABLE TO DETERMINE STUDENT_LIVING_WITH',
                           NULL,
                           'N',
                           v_AUDIT_BASE_SEVERITY);
                     END IF;
                  END;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_LIVING_WITH := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LIVING_WITH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_LIVING_WITH := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LIVING_WITH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_LIVING_WITH := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LIVING_WITH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_RESIDENCE_CHANGES
               ---------------------------------------------------------------
               BEGIN
                  SELECT NVL (CHANGE_COUNT, 0)
                    INTO v_student_record.STUDENT_RESIDENCE_CHANGES
                    FROM K12INTEL_STAGING_IC.TEMP_CHANGE_COUNT a
                   WHERE     a.PERSONID = v_some_data.PERSONID
                         AND a.CHANGE_TYPE = 'R'
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.STUDENT_RESIDENCE_CHANGES := 0;
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_RESIDENCE_CHANGES := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RESIDENCE_CHANGES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_RESIDENCE_CHANGES := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_RESIDENCE_CHANGES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_SCHOOL_CHANGES
               ---------------------------------------------------------------
               BEGIN
                  SELECT NVL (CHANGE_COUNT, 0)
                    INTO v_student_record.STUDENT_SCHOOL_CHANGES
                    FROM K12INTEL_STAGING_IC.TEMP_CHANGE_COUNT a
                   WHERE     a.PERSONID = v_some_data.PERSONID
                         AND a.CHANGE_TYPE = 'S'
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.STUDENT_SCHOOL_CHANGES := 0;
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_SCHOOL_CHANGES := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_SCHOOL_CHANGES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_SCHOOL_CHANGES := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_SCHOOL_CHANGES',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_CROSS_ENROLL_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  SELECT CASE
                            WHEN COUNT(*) > 1 THEN 'Yes'
                            ELSE 'No'
                         END
                      INTO v_student_record.STUDENT_CROSS_ENROLL_INDICATOR
                      FROM K12INTEL_DW.FTBL_ENROLLMENTS
                      WHERE STUDENT_KEY = v_student_record.STUDENT_KEY
                         AND CURRENT_ENROLLMENT_INDICATOR = 'Yes';

--                  SELECT CASE
--                            WHEN COUNT (*) > 0 THEN 'Yes'
--                            ELSE 'No'
--                         END
--                    INTO v_student_record.STUDENT_CROSS_ENROLL_INDICATOR
--                    FROM K12INTEL_STAGING_IC.ENROLLMENT
--                   WHERE     PERSONID = v_some_data.PERSONID
--                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE
--                         AND v_last_activity_date BETWEEN STARTDATE
--                                                      AND NVL (
--                                                             ENDDATE,
--                                                             TO_DATE (
--                                                                '12/31/9999',
--                                                                'mm/dd/yyyy'))
--                         AND ENROLLMENTID <> v_some_data.ENROLLMENTID;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CROSS_ENROLL_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CROSS_ENROLL_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CROSS_ENROLL_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CROSS_ENROLL_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CROSS_ENROLL_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CROSS_ENROLL_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_CURRENT_GRADE_CODE
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_CURRENT_GRADE_CODE :=
                     NVL (v_some_data.grade, '@ERR');
               --NVL (v_LOCAL_GRADE, '@ERR');  S.Schnelz 01-29-2015
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_CURRENT_GRADE_LEVEL
               ---------------------------------------------------------------
               BEGIN
                  IF v_student_record.STUDENT_CURRENT_GRADE_CODE = '@ERR'
                  THEN
                     v_student_record.STUDENT_CURRENT_GRADE_LEVEL := '@ERR';
                  ELSE
                     SELECT a.NAME
                       INTO v_student_record.STUDENT_CURRENT_GRADE_LEVEL
                       FROM k12intel_staging_ic.CampusDictionary a
                            INNER JOIN k12intel_staging_ic.CampusAttribute b
                               ON     a.attributeID = b.attributeID
                                  AND a.STAGE_SOURCE = b.STAGE_SOURCE
                      WHERE     b.object = 'GradeLevel-dep20101'
                            AND b.element = 'stateGrade'
                            AND a.CODE =
                                   v_student_record.STUDENT_CURRENT_GRADE_CODE
                            AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_CURRENT_DISTRICT_CODE
               ---------------------------------------------------------------
               BEGIN
                  IF v_LOCAL_DISTRICTID IS NOT NULL
                  THEN
                     BEGIN
                        SELECT "NUMBER"
                          INTO v_student_record.STUDENT_CURRENT_DISTRICT_CODE
                          FROM K12INTEL_STAGING_IC.DISTRICT
                         WHERE     DISTRICTID = v_LOCAL_DISTRICTID
                               AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           v_student_record.SYS_AUDIT_IND := 'Y';
                           v_student_record.STUDENT_CURRENT_DISTRICT_CODE :=
                              '@ERR';
                           v_WAREHOUSE_KEY := 0;
                           v_AUDIT_BASE_SEVERITY := 0;

                           v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                           K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                              v_SYS_ETL_SOURCE,
                                 'STUDENT_CURRENT_DISTRICT_CODE'
                              || v_some_data.DISTRICTID,
                              v_WAREHOUSE_KEY,
                              v_AUDIT_NATURAL_KEY,
                              'NO_DATA_FOUND',
                              SQLERRM,
                              'N',
                              v_AUDIT_BASE_SEVERITY);
                        WHEN TOO_MANY_ROWS
                        THEN
                           v_student_record.SYS_AUDIT_IND := 'Y';
                           v_student_record.STUDENT_CURRENT_DISTRICT_CODE :=
                              '@ERR';
                           v_WAREHOUSE_KEY := 0;
                           v_AUDIT_BASE_SEVERITY := 0;

                           v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                           K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                              v_SYS_ETL_SOURCE,
                              'STUDENT_CURRENT_DISTRICT_CODE',
                              v_WAREHOUSE_KEY,
                              v_AUDIT_NATURAL_KEY,
                              'TOO_MANY_ROWS',
                              SQLERRM,
                              'N',
                              v_AUDIT_BASE_SEVERITY);
                     END;
                  ELSE
                     v_student_record.STUDENT_CURRENT_DISTRICT_CODE :=
                        v_DEFAULT_DISTRICT_CODE;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_CURRENT_HOMEROOM
               ---------------------------------------------------------------
               BEGIN
                  SELECT NVL (ROOM_NAME, '--')
                    INTO v_student_record.STUDENT_CURRENT_HOMEROOM
                    FROM K12INTEL_STAGING_IC.TEMP_HOMEROOMS a
                   WHERE     a.PERSONID = v_some_data.PERSONID
                         AND ROWNUM = 1
                         AND a.CALENDARID = v_some_data.calendarid --v_LOCAL_CALENDARID S.Schnelz 01-29-2015
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     --v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_HOMEROOM := '--';
                  /*v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                      v_SYS_ETL_SOURCE,
                      'STUDENT_CURRENT_HOMEROOM'
                      v_WAREHOUSE_KEY,
                      v_AUDIT_NATURAL_KEY,
                      'NO_DATA_FOUND',
                      sqlerrm,
                      'N',
                      v_AUDIT_BASE_SEVERITY
                  );*/
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_HOMEROOM := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || ';CALENDARID='
                        || TO_CHAR (v_some_data.CALENDARID);

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_HOMEROOM',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CURRENT_HOMEROOM := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CURRENT_HOMEROOM',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_NEXT_YEAR_GRADE_CODE
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE :=
                     v_LOCAL_GRADE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_NEXT_YEAR_GRADE_LEVEL
               ---------------------------------------------------------------
               BEGIN
                  IF v_some_data.NEXTGRADE IS NULL
                  THEN
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL := '--';
                  ELSE
                     SELECT a.NAME
                       INTO v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL
                       FROM k12intel_staging_ic.CampusDictionary a
                            INNER JOIN k12intel_staging_ic.CampusAttribute b
                               ON     a.attributeID = b.attributeID
                                  AND a.STAGE_SOURCE = b.STAGE_SOURCE
                      WHERE     b.object = 'GradeLevel-dep20101'
                            AND b.element = 'stateGrade'
                            AND a.CODE =
                                   v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE
                            AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_GRADE_LEVEL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_NEXT_YEAR_SCHOOL_CODE
               ---------------------------------------------------------------
               DECLARE
                  v_temp_date   DATE := NULL;
               BEGIN
                  /*
                                      SELECT COALESCE (ssp.fall_grade, '--'),
                                             COALESCE (ssp.fall_grade, '--'),
                                             COALESCE (TO_CHAR (ssp.fall_school), '--'),
                                             COALESCE (ds.school_name, '--'),
                                             MAX (SSP.FALL_EFFECTIVE_DATE)
                                        INTO v_student_record.student_next_year_grade_code,
                                             v_student_record.student_next_year_grade_level,
                                             v_student_record.student_next_year_school_code,
                                             v_student_record.student_next_year_school,
                                             v_temp_date
                                        FROM k12intel_staging.mps_sap_student_profile ssp, k12intel_dw.dtbl_schools ds
                                       WHERE     TO_CHAR (ssp.fall_school) = ds.school_code
                                             AND ssp.pupil_number = v_some_data.studentnumber
                                    GROUP BY ssp.fall_grade,
                                             TO_CHAR (ssp.fall_school),
                                             ds.school_name;
                  */
                  SELECT a.VALUE
                    INTO v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE
                    FROM k12intel_staging_ic.customschool a
                         INNER JOIN k12intel_staging_ic.CAMPUSATTRIBUTE b
                            ON     a.ATTRIBUTEID = b.ATTRIBUTEID
                               AND a.stage_source = b.stage_source
                   WHERE     a.SCHOOLID = v_LOCAL_SCHOOLID
                         AND b.object = 'School'
                         AND b.ELEMENT = 'LOCALSCHOOLNUM'
                         AND a.STAGE_SIS_SCHOOL_YEAR =
                                v_LOCAL_CURRENT_SCHOOL_YEAR
                         AND a.stage_deleteflag = 0
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- Replace IC Next Year School Code with "Standard School Code"
               ---------------------------------------------------------------
               v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE :=
                  K12INTEL_METADATA.REPLACE_IC_SCHOOL_NUMBER (
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE);

               ---------------------------------------------------------------
               -- STUDENT_NEXT_YEAR_SCHOOL
               ---------------------------------------------------------------
               BEGIN
                  SELECT SCHOOL_NAME
                    INTO v_student_record.STUDENT_NEXT_YEAR_SCHOOL
                    FROM K12INTEL_DW.DTBL_SCHOOLS
                   WHERE SCHOOL_CODE =
                            v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;
                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;
                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_NEXT_YEAR_HOMEROOM
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_NEXT_YEAR_HOMEROOM := '--';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_HOMEROOM := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_HOMEROOM',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_HOMEROOM := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_HOMEROOM',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_NEXT_YEAR_HOMEROOM := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEXT_YEAR_HOMEROOM',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_GRADUATION_COHORT
               ---------------------------------------------------------------
               BEGIN
                  SELECT NVL (TO_NUMBER (COHORTYEARNGA), 0),
                         DIPLOMATYPE,
                         DIPLOMADATE
                    INTO v_student_record.STUDENT_GRADUATION_COHORT,
                         v_DIPLOMATYPE,
                         v_DIPLOMADATE
                    FROM K12INTEL_STAGING_IC.GRADUATION
                   WHERE     PERSONID = v_some_data.PERSONID
                         AND DISTRICTID = v_LOCAL_DISTRICTID
                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     --v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_GRADUATION_COHORT := 0;
                     v_DIPLOMATYPE := NULL;
                     v_DIPLOMADATE := NULL;
                  /*v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                      v_SYS_ETL_SOURCE,
                      'STUDENT_GRADUATION_COHORT',
                      v_WAREHOUSE_KEY,
                      v_AUDIT_NATURAL_KEY,
                      'NO_DATA_FOUND',
                      sqlerrm,
                      'N',
                      v_AUDIT_BASE_SEVERITY
                  );*/
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_GRADUATION_COHORT := 0;
                     v_DIPLOMATYPE := NULL;
                     v_DIPLOMADATE := NULL;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_GRADUATION_COHORT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_GRADUATION_COHORT := 0;
                     v_DIPLOMATYPE := NULL;
                     v_DIPLOMADATE := NULL;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_GRADUATION_COHORT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_1ST_GRADE_COHORT
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_1ST_GRADE_COHORT := 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_1ST_GRADE_COHORT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_1ST_GRADE_COHORT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_1ST_GRADE_COHORT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_1ST_GRADE_COHORT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_1ST_GRADE_COHORT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_1ST_GRADE_COHORT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_HOMELESS_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  CASE
                     WHEN v_some_data.HOMELESS IN ('A',
                                                   'B',
                                                   'C',
                                                   'I')
                     THEN
                        v_student_record.STUDENT_HOMELESS_INDICATOR := 'Yes';
                     ELSE
                        v_student_record.STUDENT_HOMELESS_INDICATOR := 'No';
                  END CASE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_HOMELESS_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_HOMELESS_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_HOMELESS_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_HOMELESS_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_HOMELESS_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_HOMELESS_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_AT_RISK_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_AT_RISK_INDICATOR := '--';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_AT_RISK_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_AT_RISK_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_AT_RISK_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_AT_RISK_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_AT_RISK_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_AT_RISK_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_504_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  --  v_student_record.STUDENT_504_INDICATOR :=
                  CASE
                     WHEN UPPER (v_some_data.SECTION504) IN ('Y',
                                                             '1')
                     THEN
                        v_student_record.STUDENT_504_INDICATOR := 'Yes';
                     ELSE
                        v_student_record.STUDENT_504_INDICATOR := 'No';
                  END CASE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_504_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_504_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_504_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_504_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_504_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_504_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_INDIAN_ED_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.STUDENT_INDIAN_ED_INDICATOR := '--';

                  SELECT 'Yes'
                    INTO v_student_record.STUDENT_INDIAN_ED_INDICATOR
                    FROM k12intel_staging_ic.program p
                         JOIN k12intel_staging_ic.programParticipation pp
                            ON pp.programID = p.programID
                   WHERE     p.name = 'AC-FIRST NATIONS STUDIES'
                         AND PERSONID = v_some_data.PERSONID
                         AND ROWNUM = 1
                         AND pp.startDate <= v_local_data_date
                         AND NVL (pp.endDate,
                                  TO_DATE ('12312999',
                                           'MMDDYYYY')) > v_local_data_date;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.STUDENT_INDIAN_ED_INDICATOR := 'No';
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_INDIAN_ED_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_INDIAN_ED_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_INDIAN_ED_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_INDIAN_ED_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_MIGRANT_ED_INDICATOR
               ---------------------------------------------------------------
               BEGIN
                  CASE
                     WHEN UPPER (v_some_data.MIGRANT) IN ('Y',
                                                          '1')
                     THEN
                        v_student_record.STUDENT_MIGRANT_ED_INDICATOR := 'Yes';
                     ELSE
                        v_student_record.STUDENT_MIGRANT_ED_INDICATOR := 'No';
                  END CASE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_MIGRANT_ED_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIGRANT_ED_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_MIGRANT_ED_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIGRANT_ED_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_MIGRANT_ED_INDICATOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIGRANT_ED_INDICATOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- DISTRICT_CODE
               ---------------------------------------------------------------
               BEGIN
                  v_student_record.DISTRICT_CODE :=
                     v_student_record.STUDENT_CURRENT_DISTRICT_CODE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_STATE_ID
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_STATE_ID :=
                     NVL (trim(v_some_data.STATEID), '@ERR');
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_STATE_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_STATE_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_STATE_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_STATE_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_STATE_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_STATE_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_FEDERAL_ID
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_FEDERAL_ID := NULL;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FEDERAL_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FEDERAL_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FEDERAL_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FEDERAL_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FEDERAL_ID := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FEDERAL_ID',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_LAST_NAME
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_LAST_NAME := v_some_data.LASTNAME;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_LAST_NAME_SUFFIX
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_LAST_NAME_SUFFIX :=
                     v_some_data.SUFFIX;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME_SUFFIX := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME_SUFFIX',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME_SUFFIX := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME_SUFFIX',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_LAST_NAME_SUFFIX := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_LAST_NAME_SUFFIX',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_FIRST_NAME
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_FIRST_NAME := v_some_data.FIRSTNAME;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_MIDDLE_INITIAL
               ---------------------------------------------------------------
               BEGIN
                  CASE
                     WHEN v_some_data.MIDDLENAME IS NULL
                     THEN
                        v_details_record.STUDENT_MIDDLE_INITIAL := NULL;
                     ELSE
                        v_details_record.STUDENT_MIDDLE_INITIAL :=
                           SUBSTR (v_some_data.MIDDLENAME,
                                   1,
                                   1);
                  END CASE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_INITIAL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_INITIAL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_INITIAL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_INITIAL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_INITIAL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_INITIAL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_MIDDLE_NAME
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_MIDDLE_NAME :=
                     v_some_data.MIDDLENAME;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_MIDDLE_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_MIDDLE_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_PREFERRED_LAST_NAME
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_PREFERRED_LAST_NAME :=
                     v_some_data.LASTNAME;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_LAST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_LAST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_PREFERRED_FIRST_NAME
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_PREFERRED_FIRST_NAME :=
                     v_some_data."ALIAS";
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_PREFERRED_FIRST_NAME := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_PREFERRED_FIRST_NAME',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_BIRTHDATE
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_BIRTHDATE := v_some_data.BIRTHDATE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTHDATE :=
                        TO_DATE ('01/01/1900',
                                 'mm/dd/yyyy');
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTHDATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTHDATE :=
                        TO_DATE ('01/01/1900',
                                 'mm/dd/yyyy');
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTHDATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTHDATE :=
                        TO_DATE ('01/01/1900',
                                 'mm/dd/yyyy');
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTHDATE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_BIRTH_YEAR
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_BIRTH_YEAR :=
                     NVL (TO_NUMBER (TO_CHAR (v_some_data.BIRTHDATE,
                                              'YYYY')),
                          0);
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_YEAR := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_YEAR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_YEAR := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_YEAR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_YEAR := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_YEAR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_BIRTH_MONTH
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_BIRTH_MONTH :=
                     NVL (TO_NUMBER (TO_CHAR (v_some_data.BIRTHDATE,
                                              'mm')),
                          0);
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_MONTH := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_MONTH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_MONTH := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_MONTH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_BIRTH_MONTH := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_BIRTH_MONTH',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_ORIGIN_IND_WHITE
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.student_origin_ind_asian := 'No';
                  v_details_record.student_origin_ind_black := 'No';
                  v_details_record.student_origin_ind_hispanic := 'No';
                  v_details_record.student_origin_ind_native_amer := 'No';
                  v_details_record.student_origin_ind_pacif_islnd := 'No';
                  v_details_record.student_origin_ind_white := 'No';

                  -- S.Schnelz 05-15-2015 Commented out the following and inserted source from
                  -- identityraceethnicity. These values come from the cursor now.

                  --                  IF v_some_data.HISPANICETHNICITY = 'Y'
                  --                  THEN
                  --                     v_details_record.student_origin_ind_hispanic := 'Yes';
                  --                  END IF;
                  --
                  --                  BEGIN
                  --                     IF v_some_data.RACEETHNICITY IS NOT NULL
                  --                     THEN
                  --                        SELECT DOMAIN_DECODE
                  --                          INTO v_ETHNIC_DECODE
                  --                          FROM k12intel_userdata.xtbl_domain_decodes
                  --                         WHERE     domain_name = 'STUDENT_ORIGIN_IND_FIELDS'
                  --                               AND domain_code = v_some_data.RACEETHNICITY;
                  --
                  --                        IF v_ETHNIC_DECODE = 'Asian'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_asian := 'Yes';
                  --                        ELSIF v_ETHNIC_DECODE = 'Black'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_black := 'Yes';
                  --                        ELSIF v_ETHNIC_DECODE = 'Hispanic'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_hispanic :=
                  --                              'Yes';
                  --                        ELSIF v_ETHNIC_DECODE = 'Indian'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_native_amer :=
                  --                              'Yes';
                  --                        ELSIF v_ETHNIC_DECODE = 'Islander'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_pacif_islnd :=
                  --                              'Yes';
                  --                        ELSIF v_ETHNIC_DECODE = 'White'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_white := 'Yes';
                  --                        END IF;
                  --                     END IF;
                  --                  EXCEPTION
                  --                     WHEN NO_DATA_FOUND
                  --                     THEN
                  --                        v_details_record.SYS_AUDIT_IND := 'Y';
                  --                        v_WAREHOUSE_KEY := 0;
                  --                        v_AUDIT_BASE_SEVERITY := 0;
                  --
                  --                        v_AUDIT_NATURAL_KEY :=
                  --                              v_BASE_NATURALKEY_TXT
                  --                           || ';RACEETHNICITY='
                  --                           || v_some_data.RACEETHNICITY;
                  --
                  --                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                  --                           v_SYS_ETL_SOURCE,
                  --                           'STUDENT_ORIGIN_FIELDS',
                  --                           v_WAREHOUSE_KEY,
                  --                           v_AUDIT_NATURAL_KEY,
                  --                           'NO_DATA_FOUND',
                  --                           SQLERRM,
                  --                           'N',
                  --                           v_AUDIT_BASE_SEVERITY);
                  --                     WHEN TOO_MANY_ROWS
                  --                     THEN
                  --                        v_details_record.SYS_AUDIT_IND := 'Y';
                  --                        v_WAREHOUSE_KEY := 0;
                  --                        v_AUDIT_BASE_SEVERITY := 0;
                  --
                  --                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
                  --
                  --                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                  --                           v_SYS_ETL_SOURCE,
                  --                           'STUDENT_ORIGIN_FIELDS',
                  --                           v_WAREHOUSE_KEY,
                  --                           v_AUDIT_NATURAL_KEY,
                  --                           'TOO_MANY_ROWS',
                  --                           SQLERRM,
                  --                           'N',
                  --                           v_AUDIT_BASE_SEVERITY);
                  --                  END;
                  --
                  --                  FOR v_data
                  --                     IN (SELECT a.RACEID,
                  --                                b.DOMAIN_DECODE
                  --                           FROM K12INTEL_STAGING_IC.IDENTITYRACEETHNICITY a
                  --                                LEFT JOIN
                  --                                k12intel_userdata.xtbl_domain_decodes b
                  --                                   ON     b.domain_name =
                  --                                             'STUDENT_ORIGIN_IND_FIELDS'
                  --                                      AND TO_CHAR (a.RACEID) = b.domain_code
                  --                          WHERE a.IDENTITYID = v_some_data.IDENTITYID)
                  --                  LOOP
                  --                     BEGIN
                  --                        IF v_data.DOMAIN_DECODE IS NULL
                  --                        THEN
                  --                           v_details_record.SYS_AUDIT_IND := 'Y';
                  --                           v_WAREHOUSE_KEY := 0;
                  --                           v_AUDIT_BASE_SEVERITY := 0;
                  --
                  --                           v_AUDIT_NATURAL_KEY :=
                  --                                 v_BASE_NATURALKEY_TXT
                  --                              || ';RACEID='
                  --                              || NVL (TO_CHAR (v_data.RACEID), '[NULL]');
                  --
                  --                           K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                  --                              v_SYS_ETL_SOURCE,
                  --                              'STUDENT_ORIGIN_FIELDS',
                  --                              v_WAREHOUSE_KEY,
                  --                              v_AUDIT_NATURAL_KEY,
                  --                              'NO_DATA_FOUND',
                  --                              NULL,
                  --                              'N',
                  --                              v_AUDIT_BASE_SEVERITY);
                  --                        ELSIF v_data.DOMAIN_DECODE = 'Asian'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_asian := 'Yes';
                  --                        ELSIF v_data.DOMAIN_DECODE = 'Black'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_black := 'Yes';
                  --                        ELSIF v_data.DOMAIN_DECODE = 'Hispanic'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_hispanic :=
                  --                              'Yes';
                  --                        ELSIF v_data.DOMAIN_DECODE = 'Indian'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_native_amer :=
                  --                              'Yes';
                  --                        ELSIF v_data.DOMAIN_DECODE = 'Islander'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_pacif_islnd :=
                  --                              'Yes';
                  --                        ELSIF v_data.DOMAIN_DECODE = 'White'
                  --                        THEN
                  --                           v_details_record.student_origin_ind_white := 'Yes';
                  --                        ELSE
                  --                           v_details_record.SYS_AUDIT_IND := 'Y';
                  --                           v_AUDIT_NATURAL_KEY :=
                  --                                 v_BASE_NATURALKEY_TXT
                  --                              || ';RACEID='
                  --                              || NVL (TO_CHAR (v_data.RACEID), '[NULL]')
                  --                              || ';DOMAIN_DECODE='
                  --                              || NVL (v_data.DOMAIN_DECODE, '[NULL]');
                  --
                  --
                  --                           v_WAREHOUSE_KEY := v_student_record.STUDENT_KEY;
                  --                           v_AUDIT_BASE_SEVERITY := 0;
                  --
                  --                           K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                  --                              v_SYS_ETL_SOURCE,
                  --                              'RACE CURSOR',
                  --                              v_WAREHOUSE_KEY,
                  --                              v_AUDIT_NATURAL_KEY,
                  --                              'NO_DATA_FOUND',
                  --                              NULL,
                  --                              'N',
                  --                              v_AUDIT_BASE_SEVERITY);
                  --                        END IF;
                  --                     END;
                  --                  END LOOP;

                  v_details_record.student_origin_ind_asian :=
                     CASE
                        WHEN v_some_data.Asian IS NULL THEN '@ERR'
                        WHEN v_some_data.Asian > 0 THEN 'Yes'
                        ELSE 'No'
                     END;
                  v_details_record.student_origin_ind_black :=
                     CASE
                        WHEN v_some_data.black IS NULL THEN '@ERR'
                        WHEN v_some_data.black > 0 THEN 'Yes'
                        ELSE 'No'
                     END;
                  v_details_record.student_origin_ind_hispanic :=
                     CASE
                        WHEN v_some_data.HISPANICETHNICITY IS NULL THEN '@ERR'
                        WHEN v_some_data.HISPANICETHNICITY = 'Y' THEN 'Yes'
                        ELSE 'No'
                     END;
                  v_details_record.student_origin_ind_native_amer :=
                     CASE
                        WHEN v_some_data.Am_Ind_Ak IS NULL THEN '@ERR'
                        WHEN v_some_data.Am_Ind_Ak > 0 THEN 'Yes'
                        ELSE 'No'
                     END;
                  v_details_record.student_origin_ind_pacif_islnd :=
                     CASE
                        WHEN v_some_data.pac_is IS NULL THEN '@ERR'
                        WHEN v_some_data.pac_is > 0 THEN 'Yes'
                        ELSE 'No'
                     END;
                  v_details_record.student_origin_ind_white :=
                     CASE
                        WHEN v_some_data.White IS NULL THEN '@ERR'
                        WHEN v_some_data.White > 0 THEN 'Yes'
                        ELSE 'No'
                     END;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.student_origin_ind_asian := '@ERR';
                     v_details_record.student_origin_ind_black := '@ERR';
                     v_details_record.student_origin_ind_hispanic := '@ERR';
                     v_details_record.student_origin_ind_native_amer := '@ERR';
                     v_details_record.student_origin_ind_pacif_islnd := '@ERR';
                     v_details_record.student_origin_ind_white := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ORIGIN_FIELDS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_TRANSPORT_STATUS
               ---------------------------------------------------------------
               BEGIN
                  SELECT CASE
                            WHEN COUNT (*) > 0 THEN 'Bused'
                            ELSE 'Not Bused'
                         END
                    INTO v_details_record.STUDENT_TRANSPORT_STATUS
                    FROM k12intel_staging_ic.customstudent a
                         INNER JOIN k12intel_Staging_ic.CampusAttribute b
                            ON     a.attributeid = b.attributeid
                               AND a.stage_source = b.stage_source
                   WHERE     a.PERSONID = v_some_data.PERSONID
                         AND b.object = 'Transportation'
                         AND b.element IN ('dRouteNumber',
                                           'pRouteNumber')
                         AND a.VALUE IS NOT NULL
                         AND stage_sis_school_year =
                                v_LOCAL_CURRENT_SCHOOL_YEAR
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE
                         AND a.STAGE_DELETEFLAG = 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_TRANSPORT_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_TRANSPORT_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_TRANSPORT_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_TRANSPORT_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_TRANSPORT_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_TRANSPORT_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- ADDRESS_FIELDS
               ---------------------------------------------------------------
               BEGIN
                  --               /*
                  --                               SELECT
                  --                                   NVL(PHONE,0),
                  --                                   CASE WHEN PHONEPRIVATE = 1 THEN 'No' ELSE 'Yes' END,
                  --                                   CASE WHEN PHYSICAL_POSTOFFICEBOX = 0 THEN PHYSICAL_NUMBER ELSE NULL END,
                  --                                   COALESCE(PHYSICAL_PREFIX,PHYSICAL_DIR),
                  --                                   NULL,
                  --                                   NULL,
                  --                                   PHYSICAL_STREET,
                  --                                   PHYSICAL_TAG,
                  --                                   PHYSICAL_APT,
                  --                                   CASE WHEN PHYSICAL_POSTOFFICEBOX = 0 THEN nvl(PHYSICAL_CITY,'--') ELSE '--' END,
                  --                                   CASE WHEN PHYSICAL_POSTOFFICEBOX = 0 THEN PHYSICAL_STATE ELSE NULL END,
                  --                                   CASE WHEN PHYSICAL_POSTOFFICEBOX = 0 THEN nvl(PHYSICAL_ZIP,'--') ELSE '--' END,
                  --                                   PHYSICAL_COUNTY,
                  --                                   nvl(PHYSICAL_XCOORD,'--') ,
                  --                                   nvl(PHYSICAL_YCOORD,'--') ,
                  --                                   CASE WHEN MAILING_POSTOFFICEBOX  = 1 THEN 'PO Box ' || COALESCE(to_char(MAILING_NUMBER),'') ELSE COALESCE(to_char(MAILING_NUMBER),'') END || ' ' ||
                  --                                                          CASE WHEN MAILING_PREFIX IS NOT NULL THEN MAILING_PREFIX || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_DIR IS NOT NULL THEN MAILING_DIR || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_STREET IS NOT NULL THEN MAILING_STREET || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_TAG IS NOT NULL THEN MAILING_TAG || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_APT IS NOT NULL THEN '#' || MAILING_APT || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_CITY IS NOT NULL THEN MAILING_CITY || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_STATE IS NOT NULL THEN MAILING_STATE || ' ' ELSE '' END ||
                  --                                                          CASE WHEN MAILING_ZIP IS NOT NULL THEN MAILING_ZIP ELSE '' END,
                  --                                   NVL(CATCHMENT_CODE,'--'),
                  --                                   PHYSICAL_ADDRESSID
                  --                                  into
                  --                                   v_details_record.STUDENT_PHONE,
                  --                                   v_details_record.STUDENT_PHONE_RELEASE,
                  --                                   v_details_record.STUDENT_STREET_NUMBER,
                  --                                   v_details_record.STUDENT_STREET_DIRECTION,
                  --                                   v_details_record.STUDENT_STREET_DIRECTION_PRE,
                  --                                   v_details_record.STUDENT_STREET_DIRECTION_POST,
                  --                                   v_details_record.STUDENT_STREET_NAME,
                  --                                   v_details_record.STUDENT_STREET_TYPE,
                  --                                   v_details_record.STUDENT_APARTMENT,
                  --                                   v_details_record.STUDENT_CITY,
                  --                                   v_details_record.STUDENT_STATE_CODE,
                  --                                   v_details_record.STUDENT_POSTAL_CODE,
                  --                                   v_details_record.STUDENT_COUNTY,
                  --                                   v_details_record.STUDENT_XCOORD,
                  --                                   v_details_record.STUDENT_YCOORD,
                  --                                   v_details_record.STUDENT_MAILING_ADDRESS,
                  --                                   v_student_record.STUDENT_CATCHMENT_CODE,
                  --                                   v_ADDRESSID
                  --                               FROM K12INTEL_STAGING_IC.TEMP_ADDRESS
                  --                               WHERE PERSONID = v_some_data.PERSONID
                  --                                   and STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  --               */
                  SELECT *
                    INTO v_address_record
                    FROM K12INTEL_STAGING_IC.TEMP_ADDRESS
                   WHERE     PERSONID = v_some_data.PERSONID
                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;

                  v_details_record.STUDENT_DWELLING_TYPE := '--';
                  v_details_record.STUDENT_PHONE :=
                     NVL (v_address_record.PHONE, '--');

                  CASE
                     WHEN v_address_record.PHONEPRIVATE = 1
                     THEN
                        v_details_record.STUDENT_PHONE_RELEASE := 'No';
                     ELSE
                        v_details_record.STUDENT_PHONE_RELEASE := 'Yes';
                  END CASE;

                  CASE
                     WHEN v_address_record.PHYSICAL_POSTOFFICEBOX = 0
                     THEN
                        v_details_record.STUDENT_STREET_NUMBER :=
                           v_address_record.PHYSICAL_NUMBER;
                     ELSE
                        v_details_record.STUDENT_STREET_NUMBER := NULL;
                  END CASE;

                  v_details_record.STUDENT_STREET_DIRECTION := NULL; --COALESCE(v_address_record.PHYSICAL_PREFIX,v_address_record.PHYSICAL_DIR);
                  v_details_record.STUDENT_STREET_DIRECTION_PRE :=
                     v_address_record.PHYSICAL_PREFIX;
                  v_details_record.STUDENT_STREET_DIRECTION_POST :=
                     v_address_record.PHYSICAL_DIR;
                  v_details_record.STUDENT_STREET_NAME :=
                     v_address_record.PHYSICAL_STREET;
                  v_details_record.STUDENT_STREET_TYPE :=
                     v_address_record.PHYSICAL_TAG;
                  v_details_record.STUDENT_APARTMENT :=
                     v_address_record.PHYSICAL_APT;
                  v_details_record.STUDENT_CITY :=
                     NVL (v_address_record.PHYSICAL_CITY, '--');
                  v_details_record.STUDENT_STATE_CODE :=
                     v_address_record.PHYSICAL_STATE;
                  v_details_record.STUDENT_POSTAL_CODE :=
                     NVL (v_address_record.PHYSICAL_ZIP, '--');
                  v_details_record.STUDENT_COUNTY :=
                     v_address_record.PHYSICAL_COUNTY;
                  v_details_record.STUDENT_XCOORD :=
                     NVL (v_address_record.PHYSICAL_XCOORD, '--');
                  v_details_record.STUDENT_YCOORD :=
                     NVL (v_address_record.PHYSICAL_YCOORD, '--');
                  v_student_record.STUDENT_CATCHMENT_CODE :=
                     NVL (v_address_record.CATCHMENT_CODE, '--');

                  v_details_record.STUDENT_MAILING_ADDRESS := NULL;

                  IF     v_address_record.MAILING_POSTOFFICEBOX = 1
                     AND TRIM (v_address_record.MAILING_NUMBER) IS NOT NULL
                  THEN
                     v_details_record.STUDENT_MAILING_ADDRESS :=
                        'PO Box ' || TRIM (v_address_record.MAILING_NUMBER);
                  ELSIF TRIM (v_address_record.MAILING_NUMBER) IS NOT NULL
                  THEN
                     v_details_record.STUDENT_MAILING_ADDRESS :=
                        TRIM (v_address_record.MAILING_NUMBER);
                  END IF;

                  IF TRIM (v_address_record.MAILING_PREFIX) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_PREFIX);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_PREFIX);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_STREET) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_STREET);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_STREET);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_TAG) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_TAG);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_TAG);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_DIR) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_DIR);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_DIR);
                     END CASE;
                  END IF;

                  IF v_address_record.MAILING_APT IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' #'
                              || TRIM (v_address_record.MAILING_APT);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_APT);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_CITY) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_CITY);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_CITY);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_STATE) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_STATE);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_STATE);
                     END CASE;
                  END IF;

                  IF TRIM (v_address_record.MAILING_ZIP) IS NOT NULL
                  THEN
                     CASE
                        WHEN v_details_record.STUDENT_MAILING_ADDRESS
                                IS NOT NULL
                        THEN
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                                 v_details_record.STUDENT_MAILING_ADDRESS
                              || ' '
                              || TRIM (v_address_record.MAILING_ZIP);
                        ELSE
                           v_details_record.STUDENT_MAILING_ADDRESS :=
                              TRIM (v_address_record.MAILING_ZIP);
                     END CASE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_address_record := NULL;
                     v_details_record.STUDENT_PHONE := '@ERR';
                     v_details_record.STUDENT_PHONE_RELEASE := '@ERR';
                     v_details_record.STUDENT_STREET_NUMBER := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_PRE := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_POST := NULL;
                     v_details_record.STUDENT_STREET_NAME := NULL;
                     v_details_record.STUDENT_STREET_TYPE := NULL;
                     v_details_record.STUDENT_APARTMENT := NULL;
                     v_details_record.STUDENT_CITY := '@ERR';
                     v_details_record.STUDENT_STATE_CODE := NULL;
                     v_details_record.STUDENT_POSTAL_CODE := '@ERR';
                     v_details_record.STUDENT_COUNTY := '@ERR';
                     v_details_record.STUDENT_XCOORD := '0';
                     v_details_record.STUDENT_YCOORD := '0';
                     v_details_record.STUDENT_MAILING_ADDRESS := NULL;
                     v_student_record.STUDENT_CATCHMENT_CODE := '@ERR';
                     v_details_record.STUDENT_DWELLING_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'ADDRESS FIELDS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_address_record := NULL;
                     v_details_record.STUDENT_PHONE := '@ERR';
                     v_details_record.STUDENT_PHONE_RELEASE := '@ERR';
                     v_details_record.STUDENT_STREET_NUMBER := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_PRE := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_POST := NULL;
                     v_details_record.STUDENT_STREET_NAME := NULL;
                     v_details_record.STUDENT_STREET_TYPE := NULL;
                     v_details_record.STUDENT_APARTMENT := NULL;
                     v_details_record.STUDENT_CITY := '@ERR';
                     v_details_record.STUDENT_STATE_CODE := NULL;
                     v_details_record.STUDENT_POSTAL_CODE := '@ERR';
                     v_details_record.STUDENT_COUNTY := '@ERR';
                     v_details_record.STUDENT_XCOORD := '0';
                     v_details_record.STUDENT_YCOORD := '0';
                     v_details_record.STUDENT_MAILING_ADDRESS := NULL;
                     v_student_record.STUDENT_CATCHMENT_CODE := '@ERR';
                     v_details_record.STUDENT_DWELLING_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'ADDRESS FIELDS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_address_record := NULL;
                     v_details_record.STUDENT_PHONE := '@ERR';
                     v_details_record.STUDENT_PHONE_RELEASE := '@ERR';
                     v_details_record.STUDENT_STREET_NUMBER := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_PRE := NULL;
                     v_details_record.STUDENT_STREET_DIRECTION_POST := NULL;
                     v_details_record.STUDENT_STREET_NAME := NULL;
                     v_details_record.STUDENT_STREET_TYPE := NULL;
                     v_details_record.STUDENT_APARTMENT := NULL;
                     v_details_record.STUDENT_CITY := '@ERR';
                     v_details_record.STUDENT_STATE_CODE := NULL;
                     v_details_record.STUDENT_POSTAL_CODE := '@ERR';
                     v_details_record.STUDENT_COUNTY := '@ERR';
                     v_details_record.STUDENT_XCOORD := '0';
                     v_details_record.STUDENT_YCOORD := '0';
                     v_details_record.STUDENT_MAILING_ADDRESS := NULL;
                     v_student_record.STUDENT_CATCHMENT_CODE := '@ERR';
                     v_details_record.STUDENT_DWELLING_TYPE := '@ERR';

                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'ADDRESS FIELDS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_COUNTY_CODE
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_COUNTY_CODE := NULL;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNTY_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_COUNTY_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNTY_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_COUNTY_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNTY_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_COUNTY_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_OUTSIDE_CATCHMENT
               ---------------------------------------------------------------
               BEGIN
                  IF v_address_record.PHYSICAL_ADDRESSID IS NULL
                  THEN
                     v_student_record.STUDENT_OUTSIDE_CATCHMENT := '@ERR';
                  ELSE
                     SELECT CASE
                               WHEN COUNT (*) = 0 THEN 'Yes'
                               ELSE 'No'
                            END
                       INTO v_student_record.STUDENT_OUTSIDE_CATCHMENT
                       FROM k12intel_staging_ic.schoolboundary a
                      WHERE     ADDRESSID =
                                   v_address_record.PHYSICAL_ADDRESSID
                            AND schoolid = v_some_data.schoolid
                            AND stage_source = v_some_data.stage_source;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_OUTSIDE_CATCHMENT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_OUTSIDE_CATCHMENT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_OUTSIDE_CATCHMENT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_OUTSIDE_CATCHMENT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_OUTSIDE_CATCHMENT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_OUTSIDE_CATCHMENT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;


               ---------------------------------------------------------------
               -- STUDENT_CATCHMENT_SCHOOL
               ---------------------------------------------------------------
               BEGIN
                  IF v_address_record.PHYSICAL_ADDRESSID IS NULL
                  THEN
                     v_student_record.STUDENT_CATCHMENT_SCHOOL := '@ERR';
                  ELSE
                     SELECT c.NAME
                       INTO v_student_record.STUDENT_CATCHMENT_SCHOOL
                       FROM k12intel_staging_ic.schoolboundary a
                            INNER JOIN
                            (SELECT a.stage_source,
                                    a.NAME,
                                    b.SCHOOLID
                               FROM k12intel_Staging_ic.GRADELEVEL a
                                    INNER JOIN k12intel_Staging_ic.CALENDAR b
                                       ON     a.calendarid = b.calendarid
                                          AND a.stage_source = b.stage_source
                              WHERE     a.NAME = v_some_data.GRADE
                                    AND b.endyear = v_some_data.ENDYEAR
                                    AND b.EXCLUDE = 0
                                    AND a.STAGE_SOURCE =
                                           v_some_data.STAGE_SOURCE
                                    AND a.seq <> 0) b
                               ON     a.SCHOOLID = b.SCHOOLID
                                  AND a.stage_source = b.stage_source
                            INNER JOIN k12intel_Staging_ic.school c
                               ON     a.schoolid = c.schoolid
                                  AND a.stage_source = c.stage_source
                                  AND c.stage_sis_school_year =
                                         v_LOCAL_CURRENT_SCHOOL_YEAR
                      WHERE     a.addressid =
                                   v_address_record.PHYSICAL_ADDRESSID
                            AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CATCHMENT_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || ';ADDRESSID='
                        || TO_CHAR (v_address_record.PHYSICAL_ADDRESSID);

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CATCHMENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CATCHMENT_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || ';ADDRESSID='
                        || TO_CHAR (v_address_record.PHYSICAL_ADDRESSID);

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CATCHMENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_CATCHMENT_SCHOOL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_CATCHMENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_NEIGHBORHOOD
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_NEIGHBORHOOD := '--';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_NEIGHBORHOOD := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEIGHBORHOOD',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_NEIGHBORHOOD := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEIGHBORHOOD',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_NEIGHBORHOOD := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_NEIGHBORHOOD',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_INFORMATION_RELEASE
               ---------------------------------------------------------------
               BEGIN
                  SELECT CASE
                            WHEN COUNT (*) > 0 THEN 'No'
                            ELSE 'Yes'
                         END
                    INTO v_details_record.STUDENT_INFORMATION_RELEASE
                    FROM k12intel_staging_ic.customstudent a
                         INNER JOIN k12intel_Staging_ic.CampusAttribute b
                            ON     a.attributeid = b.attributeid
                               AND a.stage_source = b.stage_source
                   WHERE     a.PERSONID = v_some_data.PERSONID
                         AND b.object = 'Student Miscellaneous'
                         AND b.element = 'releaseOfInfo'
                         AND a.VALUE = 'N'
                         AND stage_sis_school_year =
                                v_LOCAL_CURRENT_SCHOOL_YEAR
                         AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE
                         AND a.STAGE_DELETEFLAG = 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_INFORMATION_RELEASE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_INFORMATION_RELEASE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_INFORMATION_RELEASE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_INFORMATION_RELEASE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_INFORMATION_RELEASE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_INFORMATION_RELEASE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;



               ---------------------------------------------------------------
               -- STUDENT_EMAIL
               ---------------------------------------------------------------
               BEGIN
                  SELECT EMAIL
                    INTO v_details_record.STUDENT_EMAIL
                    FROM K12INTEL_STAGING_IC.CONTACT
                   WHERE     PERSONID = v_some_data.PERSONID
                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     --v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_EMAIL := NULL;
                  /*v_WAREHOUSE_KEY := 0;
                  v_AUDIT_BASE_SEVERITY := 0;

                  v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                  K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
                      v_SYS_ETL_SOURCE,
                      'STUDENT_EMAIL',
                      v_WAREHOUSE_KEY,
                      v_AUDIT_NATURAL_KEY,
                      'NO_DATA_FOUND',
                      sqlerrm,
                      'N',
                      v_AUDIT_BASE_SEVERITY
                  );*/
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_EMAIL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_EMAIL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_EMAIL := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_EMAIL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_COUNSELOR
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.STUDENT_COUNSELOR := '--';

                  --            SELECT person.lastName, firtName
                  --            WHERE TeamMember.personID=student
                  --            and   TeamMember.staffPersonID=counselor
                  SELECT lastname || ', ' || firstname
                    INTO v_details_record.STUDENT_COUNSELOR
                    FROM k12intel_staging_ic.TeamMember tm
                   WHERE     ROLE = 'Counselor'
                         AND PERSONID = v_some_data.PERSONID
                         AND ROWNUM = 1
                         AND STAGE_SOURCE = v_some_data.STAGE_SOURCE
                         AND v_local_data_date BETWEEN STARTDATE
                                                   AND NVL (
                                                          ENDDATE,
                                                          TO_DATE (
                                                             '12312999',
                                                             'MMDDYYYY'));
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     --  v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNSELOR := '--';
                  --               v_WAREHOUSE_KEY := 0;
                  --               v_AUDIT_BASE_SEVERITY := 0;
                  --
                  --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
                  --
                  --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
                  --                                                    'STUDENT_COUNSELOR',
                  --                                                    v_WAREHOUSE_KEY,
                  --                                                    v_AUDIT_NATURAL_KEY,
                  --                                                    'NO_DATA_FOUND',
                  --                                                    SQLERRM,
                  --                                                    'N',
                  --                                                    v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNSELOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_COUNSELOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_COUNSELOR := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_COUNSELOR',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_GIFTED_YEAR
               ---------------------------------------------------------------
               --         BEGIN
               --            SELECT TO_NUMBER (
               --                      TO_CHAR (MIN (TO_DATE (VALUE, 'mm/dd/yyyy')), 'YYYY'))
               --              INTO v_details_record.STUDENT_GIFTED_YEAR
               --              FROM k12intel_staging_ic.customstudent
               --             WHERE     personid = v_some_data.personid
               --                   AND attributeid IN (232,
               --                                       233,
               --                                       234,
               --                                       235,
               --                                       236,
               --                                       237,
               --                                       239)
               --                   AND stage_sis_school_year = v_LOCAL_CURRENT_SCHOOL_YEAR
               --                   AND stage_source = v_some_data.stage_source
               --                   AND STAGE_DELETEFLAG = 0;
               --         EXCEPTION
               --            WHEN NO_DATA_FOUND
               --            THEN
               --               v_details_record.SYS_AUDIT_IND := 'Y';
               --               v_details_record.STUDENT_GIFTED_YEAR := 0;
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
               --                                                    'STUDENT_GIFTED_YEAR',
               --                                                    v_WAREHOUSE_KEY,
               --                                                    v_AUDIT_NATURAL_KEY,
               --                                                    'NO_DATA_FOUND',
               --                                                    SQLERRM,
               --                                                    'N',
               --                                                    v_AUDIT_BASE_SEVERITY);
               --            WHEN TOO_MANY_ROWS
               --            THEN
               --               v_details_record.SYS_AUDIT_IND := 'Y';
               --               v_details_record.STUDENT_GIFTED_YEAR := 0;
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
               --                                                    'STUDENT_GIFTED_YEAR',
               --                                                    v_WAREHOUSE_KEY,
               --                                                    v_AUDIT_NATURAL_KEY,
               --                                                    'TOO_MANY_ROWS',
               --                                                    SQLERRM,
               --                                                    'N',
               --                                                    v_AUDIT_BASE_SEVERITY);
               --            WHEN OTHERS
               --            THEN
               --               v_details_record.SYS_AUDIT_IND := 'Y';
               --               v_details_record.STUDENT_GIFTED_YEAR := 0;
               --               v_WAREHOUSE_KEY := 0;
               --               v_AUDIT_BASE_SEVERITY := 0;
               --
               --               v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;
               --
               --               K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (v_SYS_ETL_SOURCE,
               --                                                    'STUDENT_GIFTED_YEAR',
               --                                                    v_WAREHOUSE_KEY,
               --                                                    v_AUDIT_NATURAL_KEY,
               --                                                    'Untrapped Error',
               --                                                    SQLERRM,
               --                                                    'Y',
               --                                                    v_AUDIT_BASE_SEVERITY);
               --         END;

               ---------------------------------------------------------------
               -- STUDENT_GRADUATION_STATUS
               ---------------------------------------------------------------
               BEGIN
                  IF     v_DIPLOMATYPE IS NOT NULL
                     AND v_DIPLOMADATE <= v_local_data_date
                  THEN
                     v_details_record.STUDENT_GRADUATION_STATUS := 'Graduated';
                  ELSIF     v_DIPLOMATYPE IS NOT NULL
                        AND NVL (v_DIPLOMADATE,
                                 TO_DATE ('12/31/9999',
                                          'mm/dd/yyyy')) > v_local_data_date
                  THEN
                     v_details_record.STUDENT_GRADUATION_STATUS :=
                        'Not Graduated';
                  ELSE
                     v_details_record.STUDENT_GRADUATION_STATUS :=
                        'Not Applicable';
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_GRADUATION_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_GRADUATION_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_GRADUATION_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_GRADUATION_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_GRADUATION_STATUS := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_GRADUATION_STATUS',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_DIPLOMA_TYPE
               ---------------------------------------------------------------
               BEGIN
                  IF v_DIPLOMATYPE IS NULL
                  THEN
                     v_details_record.STUDENT_DIPLOMA_TYPE := '--';
                  ELSE
                     SELECT SUBSTR (b.NAME,
                                    1,
                                    30)
                       INTO v_details_record.STUDENT_DIPLOMA_TYPE
                       FROM k12intel_staging_ic.CAMPUSATTRIBUTE a
                            INNER JOIN k12intel_staging_ic.CAMPUSDICTIONARY b
                               ON     a.ATTRIBUTEID = b.ATTRIBUTEID
                                  AND a.STAGE_SOURCE = b.STAGE_SOURCE
                      WHERE     a.OBJECT = 'Graduation'
                            AND a.ELEMENT = 'diplomaType'
                            AND b.CODE = v_DIPLOMATYPE
                            AND a.STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_DIPLOMA_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || ';DIPLOMATYPE='
                        || v_DIPLOMATYPE;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_DIPLOMA_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_DIPLOMA_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY :=
                           v_BASE_NATURALKEY_TXT
                        || ';DIPLOMATYPE='
                        || v_DIPLOMATYPE;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_DIPLOMA_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.STUDENT_DIPLOMA_TYPE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_DIPLOMA_TYPE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- DAYS_IN_CURRENT_DISTRICT
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.DAYS_IN_CURRENT_DISTRICT := 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_DISTRICT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_DISTRICT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_DISTRICT := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- DAYS_IN_CURRENT_SCHOOL
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.DAYS_IN_CURRENT_SCHOOL := 0;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_SCHOOL := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_SCHOOL := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.DAYS_IN_CURRENT_SCHOOL := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'DAYS_IN_CURRENT_SCHOOL',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- RESIDENT_DISTRICT_CODE
               ---------------------------------------------------------------
               BEGIN
                  v_details_record.RESIDENT_DISTRICT_CODE :=
                     COALESCE (v_some_data.RESIDENTDISTRICT,
                               '--');
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT_CODE := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT_CODE',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- RESIDENT_DISTRICT
               ---------------------------------------------------------------
               BEGIN
                  IF v_some_data.RESIDENTDISTRICT IS NULL
                  THEN
                     v_details_record.RESIDENT_DISTRICT := '--';
                  ELSE
                     SELECT "NAME"
                       INTO v_details_record.RESIDENT_DISTRICT
                       FROM K12INTEL_STAGING_IC.DISTRICT
                      WHERE     "NUMBER" = v_some_data.RESIDENTDISTRICT
                            AND STAGE_SOURCE = v_some_data.STAGE_SOURCE;
                  END IF;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'NO_DATA_FOUND',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_details_record.SYS_AUDIT_IND := 'Y';
                     v_details_record.RESIDENT_DISTRICT := '@ERR';
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'RESIDENT_DISTRICT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               ---------------------------------------------------------------
               -- STUDENT_ATTRIB_KEY
               ---------------------------------------------------------------
               BEGIN
                  SELECT STUDENT_ATTRIB_KEY
                    INTO v_student_record.STUDENT_ATTRIB_KEY
                    FROM K12INTEL_DW.DTBL_STUDENT_ATTRIBS
                   WHERE     1 = 1
                         AND STUDENT_ACTIVITY_INDICATOR =
                                v_student_record.STUDENT_ACTIVITY_INDICATOR
                         AND STUDENT_STATUS = v_student_record.STUDENT_STATUS
                         AND STUDENT_GENDER_CODE =
                                v_student_record.STUDENT_GENDER_CODE
                         AND STUDENT_GENDER = v_student_record.STUDENT_GENDER
                         AND STUDENT_RACE_CODE =
                                v_student_record.STUDENT_RACE_CODE
                         AND STUDENT_RACE = v_student_record.STUDENT_RACE
                         AND STUDENT_FOODSERVICE_INDICATOR =
                                v_student_record.STUDENT_FOODSERVICE_INDICATOR
                         AND STUDENT_FOODSERVICE_ELIG_CODE =
                                v_student_record.STUDENT_FOODSERVICE_ELIG_CODE
                         AND STUDENT_FOODSERVICE_ELIG =
                                v_student_record.STUDENT_FOODSERVICE_ELIG
                         AND STUDENT_SPECIAL_ED_CLASS =
                                v_student_record.STUDENT_SPECIAL_ED_CLASS
                         AND STUDENT_EDUCATIONAL_EXCEPT_TYP =
                                v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
                         AND STUDENT_SPECIAL_ED_INDICATOR =
                                v_student_record.STUDENT_SPECIAL_ED_INDICATOR
                         AND STUDENT_ESL_CLASSIFICATION =
                                v_student_record.STUDENT_ESL_CLASSIFICATION
                         AND STUDENT_ESL_INDICATOR =
                                v_student_record.STUDENT_ESL_INDICATOR
                         AND STUDENT_LEP_INDICATOR =
                                v_student_record.STUDENT_LEP_INDICATOR
                         AND STUDENT_HOMELESS_INDICATOR =
                                v_student_record.STUDENT_HOMELESS_INDICATOR
                         AND STUDENT_AT_RISK_INDICATOR =
                                v_student_record.STUDENT_AT_RISK_INDICATOR
                         AND STUDENT_504_INDICATOR =
                                v_student_record.STUDENT_504_INDICATOR
                         AND STUDENT_INDIAN_ED_INDICATOR =
                                v_student_record.STUDENT_INDIAN_ED_INDICATOR
                         AND STUDENT_MIGRANT_ED_INDICATOR =
                                v_student_record.STUDENT_MIGRANT_ED_INDICATOR
                         AND STUDENT_CURRENT_GRADE_LEVEL =
                                v_student_record.STUDENT_CURRENT_GRADE_LEVEL
                         AND STUDENT_CURRENT_GRADE_CODE =
                                v_student_record.STUDENT_CURRENT_GRADE_CODE
                         AND DISTRICT_CODE = v_student_record.DISTRICT_CODE;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     K12INTEL_METADATA.GEN_SURROGATEKEY (
                        'DTBL_STUDENT_ATTRIBS',
                        v_student_record.student_attrib_key);

                     IF v_student_record.STUDENT_ATTRIB_KEY = 0
                     THEN
                        v_student_record.SYS_AUDIT_IND := 'Y';
                        v_student_record.student_attrib_key := 0;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                           v_SYS_ETL_SOURCE,
                           'STUDENT_ATTRIB_KEY',
                           v_WAREHOUSE_KEY,
                           v_AUDIT_NATURAL_KEY,
                           'ERROR GENERATING ATTRIB KEY',
                           SQLERRM,
                           'Y',
                           v_AUDIT_BASE_SEVERITY);
                     ELSE
                        BEGIN
                           INSERT INTO k12intel_dw.dtbl_student_attribs
                              VALUES (
                                        v_student_record.STUDENT_ATTRIB_KEY, --STUDENT_ATTRIB_KEY
                                        v_student_record.STUDENT_ACTIVITY_INDICATOR, --STUDENT_ACTIVITY_INDICATOR
                                        v_student_record.STUDENT_STATUS, --STUDENT_STATUS
                                        v_student_record.STUDENT_GENDER_CODE, --STUDENT_GENDER_CODE
                                        v_student_record.STUDENT_GENDER, --STUDENT_GENDER
                                        v_student_record.STUDENT_RACE_CODE, --STUDENT_RACE_CODE
                                        v_student_record.STUDENT_RACE, --STUDENT_RACE
                                        v_student_record.STUDENT_FOODSERVICE_INDICATOR, --STUDENT_FOODSERVICE_INDICATOR
                                        v_student_record.STUDENT_FOODSERVICE_ELIG_CODE, --STUDENT_FOODSERVICE_ELIG_CODE
                                        v_student_record.STUDENT_FOODSERVICE_ELIG, --STUDENT_FOODSERVICE_ELIG
                                        v_student_record.STUDENT_SPECIAL_ED_CLASS, --STUDENT_SPECIAL_ED_CLASS
                                        v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP, --STUDENT_EDUCATIONAL_EXCEPT_TYP
                                        v_student_record.STUDENT_SPECIAL_ED_INDICATOR, --STUDENT_SPECIAL_ED_INDICATOR
                                        v_student_record.STUDENT_ESL_INDICATOR, --STUDENT_ESL_INDICATOR
                                        v_student_record.STUDENT_ESL_CLASSIFICATION, --STUDENT_ESL_CLASSIFICATION
                                        v_student_record.STUDENT_LEP_INDICATOR, --STUDENT_LEP_INDICATOR
                                        v_student_record.STUDENT_HOMELESS_INDICATOR, --STUDENT_HOMELESS_INDICATOR
                                        v_student_record.STUDENT_AT_RISK_INDICATOR, --STUDENT_AT_RISK_INDICATOR
                                        v_student_record.STUDENT_504_INDICATOR, --STUDENT_504_INDICATOR
                                        v_student_record.STUDENT_INDIAN_ED_INDICATOR, --STUDENT_INDIAN_ED_INDICATOR
                                        v_student_record.STUDENT_MIGRANT_ED_INDICATOR, --STUDENT_MIGRANT_ED_INDICATOR
                                        v_student_record.STUDENT_CURRENT_GRADE_LEVEL, --STUDENT_CURRENT_GRADE_LEVEL
                                        v_student_record.STUDENT_CURRENT_GRADE_CODE, --STUDENT_CURRENT_GRADE_CODE
                                        v_student_record.DISTRICT_CODE, --DISTRICT_CODE
                                        v_SYS_ETL_SOURCE,     --SYS_ETL_SOURCE
                                        SYSDATE,                 --SYS_CREATED
                                        SYSDATE,                 --SYS_UPDATED
                                        v_student_record.SYS_AUDIT_IND, --SYS_AUDIT_IND
                                        'N',                   --SYS_DUMMY_IND
                                        0                --SYS_PARTITION_VALUE
                                         );
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              v_student_record.SYS_AUDIT_IND := 'Y';
                              v_student_record.STUDENT_ATTRIB_KEY := 0;
                              v_WAREHOUSE_KEY := 0;
                              v_AUDIT_BASE_SEVERITY := 0;

                              v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                              K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                                 v_SYS_ETL_SOURCE,
                                 'STUDENT_ATTRIB_KEY',
                                 v_WAREHOUSE_KEY,
                                 v_AUDIT_NATURAL_KEY,
                                 'ERROR INSERTING ATTRIB RECORD',
                                 SQLERRM,
                                 'Y',
                                 v_AUDIT_BASE_SEVERITY);
                        END;
                     END IF;
                  WHEN TOO_MANY_ROWS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_ATTRIB_KEY := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ATTRIB_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'TOO_MANY_ROWS',
                        SQLERRM,
                        'N',
                        v_AUDIT_BASE_SEVERITY);
                  WHEN OTHERS
                  THEN
                     v_student_record.SYS_AUDIT_IND := 'Y';
                     v_student_record.STUDENT_ATTRIB_KEY := 0;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'STUDENT_ATTRIB_KEY',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
               END;

               BEGIN
                  SELECT STUDENT_KEY
                    INTO v_existing_STUDENT_KEY
                    FROM K12INTEL_DW.DTBL_STUDENTS
                   WHERE STUDENT_KEY = v_student_record.STUDENT_KEY;

                  BEGIN
                     UPDATE K12INTEL_DW.DTBL_STUDENTS
                        SET STUDENT_ATTRIB_KEY =
                               v_student_record.STUDENT_ATTRIB_KEY,
                            SCHOOL_KEY = v_student_record.SCHOOL_KEY,
                            STUDENT_ID = v_student_record.STUDENT_ID,
                            STUDENT_NAME = v_student_record.STUDENT_NAME,
                            STUDENT_ACTIVITY_INDICATOR =
                               v_student_record.STUDENT_ACTIVITY_INDICATOR,
                            STUDENT_STATUS = v_student_record.STUDENT_STATUS,
                            STUDENT_FTE_GROUP =
                               v_student_record.STUDENT_FTE_GROUP,
                            STUDENT_GENDER_CODE =
                               v_student_record.STUDENT_GENDER_CODE,
                            STUDENT_GENDER = v_student_record.STUDENT_GENDER,
                            STUDENT_AGE = v_student_record.STUDENT_AGE,
                            STUDENT_RACE_CODE =
                               v_student_record.STUDENT_RACE_CODE,
                            STUDENT_RACE = v_student_record.STUDENT_RACE,
                            STUDENT_ETHNICITY_CODE =
                               v_student_record.STUDENT_ETHNICITY_CODE,
                            STUDENT_ETHNICITY =
                               v_student_record.STUDENT_ETHNICITY,
                            STUDENT_LANGUAGE =
                               v_student_record.STUDENT_LANGUAGE,
                            STUDENT_HOME_LANGUAGE =
                               v_student_record.STUDENT_HOME_LANGUAGE,
                            STUDENT_COUNTRY_OF_CITIZENSHIP =
                               v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP,
                            STUDENT_COUNTRY_OF_BIRTH =
                               v_student_record.STUDENT_COUNTRY_OF_BIRTH,
                            STUDENT_FOODSERVICE_INDICATOR =
                               v_student_record.STUDENT_FOODSERVICE_INDICATOR,
                            STUDENT_FOODSERVICE_ELIG_CODE =
                               v_student_record.STUDENT_FOODSERVICE_ELIG_CODE,
                            STUDENT_FOODSERVICE_ELIG =
                               v_student_record.STUDENT_FOODSERVICE_ELIG,
                            STUDENT_SPECIAL_ED_INDICATOR =
                               v_student_record.STUDENT_SPECIAL_ED_INDICATOR,
                            STUDENT_SPECIAL_ED_CLASS =
                               v_student_record.STUDENT_SPECIAL_ED_CLASS,
                            STUDENT_EDUCATIONAL_EXCEPT_TYP =
                               v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP,
                            STUDENT_ESL_INDICATOR =
                               v_student_record.STUDENT_ESL_INDICATOR,
                            STUDENT_LEP_INDICATOR =
                               v_student_record.STUDENT_LEP_INDICATOR,
                            STUDENT_ESL_CLASSIFICATION =
                               v_student_record.STUDENT_ESL_CLASSIFICATION,
                            STUDENT_GIFTED_INDICATOR =
                               v_student_record.STUDENT_GIFTED_INDICATOR,
                            STUDENT_CUMULATIVE_GPA =
                               v_student_record.STUDENT_CUMULATIVE_GPA,
                            STUDENT_CURRENT_GPA =
                               v_student_record.STUDENT_CURRENT_GPA,
                            WEIGHTED_CUMULATIVE_GPA =
                               v_student_record.WEIGHTED_CUMULATIVE_GPA,
                            WEIGHTED_CURRENT_GPA =
                               v_student_record.WEIGHTED_CURRENT_GPA,
                            STUDENT_ADMISSION_TYPE =
                               v_student_record.STUDENT_ADMISSION_TYPE,
                            STUDENT_LIVING_WITH =
                               v_student_record.STUDENT_LIVING_WITH,
                            STUDENT_RESIDENCE_CHANGES =
                               v_student_record.STUDENT_RESIDENCE_CHANGES,
                            STUDENT_SCHOOL_CHANGES =
                               v_student_record.STUDENT_SCHOOL_CHANGES,
                            STUDENT_CATCHMENT_CODE =
                               v_student_record.STUDENT_CATCHMENT_CODE,
                            STUDENT_CROSS_ENROLL_INDICATOR =
                               v_student_record.STUDENT_CROSS_ENROLL_INDICATOR,
                            STUDENT_OUTSIDE_CATCHMENT =
                               v_student_record.STUDENT_OUTSIDE_CATCHMENT,
                            STUDENT_CATCHMENT_SCHOOL =
                               v_student_record.STUDENT_CATCHMENT_SCHOOL,
                            STUDENT_CURRENT_GRADE_CODE =
                               v_student_record.STUDENT_CURRENT_GRADE_CODE,
                            STUDENT_CURRENT_GRADE_LEVEL =
                               v_student_record.STUDENT_CURRENT_GRADE_LEVEL,
                            STUDENT_CURRENT_DISTRICT_CODE =
                               v_student_record.STUDENT_CURRENT_DISTRICT_CODE,
                            STUDENT_CURRENT_SCHOOL_CODE =
                               v_student_record.STUDENT_CURRENT_SCHOOL_CODE,
                            STUDENT_CURRENT_SCHOOL =
                               v_student_record.STUDENT_CURRENT_SCHOOL,
                            STUDENT_CURRENT_HOMEROOM =
                               v_student_record.STUDENT_CURRENT_HOMEROOM,
                            STUDENT_NEXT_YEAR_GRADE_CODE =
                               v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE,
                            STUDENT_NEXT_YEAR_GRADE_LEVEL =
                               v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL,
                            STUDENT_NEXT_YEAR_SCHOOL_CODE =
                               v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE,
                            STUDENT_NEXT_YEAR_SCHOOL =
                               v_student_record.STUDENT_NEXT_YEAR_SCHOOL,
                            STUDENT_NEXT_YEAR_HOMEROOM =
                               v_student_record.STUDENT_NEXT_YEAR_HOMEROOM,
                            STUDENT_GRADUATION_COHORT =
                               v_student_record.STUDENT_GRADUATION_COHORT,
                            STUDENT_1ST_GRADE_COHORT =
                               v_student_record.STUDENT_1ST_GRADE_COHORT,
                            STUDENT_HOMELESS_INDICATOR =
                               v_student_record.STUDENT_HOMELESS_INDICATOR,
                            STUDENT_AT_RISK_INDICATOR =
                               v_student_record.STUDENT_AT_RISK_INDICATOR,
                            STUDENT_504_INDICATOR =
                               v_student_record.STUDENT_504_INDICATOR,
                            STUDENT_INDIAN_ED_INDICATOR =
                               v_student_record.STUDENT_INDIAN_ED_INDICATOR,
                            STUDENT_MIGRANT_ED_INDICATOR =
                               v_student_record.STUDENT_MIGRANT_ED_INDICATOR,
                            DISTRICT_CODE = v_student_record.DISTRICT_CODE,
                            SYS_ETL_SOURCE = v_SYS_ETL_SOURCE,
                            SYS_UPDATED = SYSDATE,
                            SYS_AUDIT_IND = v_student_record.SYS_AUDIT_IND,
                            SYS_DUMMY_IND = 'N',
                            SYS_PARTITION_VALUE =
                               v_student_record.SYS_PARTITION_VALUE
                      WHERE     STUDENT_KEY = v_existing_STUDENT_KEY
                            AND (   (   (v_student_record.STUDENT_ATTRIB_KEY <>
                                            STUDENT_ATTRIB_KEY)
                                     OR (    v_student_record.STUDENT_ATTRIB_KEY
                                                IS NOT NULL
                                         AND STUDENT_ATTRIB_KEY IS NULL)
                                     OR (    v_student_record.STUDENT_ATTRIB_KEY
                                                IS NULL
                                         AND STUDENT_ATTRIB_KEY IS NOT NULL))
                                 OR (   (v_student_record.SCHOOL_KEY <>
                                            SCHOOL_KEY)
                                     OR (    v_student_record.SCHOOL_KEY
                                                IS NOT NULL
                                         AND SCHOOL_KEY IS NULL)
                                     OR (    v_student_record.SCHOOL_KEY
                                                IS NULL
                                         AND SCHOOL_KEY IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ID <>
                                            STUDENT_ID)
                                     OR (    v_student_record.STUDENT_ID
                                                IS NOT NULL
                                         AND STUDENT_ID IS NULL)
                                     OR (    v_student_record.STUDENT_ID
                                                IS NULL
                                         AND STUDENT_ID IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NAME <>
                                            STUDENT_NAME)
                                     OR (    v_student_record.STUDENT_NAME
                                                IS NOT NULL
                                         AND STUDENT_NAME IS NULL)
                                     OR (    v_student_record.STUDENT_NAME
                                                IS NULL
                                         AND STUDENT_NAME IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ACTIVITY_INDICATOR <>
                                            STUDENT_ACTIVITY_INDICATOR)
                                     OR (    v_student_record.STUDENT_ACTIVITY_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_ACTIVITY_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_ACTIVITY_INDICATOR
                                                IS NULL
                                         AND STUDENT_ACTIVITY_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_STATUS <>
                                            STUDENT_STATUS)
                                     OR (    v_student_record.STUDENT_STATUS
                                                IS NOT NULL
                                         AND STUDENT_STATUS IS NULL)
                                     OR (    v_student_record.STUDENT_STATUS
                                                IS NULL
                                         AND STUDENT_STATUS IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_FTE_GROUP <>
                                            STUDENT_FTE_GROUP)
                                     OR (    v_student_record.STUDENT_FTE_GROUP
                                                IS NOT NULL
                                         AND STUDENT_FTE_GROUP IS NULL)
                                     OR (    v_student_record.STUDENT_FTE_GROUP
                                                IS NULL
                                         AND STUDENT_FTE_GROUP IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_GENDER_CODE <>
                                            STUDENT_GENDER_CODE)
                                     OR (    v_student_record.STUDENT_GENDER_CODE
                                                IS NOT NULL
                                         AND STUDENT_GENDER_CODE IS NULL)
                                     OR (    v_student_record.STUDENT_GENDER_CODE
                                                IS NULL
                                         AND STUDENT_GENDER_CODE IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_GENDER <>
                                            STUDENT_GENDER)
                                     OR (    v_student_record.STUDENT_GENDER
                                                IS NOT NULL
                                         AND STUDENT_GENDER IS NULL)
                                     OR (    v_student_record.STUDENT_GENDER
                                                IS NULL
                                         AND STUDENT_GENDER IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_AGE <>
                                            STUDENT_AGE)
                                     OR (    v_student_record.STUDENT_AGE
                                                IS NOT NULL
                                         AND STUDENT_AGE IS NULL)
                                     OR (    v_student_record.STUDENT_AGE
                                                IS NULL
                                         AND STUDENT_AGE IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_RACE_CODE <>
                                            STUDENT_RACE_CODE)
                                     OR (    v_student_record.STUDENT_RACE_CODE
                                                IS NOT NULL
                                         AND STUDENT_RACE_CODE IS NULL)
                                     OR (    v_student_record.STUDENT_RACE_CODE
                                                IS NULL
                                         AND STUDENT_RACE_CODE IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_RACE <>
                                            STUDENT_RACE)
                                     OR (    v_student_record.STUDENT_RACE
                                                IS NOT NULL
                                         AND STUDENT_RACE IS NULL)
                                     OR (    v_student_record.STUDENT_RACE
                                                IS NULL
                                         AND STUDENT_RACE IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ETHNICITY_CODE <>
                                            STUDENT_ETHNICITY_CODE)
                                     OR (    v_student_record.STUDENT_ETHNICITY_CODE
                                                IS NOT NULL
                                         AND STUDENT_ETHNICITY_CODE IS NULL)
                                     OR (    v_student_record.STUDENT_ETHNICITY_CODE
                                                IS NULL
                                         AND STUDENT_ETHNICITY_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ETHNICITY <>
                                            STUDENT_ETHNICITY)
                                     OR (    v_student_record.STUDENT_ETHNICITY
                                                IS NOT NULL
                                         AND STUDENT_ETHNICITY IS NULL)
                                     OR (    v_student_record.STUDENT_ETHNICITY
                                                IS NULL
                                         AND STUDENT_ETHNICITY IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_LANGUAGE <>
                                            STUDENT_LANGUAGE)
                                     OR (    v_student_record.STUDENT_LANGUAGE
                                                IS NOT NULL
                                         AND STUDENT_LANGUAGE IS NULL)
                                     OR (    v_student_record.STUDENT_LANGUAGE
                                                IS NULL
                                         AND STUDENT_LANGUAGE IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_HOME_LANGUAGE <>
                                            STUDENT_HOME_LANGUAGE)
                                     OR (    v_student_record.STUDENT_HOME_LANGUAGE
                                                IS NOT NULL
                                         AND STUDENT_HOME_LANGUAGE IS NULL)
                                     OR (    v_student_record.STUDENT_HOME_LANGUAGE
                                                IS NULL
                                         AND STUDENT_HOME_LANGUAGE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP <>
                                            STUDENT_COUNTRY_OF_CITIZENSHIP)
                                     OR (    v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP
                                                IS NOT NULL
                                         AND STUDENT_COUNTRY_OF_CITIZENSHIP
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP
                                                IS NULL
                                         AND STUDENT_COUNTRY_OF_CITIZENSHIP
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_COUNTRY_OF_BIRTH <>
                                            STUDENT_COUNTRY_OF_BIRTH)
                                     OR (    v_student_record.STUDENT_COUNTRY_OF_BIRTH
                                                IS NOT NULL
                                         AND STUDENT_COUNTRY_OF_BIRTH IS NULL)
                                     OR (    v_student_record.STUDENT_COUNTRY_OF_BIRTH
                                                IS NULL
                                         AND STUDENT_COUNTRY_OF_BIRTH
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_FOODSERVICE_INDICATOR <>
                                            STUDENT_FOODSERVICE_INDICATOR)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_FOODSERVICE_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_INDICATOR
                                                IS NULL
                                         AND STUDENT_FOODSERVICE_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_FOODSERVICE_ELIG_CODE <>
                                            STUDENT_FOODSERVICE_ELIG_CODE)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_ELIG_CODE
                                                IS NOT NULL
                                         AND STUDENT_FOODSERVICE_ELIG_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_ELIG_CODE
                                                IS NULL
                                         AND STUDENT_FOODSERVICE_ELIG_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_FOODSERVICE_ELIG <>
                                            STUDENT_FOODSERVICE_ELIG)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_ELIG
                                                IS NOT NULL
                                         AND STUDENT_FOODSERVICE_ELIG IS NULL)
                                     OR (    v_student_record.STUDENT_FOODSERVICE_ELIG
                                                IS NULL
                                         AND STUDENT_FOODSERVICE_ELIG
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_SPECIAL_ED_INDICATOR <>
                                            STUDENT_SPECIAL_ED_INDICATOR)
                                     OR (    v_student_record.STUDENT_SPECIAL_ED_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_SPECIAL_ED_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_SPECIAL_ED_INDICATOR
                                                IS NULL
                                         AND STUDENT_SPECIAL_ED_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_SPECIAL_ED_CLASS <>
                                            STUDENT_SPECIAL_ED_CLASS)
                                     OR (    v_student_record.STUDENT_SPECIAL_ED_CLASS
                                                IS NOT NULL
                                         AND STUDENT_SPECIAL_ED_CLASS IS NULL)
                                     OR (    v_student_record.STUDENT_SPECIAL_ED_CLASS
                                                IS NULL
                                         AND STUDENT_SPECIAL_ED_CLASS
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP <>
                                            STUDENT_EDUCATIONAL_EXCEPT_TYP)
                                     OR (    v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
                                                IS NOT NULL
                                         AND STUDENT_EDUCATIONAL_EXCEPT_TYP
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP
                                                IS NULL
                                         AND STUDENT_EDUCATIONAL_EXCEPT_TYP
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ESL_INDICATOR <>
                                            STUDENT_ESL_INDICATOR)
                                     OR (    v_student_record.STUDENT_ESL_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_ESL_INDICATOR IS NULL)
                                     OR (    v_student_record.STUDENT_ESL_INDICATOR
                                                IS NULL
                                         AND STUDENT_ESL_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_LEP_INDICATOR <>
                                            STUDENT_LEP_INDICATOR)
                                     OR (    v_student_record.STUDENT_LEP_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_LEP_INDICATOR IS NULL)
                                     OR (    v_student_record.STUDENT_LEP_INDICATOR
                                                IS NULL
                                         AND STUDENT_LEP_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ESL_CLASSIFICATION <>
                                            STUDENT_ESL_CLASSIFICATION)
                                     OR (    v_student_record.STUDENT_ESL_CLASSIFICATION
                                                IS NOT NULL
                                         AND STUDENT_ESL_CLASSIFICATION
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_ESL_CLASSIFICATION
                                                IS NULL
                                         AND STUDENT_ESL_CLASSIFICATION
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_GIFTED_INDICATOR <>
                                            STUDENT_GIFTED_INDICATOR)
                                     OR (    v_student_record.STUDENT_GIFTED_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_GIFTED_INDICATOR IS NULL)
                                     OR (    v_student_record.STUDENT_GIFTED_INDICATOR
                                                IS NULL
                                         AND STUDENT_GIFTED_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CUMULATIVE_GPA <>
                                            STUDENT_CUMULATIVE_GPA)
                                     OR (    v_student_record.STUDENT_CUMULATIVE_GPA
                                                IS NOT NULL
                                         AND STUDENT_CUMULATIVE_GPA IS NULL)
                                     OR (    v_student_record.STUDENT_CUMULATIVE_GPA
                                                IS NULL
                                         AND STUDENT_CUMULATIVE_GPA
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_GPA <>
                                            STUDENT_CURRENT_GPA)
                                     OR (    v_student_record.STUDENT_CURRENT_GPA
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_GPA IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_GPA
                                                IS NULL
                                         AND STUDENT_CURRENT_GPA IS NOT NULL))
                                 OR (   (v_student_record.WEIGHTED_CUMULATIVE_GPA <>
                                            WEIGHTED_CUMULATIVE_GPA)
                                     OR (    v_student_record.WEIGHTED_CUMULATIVE_GPA
                                                IS NOT NULL
                                         AND WEIGHTED_CUMULATIVE_GPA IS NULL)
                                     OR (    v_student_record.WEIGHTED_CUMULATIVE_GPA
                                                IS NULL
                                         AND WEIGHTED_CUMULATIVE_GPA
                                                IS NOT NULL))
                                 OR (   (v_student_record.WEIGHTED_CURRENT_GPA <>
                                            WEIGHTED_CURRENT_GPA)
                                     OR (    v_student_record.WEIGHTED_CURRENT_GPA
                                                IS NOT NULL
                                         AND WEIGHTED_CURRENT_GPA IS NULL)
                                     OR (    v_student_record.WEIGHTED_CURRENT_GPA
                                                IS NULL
                                         AND WEIGHTED_CURRENT_GPA IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_ADMISSION_TYPE <>
                                            STUDENT_ADMISSION_TYPE)
                                     OR (    v_student_record.STUDENT_ADMISSION_TYPE
                                                IS NOT NULL
                                         AND STUDENT_ADMISSION_TYPE IS NULL)
                                     OR (    v_student_record.STUDENT_ADMISSION_TYPE
                                                IS NULL
                                         AND STUDENT_ADMISSION_TYPE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_LIVING_WITH <>
                                            STUDENT_LIVING_WITH)
                                     OR (    v_student_record.STUDENT_LIVING_WITH
                                                IS NOT NULL
                                         AND STUDENT_LIVING_WITH IS NULL)
                                     OR (    v_student_record.STUDENT_LIVING_WITH
                                                IS NULL
                                         AND STUDENT_LIVING_WITH IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_RESIDENCE_CHANGES <>
                                            STUDENT_RESIDENCE_CHANGES)
                                     OR (    v_student_record.STUDENT_RESIDENCE_CHANGES
                                                IS NOT NULL
                                         AND STUDENT_RESIDENCE_CHANGES
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_RESIDENCE_CHANGES
                                                IS NULL
                                         AND STUDENT_RESIDENCE_CHANGES
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_SCHOOL_CHANGES <>
                                            STUDENT_SCHOOL_CHANGES)
                                     OR (    v_student_record.STUDENT_SCHOOL_CHANGES
                                                IS NOT NULL
                                         AND STUDENT_SCHOOL_CHANGES IS NULL)
                                     OR (    v_student_record.STUDENT_SCHOOL_CHANGES
                                                IS NULL
                                         AND STUDENT_SCHOOL_CHANGES
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CATCHMENT_CODE <>
                                            STUDENT_CATCHMENT_CODE)
                                     OR (    v_student_record.STUDENT_CATCHMENT_CODE
                                                IS NOT NULL
                                         AND STUDENT_CATCHMENT_CODE IS NULL)
                                     OR (    v_student_record.STUDENT_CATCHMENT_CODE
                                                IS NULL
                                         AND STUDENT_CATCHMENT_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CROSS_ENROLL_INDICATOR <>
                                            STUDENT_CROSS_ENROLL_INDICATOR)
                                     OR (    v_student_record.STUDENT_CROSS_ENROLL_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_CROSS_ENROLL_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_CROSS_ENROLL_INDICATOR
                                                IS NULL
                                         AND STUDENT_CROSS_ENROLL_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_OUTSIDE_CATCHMENT <>
                                            STUDENT_OUTSIDE_CATCHMENT)
                                     OR (    v_student_record.STUDENT_OUTSIDE_CATCHMENT
                                                IS NOT NULL
                                         AND STUDENT_OUTSIDE_CATCHMENT
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_OUTSIDE_CATCHMENT
                                                IS NULL
                                         AND STUDENT_OUTSIDE_CATCHMENT
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CATCHMENT_SCHOOL <>
                                            STUDENT_CATCHMENT_SCHOOL)
                                     OR (    v_student_record.STUDENT_CATCHMENT_SCHOOL
                                                IS NOT NULL
                                         AND STUDENT_CATCHMENT_SCHOOL IS NULL)
                                     OR (    v_student_record.STUDENT_CATCHMENT_SCHOOL
                                                IS NULL
                                         AND STUDENT_CATCHMENT_SCHOOL
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_GRADE_CODE <>
                                            STUDENT_CURRENT_GRADE_CODE)
                                     OR (    v_student_record.STUDENT_CURRENT_GRADE_CODE
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_GRADE_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_GRADE_CODE
                                                IS NULL
                                         AND STUDENT_CURRENT_GRADE_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_GRADE_LEVEL <>
                                            STUDENT_CURRENT_GRADE_LEVEL)
                                     OR (    v_student_record.STUDENT_CURRENT_GRADE_LEVEL
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_GRADE_LEVEL
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_GRADE_LEVEL
                                                IS NULL
                                         AND STUDENT_CURRENT_GRADE_LEVEL
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_DISTRICT_CODE <>
                                            STUDENT_CURRENT_DISTRICT_CODE)
                                     OR (    v_student_record.STUDENT_CURRENT_DISTRICT_CODE
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_DISTRICT_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_DISTRICT_CODE
                                                IS NULL
                                         AND STUDENT_CURRENT_DISTRICT_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_SCHOOL_CODE <>
                                            STUDENT_CURRENT_SCHOOL_CODE)
                                     OR (    v_student_record.STUDENT_CURRENT_SCHOOL_CODE
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_SCHOOL_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_SCHOOL_CODE
                                                IS NULL
                                         AND STUDENT_CURRENT_SCHOOL_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_SCHOOL <>
                                            STUDENT_CURRENT_SCHOOL)
                                     OR (    v_student_record.STUDENT_CURRENT_SCHOOL
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_SCHOOL IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_SCHOOL
                                                IS NULL
                                         AND STUDENT_CURRENT_SCHOOL
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_CURRENT_HOMEROOM <>
                                            STUDENT_CURRENT_HOMEROOM)
                                     OR (    v_student_record.STUDENT_CURRENT_HOMEROOM
                                                IS NOT NULL
                                         AND STUDENT_CURRENT_HOMEROOM IS NULL)
                                     OR (    v_student_record.STUDENT_CURRENT_HOMEROOM
                                                IS NULL
                                         AND STUDENT_CURRENT_HOMEROOM
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE <>
                                            STUDENT_NEXT_YEAR_GRADE_CODE)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE
                                                IS NOT NULL
                                         AND STUDENT_NEXT_YEAR_GRADE_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE
                                                IS NULL
                                         AND STUDENT_NEXT_YEAR_GRADE_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL <>
                                            STUDENT_NEXT_YEAR_GRADE_LEVEL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL
                                                IS NOT NULL
                                         AND STUDENT_NEXT_YEAR_GRADE_LEVEL
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL
                                                IS NULL
                                         AND STUDENT_NEXT_YEAR_GRADE_LEVEL
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE <>
                                            STUDENT_NEXT_YEAR_SCHOOL_CODE)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE
                                                IS NOT NULL
                                         AND STUDENT_NEXT_YEAR_SCHOOL_CODE
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE
                                                IS NULL
                                         AND STUDENT_NEXT_YEAR_SCHOOL_CODE
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NEXT_YEAR_SCHOOL <>
                                            STUDENT_NEXT_YEAR_SCHOOL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_SCHOOL
                                                IS NOT NULL
                                         AND STUDENT_NEXT_YEAR_SCHOOL IS NULL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_SCHOOL
                                                IS NULL
                                         AND STUDENT_NEXT_YEAR_SCHOOL
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_NEXT_YEAR_HOMEROOM <>
                                            STUDENT_NEXT_YEAR_HOMEROOM)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_HOMEROOM
                                                IS NOT NULL
                                         AND STUDENT_NEXT_YEAR_HOMEROOM
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_NEXT_YEAR_HOMEROOM
                                                IS NULL
                                         AND STUDENT_NEXT_YEAR_HOMEROOM
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_GRADUATION_COHORT <>
                                            STUDENT_GRADUATION_COHORT)
                                     OR (    v_student_record.STUDENT_GRADUATION_COHORT
                                                IS NOT NULL
                                         AND STUDENT_GRADUATION_COHORT
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_GRADUATION_COHORT
                                                IS NULL
                                         AND STUDENT_GRADUATION_COHORT
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_1ST_GRADE_COHORT <>
                                            STUDENT_1ST_GRADE_COHORT)
                                     OR (    v_student_record.STUDENT_1ST_GRADE_COHORT
                                                IS NOT NULL
                                         AND STUDENT_1ST_GRADE_COHORT IS NULL)
                                     OR (    v_student_record.STUDENT_1ST_GRADE_COHORT
                                                IS NULL
                                         AND STUDENT_1ST_GRADE_COHORT
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_HOMELESS_INDICATOR <>
                                            STUDENT_HOMELESS_INDICATOR)
                                     OR (    v_student_record.STUDENT_HOMELESS_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_HOMELESS_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_HOMELESS_INDICATOR
                                                IS NULL
                                         AND STUDENT_HOMELESS_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_AT_RISK_INDICATOR <>
                                            STUDENT_AT_RISK_INDICATOR)
                                     OR (    v_student_record.STUDENT_AT_RISK_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_AT_RISK_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_AT_RISK_INDICATOR
                                                IS NULL
                                         AND STUDENT_AT_RISK_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_504_INDICATOR <>
                                            STUDENT_504_INDICATOR)
                                     OR (    v_student_record.STUDENT_504_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_504_INDICATOR IS NULL)
                                     OR (    v_student_record.STUDENT_504_INDICATOR
                                                IS NULL
                                         AND STUDENT_504_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_INDIAN_ED_INDICATOR <>
                                            STUDENT_INDIAN_ED_INDICATOR)
                                     OR (    v_student_record.STUDENT_INDIAN_ED_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_INDIAN_ED_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_INDIAN_ED_INDICATOR
                                                IS NULL
                                         AND STUDENT_INDIAN_ED_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.STUDENT_MIGRANT_ED_INDICATOR <>
                                            STUDENT_MIGRANT_ED_INDICATOR)
                                     OR (    v_student_record.STUDENT_MIGRANT_ED_INDICATOR
                                                IS NOT NULL
                                         AND STUDENT_MIGRANT_ED_INDICATOR
                                                IS NULL)
                                     OR (    v_student_record.STUDENT_MIGRANT_ED_INDICATOR
                                                IS NULL
                                         AND STUDENT_MIGRANT_ED_INDICATOR
                                                IS NOT NULL))
                                 OR (   (v_student_record.DISTRICT_CODE <>
                                            DISTRICT_CODE)
                                     OR (    v_student_record.DISTRICT_CODE
                                                IS NOT NULL
                                         AND DISTRICT_CODE IS NULL)
                                     OR (    v_student_record.DISTRICT_CODE
                                                IS NULL
                                         AND DISTRICT_CODE IS NOT NULL))
                                 OR (   (v_SYS_ETL_SOURCE <> SYS_ETL_SOURCE)
                                     OR (    v_SYS_ETL_SOURCE IS NOT NULL
                                         AND SYS_ETL_SOURCE IS NULL)
                                     OR (    v_SYS_ETL_SOURCE IS NULL
                                         AND SYS_ETL_SOURCE IS NOT NULL))
                                 OR (   (v_student_record.SYS_AUDIT_IND <>
                                            SYS_AUDIT_IND)
                                     OR (    v_student_record.SYS_AUDIT_IND
                                                IS NOT NULL
                                         AND SYS_AUDIT_IND IS NULL)
                                     OR (    v_student_record.SYS_AUDIT_IND
                                                IS NULL
                                         AND SYS_AUDIT_IND IS NOT NULL))
                                 OR (   (v_student_record.SYS_PARTITION_VALUE <>
                                            SYS_PARTITION_VALUE)
                                     OR (    v_student_record.SYS_PARTITION_VALUE
                                                IS NOT NULL
                                         AND SYS_PARTITION_VALUE IS NULL)
                                     OR (    v_student_record.SYS_PARTITION_VALUE
                                                IS NULL
                                         AND SYS_PARTITION_VALUE IS NOT NULL)));

                     v_rowcnt := SQL%ROWCOUNT;
                     COMMIT;

                     IF v_rowcnt > 0
                     THEN
                        v_rowcnt :=
                           K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED (1);
                     END IF;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        --ROLLBACK;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                           v_SYS_ETL_SOURCE,
                           'INSERT/COMMIT',
                           v_WAREHOUSE_KEY,
                           v_AUDIT_NATURAL_KEY,
                           'Untrapped Error',
                           SQLERRM,
                           'Y',
                           v_AUDIT_BASE_SEVERITY);
                  END;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     BEGIN
                        -- Insert new record
                        INSERT INTO K12INTEL_DW.DTBL_STUDENTS
                           VALUES (
                                     v_student_record.STUDENT_KEY, --STUDENT_KEY
                                     v_student_record.STUDENT_ATTRIB_KEY, --STUDENT_ATTRIB_KEY
                                     v_student_record.SCHOOL_KEY, --SCHOOL_KEY
                                     v_student_record.STUDENT_ID, --STUDENT_ID
                                     v_student_record.STUDENT_NAME, --STUDENT_NAME
                                     v_student_record.STUDENT_ACTIVITY_INDICATOR, --STUDENT_ACTIVITY_INDICATOR
                                     v_student_record.STUDENT_STATUS, --STUDENT_STATUS
                                     v_student_record.STUDENT_FTE_GROUP, --STUDENT_FTE_GROUP
                                     v_student_record.STUDENT_GENDER_CODE, --STUDENT_GENDER_CODE
                                     v_student_record.STUDENT_GENDER, --STUDENT_GENDER
                                     v_student_record.STUDENT_AGE, --STUDENT_AGE
                                     v_student_record.STUDENT_RACE_CODE, --STUDENT_RACE_CODE
                                     v_student_record.STUDENT_RACE, --STUDENT_RACE
                                     v_student_record.STUDENT_ETHNICITY_CODE, --STUDENT_ETHNICITY_CODE
                                     v_student_record.STUDENT_ETHNICITY, --STUDENT_ETHNICITY
                                     v_student_record.STUDENT_LANGUAGE, --STUDENT_LANGUAGE
                                     v_student_record.STUDENT_HOME_LANGUAGE, --STUDENT_HOME_LANGUAGE
                                     v_student_record.STUDENT_COUNTRY_OF_CITIZENSHIP, --STUDENT_COUNTRY_OF_CITIZENSHIP
                                     v_student_record.STUDENT_COUNTRY_OF_BIRTH, --STUDENT_COUNTRY_OF_BIRTH
                                     v_student_record.STUDENT_FOODSERVICE_INDICATOR, --STUDENT_FOODSERVICE_INDICATOR
                                     v_student_record.STUDENT_FOODSERVICE_ELIG_CODE, --STUDENT_FOODSERVICE_ELIG_CODE
                                     v_student_record.STUDENT_FOODSERVICE_ELIG, --STUDENT_FOODSERVICE_ELIG
                                     v_student_record.STUDENT_SPECIAL_ED_INDICATOR, --STUDENT_SPECIAL_ED_INDICATOR
                                     v_student_record.STUDENT_SPECIAL_ED_CLASS, --STUDENT_SPECIAL_ED_CLASS
                                     v_student_record.STUDENT_EDUCATIONAL_EXCEPT_TYP, --STUDENT_EDUCATIONAL_EXCEPT_TYP
                                     v_student_record.STUDENT_ESL_INDICATOR, --STUDENT_ESL_INDICATOR
                                     v_student_record.STUDENT_LEP_INDICATOR, --STUDENT_LEP_INDICATOR
                                     v_student_record.STUDENT_ESL_CLASSIFICATION, --STUDENT_ESL_CLASSIFICATION
                                     v_student_record.STUDENT_GIFTED_INDICATOR, --STUDENT_GIFTED_INDICATOR
                                     v_student_record.STUDENT_CUMULATIVE_GPA, --STUDENT_CUMULATIVE_GPA
                                     v_student_record.STUDENT_CURRENT_GPA, --STUDENT_CURRENT_GPA
                                     v_student_record.WEIGHTED_CUMULATIVE_GPA, --WEIGHTED_CUMULATIVE_GPA
                                     v_student_record.WEIGHTED_CURRENT_GPA, --WEIGHTED_CURRENT_GPA
                                     v_student_record.STUDENT_ADMISSION_TYPE, --STUDENT_ADMISSION_TYPE
                                     v_student_record.STUDENT_LIVING_WITH, --STUDENT_LIVING_WITH
                                     v_student_record.STUDENT_RESIDENCE_CHANGES, --STUDENT_RESIDENCE_CHANGES
                                     v_student_record.STUDENT_SCHOOL_CHANGES, --STUDENT_SCHOOL_CHANGES
                                     v_student_record.STUDENT_CATCHMENT_CODE, --STUDENT_CATCHMENT_CODE
                                     v_student_record.STUDENT_CROSS_ENROLL_INDICATOR, --STUDENT_CROSS_ENROLL_INDICATOR
                                     v_student_record.STUDENT_OUTSIDE_CATCHMENT, --STUDENT_OUTSIDE_CATCHMENT
                                     v_student_record.STUDENT_CATCHMENT_SCHOOL, --STUDENT_CATCHMENT_SCHOOL
                                     v_student_record.STUDENT_CURRENT_GRADE_CODE, --STUDENT_CURRENT_GRADE_CODE
                                     v_student_record.STUDENT_CURRENT_GRADE_LEVEL, --STUDENT_CURRENT_GRADE_LEVEL
                                     v_student_record.STUDENT_CURRENT_DISTRICT_CODE, --STUDENT_CURRENT_DISTRICT_CODE
                                     v_student_record.STUDENT_CURRENT_SCHOOL_CODE, --STUDENT_CURRENT_SCHOOL_CODE
                                     v_student_record.STUDENT_CURRENT_SCHOOL, --STUDENT_CURRENT_SCHOOL
                                     v_student_record.STUDENT_CURRENT_HOMEROOM, --STUDENT_CURRENT_HOMEROOM
                                     v_student_record.STUDENT_NEXT_YEAR_GRADE_CODE, --STUDENT_NEXT_YEAR_GRADE_CODE
                                     v_student_record.STUDENT_NEXT_YEAR_GRADE_LEVEL, --STUDENT_NEXT_YEAR_GRADE_LEVEL
                                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL_CODE, --STUDENT_NEXT_YEAR_SCHOOL_CODE
                                     v_student_record.STUDENT_NEXT_YEAR_SCHOOL, --STUDENT_NEXT_YEAR_SCHOOL
                                     v_student_record.STUDENT_NEXT_YEAR_HOMEROOM, --STUDENT_NEXT_YEAR_HOMEROOM
                                     v_student_record.STUDENT_GRADUATION_COHORT, --STUDENT_GRADUATION_COHORT
                                     v_student_record.STUDENT_1ST_GRADE_COHORT, --STUDENT_1ST_GRADE_COHORT
                                     v_student_record.STUDENT_HOMELESS_INDICATOR, --STUDENT_HOMELESS_INDICATOR
                                     v_student_record.STUDENT_AT_RISK_INDICATOR, --STUDENT_AT_RISK_INDICATOR
                                     v_student_record.STUDENT_504_INDICATOR, --STUDENT_504_INDICATOR
                                     v_student_record.STUDENT_INDIAN_ED_INDICATOR, --STUDENT_INDIAN_ED_INDICATOR
                                     v_student_record.STUDENT_MIGRANT_ED_INDICATOR, --STUDENT_MIGRANT_ED_INDICATOR
                                     v_student_record.DISTRICT_CODE, --DISTRICT_CODE
                                     v_SYS_ETL_SOURCE,        --SYS_ETL_SOURCE
                                     SYSDATE,                   -- SYS_CREATED
                                     SYSDATE,                   -- SYS_UPDATED
                                     v_student_record.SYS_AUDIT_IND, --SYS_AUDIT_IND
                                     'N',                     -- SYS_DUMMY_IND
                                     v_student_record.SYS_PARTITION_VALUE --SYS_PARTITION_VALUE
                                                                         );

                        COMMIT;

                        v_rowcnt :=
                           K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED (1);
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           --ROLLBACK;
                           v_WAREHOUSE_KEY := 0;
                           v_AUDIT_BASE_SEVERITY := 0;

                           v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                           K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                              v_SYS_ETL_SOURCE,
                              'INSERT/COMMIT',
                              v_WAREHOUSE_KEY,
                              v_AUDIT_NATURAL_KEY,
                              'Untrapped Error',
                              SQLERRM,
                              'Y',
                              v_AUDIT_BASE_SEVERITY);
                     END;
                  WHEN OTHERS
                  THEN
                     ROLLBACK;
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'INSERT/COMMIT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
                     RAISE;
               END;


               BEGIN
                  SELECT STUDENT_KEY
                    INTO v_existing_STUDENT_KEY
                    FROM K12INTEL_DW.DTBL_STUDENT_DETAILS
                   WHERE student_key = v_student_record.student_key;

                  BEGIN
                     UPDATE K12INTEL_DW.DTBL_STUDENT_DETAILS
                        SET STUDENT_ID = v_student_record.STUDENT_ID,
                            STUDENT_STATE_ID =
                               v_details_record.STUDENT_STATE_ID,
                            STUDENT_FEDERAL_ID =
                               v_details_record.STUDENT_FEDERAL_ID,
                            STUDENT_LAST_NAME =
                               v_details_record.STUDENT_LAST_NAME,
                            STUDENT_LAST_NAME_SUFFIX =
                               v_details_record.STUDENT_LAST_NAME_SUFFIX,
                            STUDENT_FIRST_NAME =
                               v_details_record.STUDENT_FIRST_NAME,
                            STUDENT_MIDDLE_INITIAL =
                               v_details_record.STUDENT_MIDDLE_INITIAL,
                            STUDENT_MIDDLE_NAME =
                               v_details_record.STUDENT_MIDDLE_NAME,
                            STUDENT_PREFERRED_LAST_NAME =
                               v_details_record.STUDENT_PREFERRED_LAST_NAME,
                            STUDENT_PREFERRED_FIRST_NAME =
                               v_details_record.STUDENT_PREFERRED_FIRST_NAME,
                            STUDENT_BIRTHDATE =
                               v_details_record.STUDENT_BIRTHDATE,
                            STUDENT_BIRTH_YEAR =
                               v_details_record.STUDENT_BIRTH_YEAR,
                            STUDENT_BIRTH_MONTH =
                               v_details_record.STUDENT_BIRTH_MONTH,
                            STUDENT_ORIGIN_IND_ASIAN =
                               v_details_record.STUDENT_ORIGIN_IND_ASIAN,
                            STUDENT_ORIGIN_IND_BLACK =
                               v_details_record.STUDENT_ORIGIN_IND_BLACK,
                            STUDENT_ORIGIN_IND_HISPANIC =
                               v_details_record.STUDENT_ORIGIN_IND_HISPANIC,
                            STUDENT_ORIGIN_IND_NATIVE_AMER =
                               v_details_record.STUDENT_ORIGIN_IND_NATIVE_AMER,
                            STUDENT_ORIGIN_IND_PACIF_ISLND =
                               v_details_record.STUDENT_ORIGIN_IND_PACIF_ISLND,
                            STUDENT_ORIGIN_IND_WHITE =
                               v_details_record.STUDENT_ORIGIN_IND_WHITE,
                            STUDENT_TRANSPORT_STATUS =
                               v_details_record.STUDENT_TRANSPORT_STATUS,
                            STUDENT_DWELLING_TYPE =
                               v_details_record.STUDENT_DWELLING_TYPE,
                            STUDENT_MAILING_ADDRESS =
                               v_details_record.STUDENT_MAILING_ADDRESS,
                            STUDENT_STREET_NUMBER =
                               v_details_record.STUDENT_STREET_NUMBER,
                            STUDENT_STREET_DIRECTION =
                               v_details_record.STUDENT_STREET_DIRECTION,
                            STUDENT_STREET_DIRECTION_PRE =
                               v_details_record.STUDENT_STREET_DIRECTION_PRE,
                            STUDENT_STREET_DIRECTION_POST =
                               v_details_record.STUDENT_STREET_DIRECTION_POST,
                            STUDENT_STREET_NAME =
                               v_details_record.STUDENT_STREET_NAME,
                            STUDENT_STREET_TYPE =
                               v_details_record.STUDENT_STREET_TYPE,
                            STUDENT_APARTMENT =
                               v_details_record.STUDENT_APARTMENT,
                            STUDENT_CITY = v_details_record.STUDENT_CITY,
                            STUDENT_STATE_CODE =
                               v_details_record.STUDENT_STATE_CODE,
                            STUDENT_POSTAL_CODE =
                               v_details_record.STUDENT_POSTAL_CODE,
                            STUDENT_COUNTY_CODE =
                               v_details_record.STUDENT_COUNTY_CODE,
                            STUDENT_COUNTY = v_details_record.STUDENT_COUNTY,
                            STUDENT_XCOORD = v_details_record.STUDENT_XCOORD,
                            STUDENT_YCOORD = v_details_record.STUDENT_YCOORD,
                            STUDENT_NEIGHBORHOOD =
                               v_details_record.STUDENT_NEIGHBORHOOD,
                            STUDENT_PHONE = v_details_record.STUDENT_PHONE,
                            STUDENT_PHONE_RELEASE =
                               v_details_record.STUDENT_PHONE_RELEASE,
                            STUDENT_INFORMATION_RELEASE =
                               v_details_record.STUDENT_INFORMATION_RELEASE,
                            STUDENT_EMAIL = v_details_record.STUDENT_EMAIL,
                            STUDENT_COUNSELOR =
                               v_details_record.STUDENT_COUNSELOR,
                            STUDENT_GIFTED_YEAR =
                               v_details_record.STUDENT_GIFTED_YEAR,
                            STUDENT_GRADUATION_STATUS =
                               v_details_record.STUDENT_GRADUATION_STATUS,
                            STUDENT_DIPLOMA_TYPE =
                               v_details_record.STUDENT_DIPLOMA_TYPE,
                            DAYS_IN_CURRENT_DISTRICT =
                               v_details_record.DAYS_IN_CURRENT_DISTRICT,
                            DAYS_IN_CURRENT_SCHOOL =
                               v_details_record.DAYS_IN_CURRENT_SCHOOL,
                            RESIDENT_DISTRICT_CODE =
                               v_details_record.RESIDENT_DISTRICT_CODE,
                            RESIDENT_DISTRICT =
                               v_details_record.RESIDENT_DISTRICT,
                            DISTRICT_CODE = v_student_record.DISTRICT_CODE,
                            SYS_ETL_SOURCE = v_SYS_ETL_SOURCE,
                            SYS_UPDATED = SYSDATE,
                            SYS_AUDIT_IND = v_details_record.SYS_AUDIT_IND,
                            SYS_DUMMY_IND = 'N',
                            SYS_PARTITION_VALUE =
                               v_student_record.SYS_PARTITION_VALUE
                      WHERE     STUDENT_KEY = v_existing_STUDENT_KEY
                            AND (   (   (v_student_record.STUDENT_ID <>
                                            STUDENT_ID)
                                     OR (    v_student_record.STUDENT_ID
                                                IS NOT NULL
                                         AND STUDENT_ID IS NULL)
                                     OR (    v_student_record.STUDENT_ID
                                                IS NULL
                                         AND STUDENT_ID IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STATE_ID <>
                                            STUDENT_STATE_ID)
                                     OR (    v_details_record.STUDENT_STATE_ID
                                                IS NOT NULL
                                         AND STUDENT_STATE_ID IS NULL)
                                     OR (    v_details_record.STUDENT_STATE_ID
                                                IS NULL
                                         AND STUDENT_STATE_ID IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_FEDERAL_ID <>
                                            STUDENT_FEDERAL_ID)
                                     OR (    v_details_record.STUDENT_FEDERAL_ID
                                                IS NOT NULL
                                         AND STUDENT_FEDERAL_ID IS NULL)
                                     OR (    v_details_record.STUDENT_FEDERAL_ID
                                                IS NULL
                                         AND STUDENT_FEDERAL_ID IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_LAST_NAME <>
                                            STUDENT_LAST_NAME)
                                     OR (    v_details_record.STUDENT_LAST_NAME
                                                IS NOT NULL
                                         AND STUDENT_LAST_NAME IS NULL)
                                     OR (    v_details_record.STUDENT_LAST_NAME
                                                IS NULL
                                         AND STUDENT_LAST_NAME IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_LAST_NAME_SUFFIX <>
                                            STUDENT_LAST_NAME_SUFFIX)
                                     OR (    v_details_record.STUDENT_LAST_NAME_SUFFIX
                                                IS NOT NULL
                                         AND STUDENT_LAST_NAME_SUFFIX IS NULL)
                                     OR (    v_details_record.STUDENT_LAST_NAME_SUFFIX
                                                IS NULL
                                         AND STUDENT_LAST_NAME_SUFFIX
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_FIRST_NAME <>
                                            STUDENT_FIRST_NAME)
                                     OR (    v_details_record.STUDENT_FIRST_NAME
                                                IS NOT NULL
                                         AND STUDENT_FIRST_NAME IS NULL)
                                     OR (    v_details_record.STUDENT_FIRST_NAME
                                                IS NULL
                                         AND STUDENT_FIRST_NAME IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_MIDDLE_INITIAL <>
                                            STUDENT_MIDDLE_INITIAL)
                                     OR (    v_details_record.STUDENT_MIDDLE_INITIAL
                                                IS NOT NULL
                                         AND STUDENT_MIDDLE_INITIAL IS NULL)
                                     OR (    v_details_record.STUDENT_MIDDLE_INITIAL
                                                IS NULL
                                         AND STUDENT_MIDDLE_INITIAL
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_MIDDLE_NAME <>
                                            STUDENT_MIDDLE_NAME)
                                     OR (    v_details_record.STUDENT_MIDDLE_NAME
                                                IS NOT NULL
                                         AND STUDENT_MIDDLE_NAME IS NULL)
                                     OR (    v_details_record.STUDENT_MIDDLE_NAME
                                                IS NULL
                                         AND STUDENT_MIDDLE_NAME IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_PREFERRED_LAST_NAME <>
                                            STUDENT_PREFERRED_LAST_NAME)
                                     OR (    v_details_record.STUDENT_PREFERRED_LAST_NAME
                                                IS NOT NULL
                                         AND STUDENT_PREFERRED_LAST_NAME
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_PREFERRED_LAST_NAME
                                                IS NULL
                                         AND STUDENT_PREFERRED_LAST_NAME
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_PREFERRED_FIRST_NAME <>
                                            STUDENT_PREFERRED_FIRST_NAME)
                                     OR (    v_details_record.STUDENT_PREFERRED_FIRST_NAME
                                                IS NOT NULL
                                         AND STUDENT_PREFERRED_FIRST_NAME
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_PREFERRED_FIRST_NAME
                                                IS NULL
                                         AND STUDENT_PREFERRED_FIRST_NAME
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_BIRTHDATE <>
                                            STUDENT_BIRTHDATE)
                                     OR (    v_details_record.STUDENT_BIRTHDATE
                                                IS NOT NULL
                                         AND STUDENT_BIRTHDATE IS NULL)
                                     OR (    v_details_record.STUDENT_BIRTHDATE
                                                IS NULL
                                         AND STUDENT_BIRTHDATE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_BIRTH_YEAR <>
                                            STUDENT_BIRTH_YEAR)
                                     OR (    v_details_record.STUDENT_BIRTH_YEAR
                                                IS NOT NULL
                                         AND STUDENT_BIRTH_YEAR IS NULL)
                                     OR (    v_details_record.STUDENT_BIRTH_YEAR
                                                IS NULL
                                         AND STUDENT_BIRTH_YEAR IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_BIRTH_MONTH <>
                                            STUDENT_BIRTH_MONTH)
                                     OR (    v_details_record.STUDENT_BIRTH_MONTH
                                                IS NOT NULL
                                         AND STUDENT_BIRTH_MONTH IS NULL)
                                     OR (    v_details_record.STUDENT_BIRTH_MONTH
                                                IS NULL
                                         AND STUDENT_BIRTH_MONTH IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_ASIAN <>
                                            STUDENT_ORIGIN_IND_ASIAN)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_ASIAN
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_ASIAN IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_ASIAN
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_ASIAN
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_BLACK <>
                                            STUDENT_ORIGIN_IND_BLACK)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_BLACK
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_BLACK IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_BLACK
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_BLACK
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_HISPANIC <>
                                            STUDENT_ORIGIN_IND_HISPANIC)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_HISPANIC
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_HISPANIC
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_HISPANIC
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_HISPANIC
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_NATIVE_AMER <>
                                            STUDENT_ORIGIN_IND_NATIVE_AMER)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_NATIVE_AMER
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_NATIVE_AMER
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_NATIVE_AMER
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_NATIVE_AMER
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_PACIF_ISLND <>
                                            STUDENT_ORIGIN_IND_PACIF_ISLND)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_PACIF_ISLND
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_PACIF_ISLND
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_PACIF_ISLND
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_PACIF_ISLND
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_ORIGIN_IND_WHITE <>
                                            STUDENT_ORIGIN_IND_WHITE)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_WHITE
                                                IS NOT NULL
                                         AND STUDENT_ORIGIN_IND_WHITE IS NULL)
                                     OR (    v_details_record.STUDENT_ORIGIN_IND_WHITE
                                                IS NULL
                                         AND STUDENT_ORIGIN_IND_WHITE
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_TRANSPORT_STATUS <>
                                            STUDENT_TRANSPORT_STATUS)
                                     OR (    v_details_record.STUDENT_TRANSPORT_STATUS
                                                IS NOT NULL
                                         AND STUDENT_TRANSPORT_STATUS IS NULL)
                                     OR (    v_details_record.STUDENT_TRANSPORT_STATUS
                                                IS NULL
                                         AND STUDENT_TRANSPORT_STATUS
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_DWELLING_TYPE <>
                                            STUDENT_DWELLING_TYPE)
                                     OR (    v_details_record.STUDENT_DWELLING_TYPE
                                                IS NOT NULL
                                         AND STUDENT_DWELLING_TYPE IS NULL)
                                     OR (    v_details_record.STUDENT_DWELLING_TYPE
                                                IS NULL
                                         AND STUDENT_DWELLING_TYPE
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_MAILING_ADDRESS <>
                                            STUDENT_MAILING_ADDRESS)
                                     OR (    v_details_record.STUDENT_MAILING_ADDRESS
                                                IS NOT NULL
                                         AND STUDENT_MAILING_ADDRESS IS NULL)
                                     OR (    v_details_record.STUDENT_MAILING_ADDRESS
                                                IS NULL
                                         AND STUDENT_MAILING_ADDRESS
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_NUMBER <>
                                            STUDENT_STREET_NUMBER)
                                     OR (    v_details_record.STUDENT_STREET_NUMBER
                                                IS NOT NULL
                                         AND STUDENT_STREET_NUMBER IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_NUMBER
                                                IS NULL
                                         AND STUDENT_STREET_NUMBER
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_DIRECTION <>
                                            STUDENT_STREET_DIRECTION)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION
                                                IS NOT NULL
                                         AND STUDENT_STREET_DIRECTION IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION
                                                IS NULL
                                         AND STUDENT_STREET_DIRECTION
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_DIRECTION_PRE <>
                                            STUDENT_STREET_DIRECTION_PRE)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION_PRE
                                                IS NOT NULL
                                         AND STUDENT_STREET_DIRECTION_PRE
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION_PRE
                                                IS NULL
                                         AND STUDENT_STREET_DIRECTION_PRE
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_DIRECTION_POST <>
                                            STUDENT_STREET_DIRECTION_POST)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION_POST
                                                IS NOT NULL
                                         AND STUDENT_STREET_DIRECTION_POST
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_DIRECTION_POST
                                                IS NULL
                                         AND STUDENT_STREET_DIRECTION_POST
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_NAME <>
                                            STUDENT_STREET_NAME)
                                     OR (    v_details_record.STUDENT_STREET_NAME
                                                IS NOT NULL
                                         AND STUDENT_STREET_NAME IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_NAME
                                                IS NULL
                                         AND STUDENT_STREET_NAME IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STREET_TYPE <>
                                            STUDENT_STREET_TYPE)
                                     OR (    v_details_record.STUDENT_STREET_TYPE
                                                IS NOT NULL
                                         AND STUDENT_STREET_TYPE IS NULL)
                                     OR (    v_details_record.STUDENT_STREET_TYPE
                                                IS NULL
                                         AND STUDENT_STREET_TYPE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_APARTMENT <>
                                            STUDENT_APARTMENT)
                                     OR (    v_details_record.STUDENT_APARTMENT
                                                IS NOT NULL
                                         AND STUDENT_APARTMENT IS NULL)
                                     OR (    v_details_record.STUDENT_APARTMENT
                                                IS NULL
                                         AND STUDENT_APARTMENT IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_CITY <>
                                            STUDENT_CITY)
                                     OR (    v_details_record.STUDENT_CITY
                                                IS NOT NULL
                                         AND STUDENT_CITY IS NULL)
                                     OR (    v_details_record.STUDENT_CITY
                                                IS NULL
                                         AND STUDENT_CITY IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_STATE_CODE <>
                                            STUDENT_STATE_CODE)
                                     OR (    v_details_record.STUDENT_STATE_CODE
                                                IS NOT NULL
                                         AND STUDENT_STATE_CODE IS NULL)
                                     OR (    v_details_record.STUDENT_STATE_CODE
                                                IS NULL
                                         AND STUDENT_STATE_CODE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_POSTAL_CODE <>
                                            STUDENT_POSTAL_CODE)
                                     OR (    v_details_record.STUDENT_POSTAL_CODE
                                                IS NOT NULL
                                         AND STUDENT_POSTAL_CODE IS NULL)
                                     OR (    v_details_record.STUDENT_POSTAL_CODE
                                                IS NULL
                                         AND STUDENT_POSTAL_CODE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_COUNTY_CODE <>
                                            STUDENT_COUNTY_CODE)
                                     OR (    v_details_record.STUDENT_COUNTY_CODE
                                                IS NOT NULL
                                         AND STUDENT_COUNTY_CODE IS NULL)
                                     OR (    v_details_record.STUDENT_COUNTY_CODE
                                                IS NULL
                                         AND STUDENT_COUNTY_CODE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_COUNTY <>
                                            STUDENT_COUNTY)
                                     OR (    v_details_record.STUDENT_COUNTY
                                                IS NOT NULL
                                         AND STUDENT_COUNTY IS NULL)
                                     OR (    v_details_record.STUDENT_COUNTY
                                                IS NULL
                                         AND STUDENT_COUNTY IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_XCOORD <>
                                            STUDENT_XCOORD)
                                     OR (    v_details_record.STUDENT_XCOORD
                                                IS NOT NULL
                                         AND STUDENT_XCOORD IS NULL)
                                     OR (    v_details_record.STUDENT_XCOORD
                                                IS NULL
                                         AND STUDENT_XCOORD IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_YCOORD <>
                                            STUDENT_YCOORD)
                                     OR (    v_details_record.STUDENT_YCOORD
                                                IS NOT NULL
                                         AND STUDENT_YCOORD IS NULL)
                                     OR (    v_details_record.STUDENT_YCOORD
                                                IS NULL
                                         AND STUDENT_YCOORD IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_NEIGHBORHOOD <>
                                            STUDENT_NEIGHBORHOOD)
                                     OR (    v_details_record.STUDENT_NEIGHBORHOOD
                                                IS NOT NULL
                                         AND STUDENT_NEIGHBORHOOD IS NULL)
                                     OR (    v_details_record.STUDENT_NEIGHBORHOOD
                                                IS NULL
                                         AND STUDENT_NEIGHBORHOOD IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_PHONE <>
                                            STUDENT_PHONE)
                                     OR (    v_details_record.STUDENT_PHONE
                                                IS NOT NULL
                                         AND STUDENT_PHONE IS NULL)
                                     OR (    v_details_record.STUDENT_PHONE
                                                IS NULL
                                         AND STUDENT_PHONE IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_PHONE_RELEASE <>
                                            STUDENT_PHONE_RELEASE)
                                     OR (    v_details_record.STUDENT_PHONE_RELEASE
                                                IS NOT NULL
                                         AND STUDENT_PHONE_RELEASE IS NULL)
                                     OR (    v_details_record.STUDENT_PHONE_RELEASE
                                                IS NULL
                                         AND STUDENT_PHONE_RELEASE
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_INFORMATION_RELEASE <>
                                            STUDENT_INFORMATION_RELEASE)
                                     OR (    v_details_record.STUDENT_INFORMATION_RELEASE
                                                IS NOT NULL
                                         AND STUDENT_INFORMATION_RELEASE
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_INFORMATION_RELEASE
                                                IS NULL
                                         AND STUDENT_INFORMATION_RELEASE
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_EMAIL <>
                                            STUDENT_EMAIL)
                                     OR (    v_details_record.STUDENT_EMAIL
                                                IS NOT NULL
                                         AND STUDENT_EMAIL IS NULL)
                                     OR (    v_details_record.STUDENT_EMAIL
                                                IS NULL
                                         AND STUDENT_EMAIL IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_COUNSELOR <>
                                            STUDENT_COUNSELOR)
                                     OR (    v_details_record.STUDENT_COUNSELOR
                                                IS NOT NULL
                                         AND STUDENT_COUNSELOR IS NULL)
                                     OR (    v_details_record.STUDENT_COUNSELOR
                                                IS NULL
                                         AND STUDENT_COUNSELOR IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_GIFTED_YEAR <>
                                            STUDENT_GIFTED_YEAR)
                                     OR (    v_details_record.STUDENT_GIFTED_YEAR
                                                IS NOT NULL
                                         AND STUDENT_GIFTED_YEAR IS NULL)
                                     OR (    v_details_record.STUDENT_GIFTED_YEAR
                                                IS NULL
                                         AND STUDENT_GIFTED_YEAR IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_GRADUATION_STATUS <>
                                            STUDENT_GRADUATION_STATUS)
                                     OR (    v_details_record.STUDENT_GRADUATION_STATUS
                                                IS NOT NULL
                                         AND STUDENT_GRADUATION_STATUS
                                                IS NULL)
                                     OR (    v_details_record.STUDENT_GRADUATION_STATUS
                                                IS NULL
                                         AND STUDENT_GRADUATION_STATUS
                                                IS NOT NULL))
                                 OR (   (v_details_record.STUDENT_DIPLOMA_TYPE <>
                                            STUDENT_DIPLOMA_TYPE)
                                     OR (    v_details_record.STUDENT_DIPLOMA_TYPE
                                                IS NOT NULL
                                         AND STUDENT_DIPLOMA_TYPE IS NULL)
                                     OR (    v_details_record.STUDENT_DIPLOMA_TYPE
                                                IS NULL
                                         AND STUDENT_DIPLOMA_TYPE IS NOT NULL))
                                 OR (   (v_details_record.DAYS_IN_CURRENT_DISTRICT <>
                                            DAYS_IN_CURRENT_DISTRICT)
                                     OR (    v_details_record.DAYS_IN_CURRENT_DISTRICT
                                                IS NOT NULL
                                         AND DAYS_IN_CURRENT_DISTRICT IS NULL)
                                     OR (    v_details_record.DAYS_IN_CURRENT_DISTRICT
                                                IS NULL
                                         AND DAYS_IN_CURRENT_DISTRICT
                                                IS NOT NULL))
                                 OR (   (v_details_record.DAYS_IN_CURRENT_SCHOOL <>
                                            DAYS_IN_CURRENT_SCHOOL)
                                     OR (    v_details_record.DAYS_IN_CURRENT_SCHOOL
                                                IS NOT NULL
                                         AND DAYS_IN_CURRENT_SCHOOL IS NULL)
                                     OR (    v_details_record.DAYS_IN_CURRENT_SCHOOL
                                                IS NULL
                                         AND DAYS_IN_CURRENT_SCHOOL
                                                IS NOT NULL))
                                 OR (   (v_details_record.RESIDENT_DISTRICT_CODE <>
                                            RESIDENT_DISTRICT_CODE)
                                     OR (    v_details_record.RESIDENT_DISTRICT_CODE
                                                IS NOT NULL
                                         AND RESIDENT_DISTRICT_CODE IS NULL)
                                     OR (    v_details_record.RESIDENT_DISTRICT_CODE
                                                IS NULL
                                         AND RESIDENT_DISTRICT_CODE
                                                IS NOT NULL))
                                 OR (   (v_details_record.RESIDENT_DISTRICT <>
                                            RESIDENT_DISTRICT)
                                     OR (    v_details_record.RESIDENT_DISTRICT
                                                IS NOT NULL
                                         AND RESIDENT_DISTRICT IS NULL)
                                     OR (    v_details_record.RESIDENT_DISTRICT
                                                IS NULL
                                         AND RESIDENT_DISTRICT IS NOT NULL))
                                 OR (   (v_student_record.DISTRICT_CODE <>
                                            DISTRICT_CODE)
                                     OR (    v_student_record.DISTRICT_CODE
                                                IS NOT NULL
                                         AND DISTRICT_CODE IS NULL)
                                     OR (    v_student_record.DISTRICT_CODE
                                                IS NULL
                                         AND DISTRICT_CODE IS NOT NULL))
                                 OR (   (v_SYS_ETL_SOURCE <> SYS_ETL_SOURCE)
                                     OR (    v_SYS_ETL_SOURCE IS NOT NULL
                                         AND SYS_ETL_SOURCE IS NULL)
                                     OR (    v_SYS_ETL_SOURCE IS NULL
                                         AND SYS_ETL_SOURCE IS NOT NULL))
                                 OR (   (v_details_record.SYS_AUDIT_IND <>
                                            SYS_AUDIT_IND)
                                     OR (    v_details_record.SYS_AUDIT_IND
                                                IS NOT NULL
                                         AND SYS_AUDIT_IND IS NULL)
                                     OR (    v_details_record.SYS_AUDIT_IND
                                                IS NULL
                                         AND SYS_AUDIT_IND IS NOT NULL))
                                 OR (   (v_student_record.SYS_PARTITION_VALUE <>
                                            SYS_PARTITION_VALUE)
                                     OR (    v_student_record.SYS_PARTITION_VALUE
                                                IS NOT NULL
                                         AND SYS_PARTITION_VALUE IS NULL)
                                     OR (    v_student_record.SYS_PARTITION_VALUE
                                                IS NULL
                                         AND SYS_PARTITION_VALUE IS NOT NULL)));

                     v_rowcnt := SQL%ROWCOUNT;
                     COMMIT;

                     IF v_rowcnt > 0
                     THEN
                        v_rowcnt :=
                           K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED (1);
                     END IF;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        ROLLBACK;
                        v_WAREHOUSE_KEY := 0;
                        v_AUDIT_BASE_SEVERITY := 0;

                        v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                        K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                           v_SYS_ETL_SOURCE,
                           'INSERT/COMMIT STUDENT DETAILS',
                           v_WAREHOUSE_KEY,
                           v_AUDIT_NATURAL_KEY,
                           'Untrapped Error',
                           SQLERRM,
                           'Y',
                           v_AUDIT_BASE_SEVERITY);
                  END;
               --     select count(*) into v_rowcnt from k12intel_dw.dtbl_students;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     BEGIN
                        -- Insert new record
                        INSERT INTO K12INTEL_DW.DTBL_STUDENT_DETAILS
                           VALUES (
                                     v_student_record.STUDENT_KEY,
                                     v_student_record.STUDENT_ID,
                                     v_details_record.STUDENT_STATE_ID,
                                     v_details_record.STUDENT_FEDERAL_ID,
                                     v_details_record.STUDENT_LAST_NAME,
                                     v_details_record.STUDENT_LAST_NAME_SUFFIX,
                                     v_details_record.STUDENT_FIRST_NAME,
                                     v_details_record.STUDENT_MIDDLE_INITIAL,
                                     v_details_record.STUDENT_MIDDLE_NAME,
                                     v_details_record.STUDENT_PREFERRED_LAST_NAME,
                                     v_details_record.STUDENT_PREFERRED_FIRST_NAME,
                                     v_details_record.STUDENT_BIRTHDATE,
                                     v_details_record.STUDENT_BIRTH_YEAR,
                                     v_details_record.STUDENT_BIRTH_MONTH,
                                     v_details_record.STUDENT_ORIGIN_IND_ASIAN,
                                     v_details_record.STUDENT_ORIGIN_IND_BLACK,
                                     v_details_record.STUDENT_ORIGIN_IND_HISPANIC,
                                     v_details_record.STUDENT_ORIGIN_IND_NATIVE_AMER,
                                     v_details_record.STUDENT_ORIGIN_IND_PACIF_ISLND,
                                     v_details_record.STUDENT_ORIGIN_IND_WHITE,
                                     v_details_record.STUDENT_TRANSPORT_STATUS,
                                     v_details_record.STUDENT_DWELLING_TYPE,
                                     v_details_record.STUDENT_MAILING_ADDRESS,
                                     v_details_record.STUDENT_STREET_NUMBER,
                                     v_details_record.STUDENT_STREET_DIRECTION,
                                     v_details_record.STUDENT_STREET_DIRECTION_PRE,
                                     v_details_record.STUDENT_STREET_DIRECTION_POST,
                                     v_details_record.STUDENT_STREET_NAME,
                                     v_details_record.STUDENT_STREET_TYPE,
                                     v_details_record.STUDENT_APARTMENT,
                                     v_details_record.STUDENT_CITY,
                                     v_details_record.STUDENT_STATE_CODE,
                                     v_details_record.STUDENT_POSTAL_CODE,
                                     v_details_record.STUDENT_COUNTY_CODE,
                                     v_details_record.STUDENT_COUNTY,
                                     v_details_record.STUDENT_XCOORD,
                                     v_details_record.STUDENT_YCOORD,
                                     v_details_record.STUDENT_NEIGHBORHOOD,
                                     v_details_record.STUDENT_PHONE,
                                     v_details_record.STUDENT_PHONE_RELEASE,
                                     v_details_record.STUDENT_INFORMATION_RELEASE,
                                     v_details_record.STUDENT_EMAIL,
                                     v_details_record.STUDENT_COUNSELOR,
                                     v_details_record.STUDENT_GIFTED_YEAR,
                                     v_details_record.STUDENT_GRADUATION_STATUS,
                                     v_details_record.STUDENT_DIPLOMA_TYPE,
                                     v_details_record.DAYS_IN_CURRENT_DISTRICT,
                                     v_details_record.DAYS_IN_CURRENT_SCHOOL,
                                     v_details_record.RESIDENT_DISTRICT_CODE,
                                     v_details_record.RESIDENT_DISTRICT,
                                     v_student_record.DISTRICT_CODE,
                                     v_SYS_ETL_SOURCE,        --SYS_ETL_SOURCE
                                     SYSDATE,                   -- SYS_CREATED
                                     SYSDATE,                   -- SYS_UPDATED
                                     v_details_record.SYS_AUDIT_IND, --SYS_AUDIT_IND
                                     'N',                     -- SYS_DUMMY_IND
                                     v_student_record.SYS_PARTITION_VALUE --SYS_PARTITION_VALUE
                                                                         );

                        COMMIT;
                        v_rowcnt :=
                           K12INTEL_AUTOMATION_PKG.INC_ROWS_INSERTED (1);
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           --ROLLBACK;
                           v_WAREHOUSE_KEY := 0;
                           v_AUDIT_BASE_SEVERITY := 0;

                           v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                           K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                              v_SYS_ETL_SOURCE,
                              'INSERT/COMMIT STUDENT DETAILS',
                              v_WAREHOUSE_KEY,
                              v_AUDIT_NATURAL_KEY,
                              'Untrapped Error',
                              SQLERRM,
                              'Y',
                              v_AUDIT_BASE_SEVERITY);
                     END;
                  WHEN OTHERS
                  THEN
                     v_WAREHOUSE_KEY := 0;
                     v_AUDIT_BASE_SEVERITY := 0;

                     v_AUDIT_NATURAL_KEY := v_BASE_NATURALKEY_TXT;

                     K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                        v_SYS_ETL_SOURCE,
                        'INSERT/COMMIT',
                        v_WAREHOUSE_KEY,
                        v_AUDIT_NATURAL_KEY,
                        'Untrapped Error',
                        SQLERRM,
                        'Y',
                        v_AUDIT_BASE_SEVERITY);
                     RAISE;
               END;

               v_rowcnt := K12INTEL_AUTOMATION_PKG.INC_ROWS_PROCESSED (1);

               -- Update the stats every 1000 records
               --delete from k12intel_dw.address where personid <> -111;
               IF MOD (v_rowcnt,
                       1000) = 0
               THEN
                  -- Write task stats
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
               END IF;

               COMMIT;
            END;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_WAREHOUSE_KEY := 0;
               v_AUDIT_BASE_SEVERITY := 0;

               v_AUDIT_NATURAL_KEY := '';

               K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
                  v_SYS_ETL_SOURCE,
                  'STUDENT LOOP',
                  v_WAREHOUSE_KEY,
                  v_AUDIT_NATURAL_KEY,
                  'Untrapped Error',
                  SQLERRM,
                  'Y',
                  v_AUDIT_BASE_SEVERITY);
         END;
      END LOOP;
   EXCEPTION                                         -- Loop expection handler
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;

         v_AUDIT_NATURAL_KEY := '';

         K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
            v_SYS_ETL_SOURCE,
            'CURSOR LOOP',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'Untrapped Error',
            SQLERRM,
            'Y',
            v_AUDIT_BASE_SEVERITY);
   END;

   BEGIN
      UPDATE k12intel_dw.dtbl_students a
         SET student_activity_indicator = 'Deleted',
             student_status = 'Deleted'
       WHERE     sys_etl_source = v_SYS_ETL_SOURCE
             AND NOT EXISTS
                        (SELECT NULL
                           FROM k12intel_staging_ic.person b
                          WHERE     a.student_id = b.STUDENTNUMBER
                                AND b.stage_source = p_PARAM_STAGE_SOURCE)
             AND (   student_activity_indicator <> 'Deleted'
                  OR student_status <> 'Deleted');

      v_rowcnt := SQL%ROWCOUNT;
      v_rowcnt := K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED (v_rowcnt);
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         v_WAREHOUSE_KEY := 0;
         v_AUDIT_BASE_SEVERITY := 0;

         v_AUDIT_NATURAL_KEY := '';

         K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
            v_SYS_ETL_SOURCE,
            'DELETED STUDENTS',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'Untrapped Error',
            SQLERRM,
            'Y',
            v_AUDIT_BASE_SEVERITY);
   END;

   /*
                BEGIN
                    update k12intel_dw.dtbl_students
                    set student_activity_indicator = 'Inactive',
                        student_status = 'Inactive'
                    where sys_etl_source = v_SYS_ETL_SOURCE
            --            and exists(
            --                select null
            --                from k12intel_Staging_ic.PERSON a
            --                where not exists(
            --                            select null
            --                            from K12INTEL_STAGING_IC.Enrollment b
            --                            where b.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
            --                                --ignore future enrollments
            --                                and b.stateExclude = 0
            --                                and b.startDate <= v_local_data_date
            --                                and b.stage_deleteflag = 0
            --                                and a.PERSONID = b.personid
            --                                and a.STAGE_SOURCE = b.stage_source
            --                          )
            --                    and a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
            --                    and dtbl_students.student_id = a.STUDENTNUMBER
            --             )
   --                     and exists(
   --                         select null
   --                         from k12intel_Staging_ic.PERSON a
   --                         WHERE a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
   --                             and dtbl_students.student_id = a.STUDENTNUMBER
   --                     )
   --                     and not exists(
   --                         select null
   --                         from k12intel_Staging_ic.PERSON a
   --                         inner join K12INTEL_STAGING_IC.Enrollment b
   --                         on a.PERSONID = b.personid and a.STAGE_SOURCE = b.STAGE_SOURCE
   --                         WHERE 1=1
   --                             and b.stateExclude = 0
   --                             and b.startDate <= v_local_data_date
   --                             and b.stage_deleteflag = 0
   --                             and a.STAGE_SOURCE = p_PARAM_STAGE_SOURCE
   --                             and dtbl_students.student_id = a.STUDENTNUMBER
   --                      )
   --                      and
   --                      (
   --                         student_activity_indicator <> 'Inactive' or student_status <> 'Inactive'
   --                      );
   --
   --                 v_rowcnt := SQL%ROWCOUNT;
   --                 v_rowcnt := K12INTEL_AUTOMATION_PKG.INC_ROWS_UPDATED(v_rowcnt);
   --                 COMMIT;
   --             EXCEPTION
   --                 WHEN OTHERS THEN
   --                      v_WAREHOUSE_KEY := 0;
   --                      v_AUDIT_BASE_SEVERITY := 0;
   --
   --                      v_AUDIT_NATURAL_KEY := '';
   --
   --                      K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT(
   --                          v_SYS_ETL_SOURCE,
   --                          'INACTIVE STUDENTS UPDATE',
   --                          v_WAREHOUSE_KEY,
   --                          v_AUDIT_NATURAL_KEY,
   --                          'Untrapped Error',
   --                          sqlerrm,
   --                          'Y',
   --                          v_AUDIT_BASE_SEVERITY
   --                      );
   --             END;*/

   DBMS_OUTPUT.PUT_LINE (TRUNC ( (SYSDATE - v_start_time) * 24 * 60 * 60));

   -- Write task stats

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
            'WRITE TASK STAT',
            v_WAREHOUSE_KEY,
            v_AUDIT_NATURAL_KEY,
            'Untrapped Error',
            SQLERRM,
            'Y',
            v_AUDIT_BASE_SEVERITY);
   END;

   --   EXCEPTION
   --   WHEN OTHERS
   --   THEN
   --      v_WAREHOUSE_KEY := 0;
   --      v_AUDIT_BASE_SEVERITY := 0;
   --
   --      v_AUDIT_NATURAL_KEY := '';
   --
   --      K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
   --         v_SYS_ETL_SOURCE,
   --         'TOTAL BUILD FAILURE!',
   --         v_WAREHOUSE_KEY,
   --         v_AUDIT_NATURAL_KEY,
   --         'Untrapped Error',
   --         SQLERRM,
   --         'Y',
   --         v_AUDIT_BASE_SEVERITY);

   -- Write task stats

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
END;
/
