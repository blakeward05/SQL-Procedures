DECLARE
   p_PARAM_STAGE_SOURCE          VARCHAR2(50) := 'MPS_IC';
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
          AND    build_method = 'REFRESH'
          ) 
             
   LOOP
      BEGIN
          DBMS_OUTPUT.PUT_LINE(YEARREC.SCOPE_YEAR || ' Beginning Loop with Year' || sysdate);
             
          --MOVED DELETE ABOVE INREC LOOP TO DELETE ALL YEAR RECORDS AT ONCE BW 12/17/15      
         FOR inrec
            IN (
                 SELECT distinct c.calendarid, c.name, cs.value, sch.school_key
                FROM
                    k12intel_staging_ic.day d
                    JOIN k12intel_staging_ic.periodschedule ps ON d.periodScheduleID = ps.periodScheduleID and ps.stage_source = d.stage_source
                    JOIN k12intel_staging_ic.period p on p.periodscheduleid = ps.periodscheduleid and ps.stage_source = p.stage_source
                    --ADDING IN ENDYEAR ON CALENDAR JOIN TO JUST GET CALENDARS FOR SCOPE YEAR BW 12/17/15
                    JOIN k12intel_staging_ic.calendar c ON c.calendarid = d.calendarid and c.stage_source = p.stage_source and to_char(c.endyear) = 2016
                    JOIN k12intel_staging_ic.school s on s.schoolid = c.schoolid and s.stage_deleteflag = 0 and s.stage_source = c.stage_source
                    JOIN K12intel_STAGING_IC.CustomSchool CS  on cs.attributeid = 634 AND CS.STAGE_SOURCE= s.STAGE_SOURCE
                        AND CS.STAGE_DELETEFLAG = 0 and CS.SchoolId = S.SchoolID  AND CS.STAGE_SIS_SCHOOL_YEAR = S.STAGE_SIS_SCHOOL_YEAR
                    join k12intel_dw.dtbl_schools sch on sch.school_code = cs.value 
                WHERE 1=1
                    and p.name in ('01', '02', '03', '04', '05', '06', '07')
                    and c.calendarid IN (3414)
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
                                            dtbl_school_dates.school_key
                                            =inrec.school_key
                                            -- One student for testing
                                            and mpsf_period_attendance.student_key
                                            = 311963
                                            );

                COMMIT;
            
               DBMS_OUTPUT.PUT_LINE(YEARREC.SCOPE_YEAR || ' Beginning Loop with Calendar' || inrec.name);
               
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
                                          --NO LONGER NEED NOT EXISTS BECAUSE JUST PROCESSING CERTAIN CALENDARS NOT WHOLE SCHOOL BW 12/17
--                                                 NOT EXISTS
--                                                    ( SELECT
--                                                         1
--                                                   FROM
--                                                         k12intel_staging_ic.day d1
--                                                         JOIN
--                                                         k12intel_staging_ic.periodschedule ps1
--                                                            ON d1.periodScheduleID =
--                                                                  ps1.periodScheduleID
--                                                         JOIN
--                                                         k12intel_staging_ic.period p1
--                                                            ON p1.periodscheduleid =
--                                                                  ps1.periodscheduleid
--                                                         JOIN
--                                                         k12intel_staging_ic.calendar c1
--                                                            ON c1.calendarid =
--                                                                  d1.calendarid
--                                                   WHERE
--                                                         p1.name IN ('AM',
--                                                                     'PM')
--                                                  AND    c1.calendarid =
--                                                            c.calendarid)
                                                            ) sd
                                     ON ds.school_key = sd.school_key
                            WHERE
                                  1 = 1
                           AND    sd.local_school_year = yearrec.scope_year
                           AND    sd.local_enroll_day = 1
                           --AND    ss.school_key = inrec.school_key
                           --DON'T NEED JOIN ON SCHOOL_KEY BECAUSE USING CALENDARID ABOVE TO PROCESS ONE CALENDAR BW 12/17/15
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
                          AND dst.student_id = '8806795'
                               );
            END;

            v_STAT_ROWS_PROCESSED := v_STAT_ROWS_PROCESSED + SQL%ROWCOUNT;
            DBMS_OUTPUT.PUT_LINE(v_STAT_ROWS_PROCESSED || 'Rows Processed');

--            BEGIN
--               K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_TASK_STATS;
--            EXCEPTION
--               WHEN OTHERS
--               THEN
--                  v_WAREHOUSE_KEY := 0;
--                  v_AUDIT_BASE_SEVERITY := 0;
--                  v_AUDIT_NATURAL_KEY := '';
--
--                  K12INTEL_METADATA.K12INTEL_AUTOMATION_PKG.WRITE_AUDIT (
--                     v_SYS_ETL_SOURCE,
--                     'WRITE TASK STATS',
--                     v_WAREHOUSE_KEY,
--                     v_AUDIT_NATURAL_KEY,
--                     'Untrapped Error',
--                     SQLERRM,
--                     'Y',
--                     v_AUDIT_BASE_SEVERITY);
--            END;

            COMMIT;
         END LOOP;
      END;
   END LOOP;
END;