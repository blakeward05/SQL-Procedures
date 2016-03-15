select
            marks.student_key,
            risk_factor_key,
            marks.student_risk_identified_date,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 'No' else 'Yes' end credits_on_track,
            case when nvl(ct.RISK_LEVEL, 'Low') = 'Low' then 0 else 1 end credits_on_track,
            total_credits_earned,
            total_credits_attempted,
            total_credits_earned_tot,
            ct.high_value,
            nvl(ct.RISK_LEVEL, 'Low'),
            NULL,
            marks.risk_factor_effective_end,
            case ct.RISK_LEVEL when 'High' then 2 when 'Moderate' then 1 else 0 end,
            marks.risk_factor_effective_end - marks.student_risk_identified_date,
            drsk.RISK_FACTOR_DEFAULT_ACTION
        from (
            select sart.student_key, sart.STUDENT_RISK_REPORT_TEXT mark_period
                ,max(sart.STUDENT_RISK_IDENTIFIED_DATE) student_risk_identified_date
                ,max(sart.RISK_FACTOR_EFFECTIVE_END) risk_factor_effective_end
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned
                ,sum(case when sub.DATE_VALUE <= sart.STUDENT_RISK_IDENTIFIED_DATE then sub.MARK_CREDIT_VALUE_ATTEMPTED else null end) total_credits_attempted
                ,sum(case when sub.DATE_VALUE <= sart.RISK_FACTOR_EFFECTIVE_END then sub.MARK_CREDIT_VALUE_EARNED else null end) total_credits_earned_tot
            from k12intel_staging.STUDENTS_AT_RISK_TEMP sart
            left join (
                select fmrk.student_key, dsdt.date_value, sum(mark_credit_value_earned) mark_credit_value_earned,  sum(mark_credit_value_attempted) mark_credit_value_attempted
                from
                    K12INTEL_DW.ftbl_student_marks fmrk
                    inner join k12intel_dw.dtbl_school_dates dsdt
                        on fmrk.SCHOOL_DATES_KEY = dsdt.SCHOOL_DATES_KEY
                    inner join k12intel_dw.dtbl_students dstu
                        on fmrk.STUDENT_KEY = dstu.student_key
                    inner join k12intel_dw.dtbl_courses c
                        on fmrk.course_key = c.course_key
                    inner join k12intel_dw.dtbl_scales s
                        on fmrk.scale_key = s.scale_key
                 where 1=1
                   and dstu.STUDENT_CURRENT_GRADE_CODE in ('09','10','11','12')
                    and dstu.STUDENT_ACTIVITY_INDICATOR = 'Active'
                   and fmrk.MARK_TYPE = 'Final'
                   and fmrk.HIGH_SCHOOL_CREDIT_INDICATOR = 'Yes'
                 group by fmrk.student_key, dsdt.date_value
            ) sub
                on sub.student_key = sart.student_key
            where RISK_FACTOR_KEY = -1
                group by sart.student_key, sart.STUDENT_RISK_REPORT_TEXT
        ) marks
        inner join k12intel_dw.DTBL_STUDENTS_EXTENSION dstu_ext
            on marks.student_key = dstu_ext.student_key
                inner join k12intel_dw.DTBL_STUDENTS dstu2                                                                                                                 -- Matt Michala  12/28/2011  (join to DSTU2 to make sure only insert SLCTOTHS records into SART for HS Grades based on the -1 key records)
                on marks.student_key = dstu2.student_key AND  DSTU2.STUDENT_CURRENT_GRADE_CODE in ('09','10','11','12')
        inner join k12intel_dw.dtbl_risk_factors drsk
            on risk_factor_id = 'SLTCTOTHS'
        left join k12intel_userdata.XTBL_SAIL_CREDIT_THRESHOLDS ct
            on ct.years = dstu_ext.STUDENT_YEARS_IN_HIGH_SCHOOL--case when to_date('07/01' || k12intel_metadata.get_sis_school_year(),'mm/dd/yyyy') > sub.student_risk_identified_date THEN sub.STUDENT_YEARS_IN_HIGH_SCHOOL-1 else sub.STUDENT_YEARS_IN_HIGH_SCHOOL END
                and ct.MARK_PERIOD = marks.mark_period
                and marks.total_credits_earned between low_value and high_value
                and ct.MS_HS = 'H'
 where dstu2.student_id = '8374833'