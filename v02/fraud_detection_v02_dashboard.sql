with dashboard_data as (select a.*,
                                b.* except(report_date, isrc, duration),
                                c.* except(isrc)
                        from (select * 
                                from `umg-data-science.detect_fraud_spinnup.v02_results`
                                WHERE _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM `umg-data-science.detect_fraud_spinnup.v02_results`)) as a
                        inner join (select * 
                                from `umg-data-science.detect_fraud_spinnup.v02_data`
                                WHERE _PARTITIONTIME = (SELECT MAX(_PARTITIONTIME) FROM `umg-data-science.detect_fraud_spinnup.v02_results`)) as b
                        on a.report_date = b.report_date and a.isrc = b.isrc
                        left join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as c
                        on a.isrc = c.isrc),
    drop_dup as (select *,
                        row_number() over(partition by report_date, isrc order by report_date, isrc) as row_num
                        from dashboard_data)
        
    select * except(row_num) from drop_dup
    where row_num = 1;




