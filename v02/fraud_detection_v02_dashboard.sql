select a.*, b.* except(report_date, isrc, duration), c.* except(isrc)
from `umg-data-science.detect_fraud_spinnup.v02_results` as a
left join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as c
on a.isrc = c.isrc
inner join `umg-data-science.detect_fraud_spinnup.v02_data` as b
on a.report_date = b.report_date and a.isrc = b.isrc;


