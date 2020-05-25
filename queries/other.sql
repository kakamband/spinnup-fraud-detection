SELECT report_date, 
  COUNT(*) AS total_number_of_tracks,
  SUM(non_fraud_not_picked) AS non_fraud_not_picked_AV,
  SUM(non_fraud) AS non_fraud_AV,
  SUM(fraud) AS fraud_AV,
  SUM(non_fraud_not_picked)/COUNT(*) AS non_fraud_not_picked_PCT,
  SUM(non_fraud)/COUNT(*) AS non_fraud_PCT,
  SUM(fraud)/COUNT(*) AS fraud_PCT
FROM
    (SELECT report_date, isrc, 
      IF(category = 'non-fraud' OR category = 'not-picked', 1, 0) AS non_fraud_not_picked,
      IF(category = 'non-fraud', 1, 0) AS non_fraud,
      IF(category != 'non-fraud' AND category != 'not-picked', 1, 0) AS fraud,
     FROM (SELECT a.report_date, a.isrc, IFNULL(b.category,'not-picked') as category,
           FROM (SELECT report_date, isrc
                FROM `umg-spinnup.spotify.spotify_trends` 
                WHERE report_date BETWEEN '2020-01-01' AND '2020-05-22'
                GROUP BY report_date, isrc) AS a
           LEFT JOIN (SELECT report_date, isrc, MAX(category) AS category
                      FROM `umg-data-science.detect_fraud_spinnup.v02_results`
                      GROUP BY report_date, isrc) AS b
           ON a.report_date = b.report_date AND a.isrc = b.isrc))
GROUP BY report_date
ORDER BY report_date DESC


--comparison between v02 and current model
SELECT stream_date, 
  COUNT(*) AS total_number_of_tracks,
  SUM(is_fraud_track) as cmodel_fraud,
  SUM(IF(category_v02 != 'non-fraud' AND category_v02 != 'not-picked',1,0)) as v02_fraud,
  SUM(fraud_and_fraud) as fraud_and_fraud_AV,
  SUM(fraud_and_not_picked_or_non_fraud) AS fraud_and_not_picked_or_non_fraud_AV,
  SUM(fraud_and_not_picked) AS fraud_and_not_picked_AV,
  SUM(fraud_and_non_fraud) AS fraud_and_non_fraud_AV,
  SUM(non_fraud_and_fraud) AS non_fraud_and_fraud_AV,
  SUM(non_fraud_and_not_picked_or_non_fraud) AS non_fraud_and_not_picked_or_non_fraud_AV,
  SUM(non_fraud_and_non_fraud) AS non_fraud_and_non_fraud_AV,
  SUM(fraud_and_fraud)/COUNT(*) AS fraud_and_fraud_PCT,
  SUM(fraud_and_not_picked_or_non_fraud)/COUNT(*) AS fraud_and_not_picked_or_non_fraud_PCT,
  SUM(fraud_and_not_picked)/COUNT(*) AS fraud_and_not_picked_PCT,
  SUM(fraud_and_non_fraud)/COUNT(*) AS fraud_and_non_fraud_PCT,
  SUM(non_fraud_and_fraud)/COUNT(*) AS non_fraud_and_fraud_PCT,
  SUM(non_fraud_and_not_picked_or_non_fraud)/COUNT(*) AS non_fraud_and_not_picked_or_non_fraud_PCT,
  SUM(non_fraud_and_non_fraud)/COUNT(*) AS non_fraud_and_non_fraud_PCT
FROM
(SELECT stream_date, isrc, is_fraud_track, category_v02,
  IF(is_fraud_track = 1 AND category_v02 != 'non-fraud' AND category_v02 != 'not-picked', 1, 0) as fraud_and_fraud,
  IF(is_fraud_track = 1 AND (category_v02 = 'not-picked' OR category_v02 = 'non-fraud'), 1, 0) as fraud_and_not_picked_or_non_fraud,
  IF(is_fraud_track = 1 AND category_v02 = 'not-picked', 1, 0) as fraud_and_not_picked,
  IF(is_fraud_track = 1 AND category_v02 = 'non-fraud', 1, 0) as fraud_and_non_fraud,
  IF(is_fraud_track = 0 AND category_v02 != 'non-fraud' AND category_v02 != 'not-picked', 1, 0) as non_fraud_and_fraud,
  IF(is_fraud_track = 0 AND (category_v02 = 'not-picked' OR category_v02 = 'non-fraud'), 1, 0) as non_fraud_and_not_picked_or_non_fraud,
  IF(is_fraud_track = 0 AND category_v02 = 'not-picked', 1, 0) as non_fraud_and_not_picked,
  IF(is_fraud_track = 0 AND category_v02 = 'non-fraud', 1, 0) as non_fraud_and_non_fraud
FROM
  (SELECT stream_date, isrc, is_fraud_track, category_v02
       FROM (SELECT a.stream_date, a.isrc, a.is_fraud_track, IFNULL(b.category,'not-picked') as category_v02
             FROM (SELECT stream_date, isrc, max(is_fraud_track) as is_fraud_track
                   FROM `umg-spinnup.spotify.spotify_analysis_track_daily` 
                   WHERE stream_date BETWEEN '2020-01-01' AND '2020-05-22'
                   GROUP BY stream_date, isrc) AS a
             LEFT JOIN (SELECT report_date, isrc, MAX(category) AS category
                        FROM `umg-data-science.detect_fraud_spinnup.v02_results`
                        GROUP BY report_date, isrc) AS b
             ON a.stream_date = b.report_date AND a.isrc = b.isrc))
  )
GROUP BY stream_date
ORDER BY stream_date DESC;