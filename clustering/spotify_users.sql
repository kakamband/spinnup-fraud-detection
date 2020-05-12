CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spotify_users` as
SELECT * FROM (SELECT
                a.user_id,
                COUNT(distinct a.report_date) as num_days,
                SUM(a.number_of_streams) as streams,
                COUNT(distinct a.isrc) as tracks,
                COUNT(distinct a.upc) as products,
                COUNT(distinct b.spinnup_artist_id) as accounts,
                SUM(a.number_of_streams)/COUNT(distinct a.isrc) as streams_per_track,
                SUM(a.number_of_streams)/COUNT(distinct a.upc) as streams_per_product,
                SUM(a.number_of_streams)/COUNT(distinct b.spinnup_artist_id) as streams_per_account,
                SUM(a.number_of_streams)/COUNT(distinct a.report_date) as avg_streams_per_day,
                COUNT(distinct a.isrc)/COUNT(distinct a.report_date) as avg_tracks_per_day,
                COUNT(distinct a.upc)/COUNT(distinct a.report_date) as avg_products_per_day,
                COUNT(distinct b.spinnup_artist_id)/COUNT(distinct a.report_date) as avg_accounts_per_day,
                (SUM(a.number_of_streams)/COUNT(distinct a.isrc))/COUNT(distinct report_date) as avg_streams_per_track_per_day,
                (SUM(a.number_of_streams)/COUNT(distinct a.upc))/COUNT(distinct report_date) as avg_streams_per_product_per_day,
                (SUM(a.number_of_streams)/COUNT(distinct b.spinnup_artist_id))/COUNT(distinct report_date) as avg_streams_per_account_per_day,
                MAX(IF(a.partner_access_type = 'PREMIUM', 1, 0)) as is_premium
                FROM 
                    (SELECT * FROM `umg-spinnup.spotify.spotify_trends`
                     WHERE report_date BETWEEN DATE_SUB('2020-04-24', INTERVAL 28 DAY) AND '2020-04-24'
                     AND stream_length >= 30) as a
                LEFT JOIN `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as b
                ON a.isrc = b.isrc
                GROUP BY user_id)
WHERE streams > 200

