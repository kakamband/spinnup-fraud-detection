--FOR ALL DATA FROM SPINNUP

--TRACK LEVEL

--get ids and metadata
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
select a.isrc, a.spotify_track_id, b.upc as spinnup_upc, b.user_id as spinnup_user_id, b.artist_id as spinnup_artist_id,
  c.* except(isrc, duration),
  c.duration/1000 as duration,
  d.* except(isrc, duration, release_date),
  d.duration as duration_fuga,
  d.release_date as release_date_fuga,
  e.* except(track_id)
from (select isrc, MAX(REPLACE(partner_track_uri, 'spotify:track:', '')) as spotify_track_id
      FROM `umg-spinnup.spotify.spotify_trends` 
      group by isrc) as a
left join `umg-spinnup.trends.sch_user_assets` as b
on a.isrc = b.isrc
left join `umg-data-science.discovar.spotify_tracks_metadata_spinnup` as c
on a.spotify_track_id = c.track_id
left join `umg-spinnup.fuga.sch_products_assets` as d
on a.isrc = d.isrc
left join `umg-data-science.discovar.spotify_tracks_audio_features` as e
on a.spotify_track_id = e.track_id

--add audio features to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
select a.*, b.* except(track_id)
from `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
left join `umg-data-science.discovar.spotify_tracks_audio_features` as b
on a.track_id = b.track_id;

--create artist level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT spinnup_user_id, spinnup_artist_id, COUNT(DISTINCT isrc) as num_tracks, COUNT(DISTINCT spinnup_upc) as num_products
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
GROUP BY spinnup_user_id, spinnup_artist_id;

--create product level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_products` AS
SELECT spinnup_upc, spinnup_artist_id, spinnup_user_id, COUNT(DISTINCT isrc) as num_tracks_in_product
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
GROUP BY spinnup_upc, spinnup_artist_id, spinnup_user_id;

--add product type to product level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_products` AS
SELECT *, 
    IF(num_tracks_in_product>=1 and num_tracks_in_product<=2, 1, 0) as single,
    IF(num_tracks_in_product>=3 and num_tracks_in_product<=6, 1, 0) as ep,
    IF(num_tracks_in_product>=7 and num_tracks_in_product<=25, 1, 0) as album
FROM `umg-data-science.detect_fraud_spinnup.spinnup_products`;

--add product type (counts) to artist level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.single, b.ep, b.album 
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS a
INNER JOIN (SELECT spinnup_artist_id, 
                    SUM(single) as single,
                    SUM(ep) as ep,
                    SUM(album) as album FROM `umg-data-science.detect_fraud_spinnup.spinnup_products`
            GROUP BY spinnup_artist_id) AS b
ON a.spinnup_artist_id = b.spinnup_artist_id;

--add fraudster label to artist level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud` AS b
ON a.spinnup_artist_id = b.artist_id;

--replace null fraudster label with 0
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT * EXCEPT(fraudster),
        IF(fraudster IS NULL, 0, fraudster) as fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists`;

--create track level table with product type information
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.single as is_single, b.ep as from_ep, b.album as from_album
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS a
LEFT JOIN (SELECT spinnup_upc, 
                    single,
                    ep,
                    album 
                FROM `umg-data-science.detect_fraud_spinnup.spinnup_products`) AS b
ON a.spinnup_upc = b.spinnup_upc;

--add fraudster label to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.fraudster as from_fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS a
LEFT JOIN (SELECT spinnup_artist_id, 
                fraudster
                FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists`) AS b
ON a.spinnup_artist_id = b.spinnup_artist_id;

--add fraud group to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.group as fraud_group
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud_grouped` as b
ON a.spinnup_artist_id = b.artist_id;

--add fraud group to artist level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.group as fraud_group
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` as a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud_grouped` as b
ON a.spinnup_artist_id = b.artist_id;

-------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------

--ARTISTS SOCIAL

--get the data for social media platforms from a specific date

CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists_socials` AS    
SELECT a.spinnup_user_id, 
  b.soundcloud_followers,
  c.soundcloud_comments,
  d.soundcloud_plays,
  e.instagram_followers,
  f.facebook_likes,
  g.youtube_views,
  h.youtube_followers,
  i.twitter_followers,
  j.tumblr_followers 
FROM (SELECT CAST(spinnup_user_id AS INT64) AS spinnup_user_id 
      FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` 
      GROUP BY spinnup_user_id) AS a
LEFT JOIN (SELECT spinnup_user_id, metric_value AS soundcloud_followers
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                       GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER by spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'soundcloud' AND metric_name = 'followers') AS b
ON a.spinnup_user_id = b.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS soundcloud_comments
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'soundcloud' AND metric_name = 'comments') AS c
ON a.spinnup_user_id = c.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS soundcloud_plays
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'soundcloud' AND metric_name = 'plays') AS d
           ON a.spinnup_user_id = d.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS instagram_followers
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'instagram' AND metric_name = 'followers') AS e
ON a.spinnup_user_id = e.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS facebook_likes
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'facebook' AND metric_name = 'likes') AS f
ON a.spinnup_user_id = f.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS youtube_views
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'youtube' AND metric_name = 'views') AS g
ON a.spinnup_user_id = g.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS youtube_followers
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")      
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'youtube' AND metric_name = 'followers') AS h
ON a.spinnup_user_id = h.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS twitter_followers
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'twitter' AND metric_name = 'followers') AS i
ON a.spinnup_user_id = i.spinnup_user_id
LEFT JOIN (SELECT spinnup_user_id, metric_value AS tumblr_followers
           FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) AS metric_value 
                 FROM (SELECT * 
                       FROM `umg-spinnup.social_ae.artist_metric` 
                       WHERE metric_date = "2020-03-30")
                 GROUP BY spinnup_user_id, service_name, metric_name
                 ORDER BY spinnup_user_id, service_name, metric_name)
           WHERE service_name = 'tumblr' AND metric_name = 'followers') AS j
ON a.spinnup_user_id = j.spinnup_user_id;

--add to artist level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.* except(user_id)
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.spinnup_artists_socials` AS b
ON CAST(a.spinnup_user_id AS int64) = b.spinnup_user_id;