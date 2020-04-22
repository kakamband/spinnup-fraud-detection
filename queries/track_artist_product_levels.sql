--FOR ALL DATA FROM SPINNUP

--get all ids (this is track level)
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT isrc, artist_id, user_id, upc
FROM `umg-spinnup.trends.sch_user_assets`;

--create artist level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT user_id, artist_id, COUNT(DISTINCT isrc) as num_tracks, COUNT(DISTINCT upc) as num_products
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
GROUP BY user_id, artist_id;

--create product level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_products` AS
SELECT upc, artist_id, user_id, COUNT(DISTINCT isrc) as num_tracks_in_product
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
GROUP BY upc, artist_id, user_id;

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
INNER JOIN (SELECT artist_id, 
                    SUM(single) as single,
                    SUM(ep) as ep,
                    SUM(album) as album FROM `umg-data-science.detect_fraud_spinnup.spinnup_products`
            GROUP BY artist_id) AS b
ON a.artist_id = b.artist_id;

--add fraudster label to artist level table
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud` AS b
ON a.artist_id = b.artist_id;

--replace null fraudster label with 0
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT * EXCEPT(fraudster),
        IF(fraudster IS NULL, 0, fraudster) as fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists`;

--create track level table with product type information
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.single as is_single, b.ep as from_ep, b.album as from_album
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS a
LEFT JOIN (SELECT upc, 
                    single,
                    ep,
                    album 
                FROM `umg-data-science.detect_fraud_spinnup.spinnup_products`) AS b
ON a.upc = b.upc;

--add fraudster label to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.fraudster as from_fraudster
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS a
LEFT JOIN (SELECT artist_id, 
                fraudster
                FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists`) AS b
ON a.artist_id = b.artist_id;

--add fraud group to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.group as fraud_group
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud_grouped` as b
ON a.artist_id = b.artist_id;

--add fraud group to artist level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.group as fraud_group
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` as a
LEFT JOIN `umg-data-science.detect_fraud_spinnup.fraud_grouped` as b
ON a.artist_id = b.artist_id;

--add spotify ids
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
select a.*, b.* except(isrc)
from `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
left join
    (select isrc, MAX(track_id) as spotify_id, 
                  MAX(artist_id) as spotify_artist_id,
                  MAX(album_id) as spotify_album_id,
                  MAX(album_type) as album_type
    from `umg-data-science.discovar.spotify_tracks_metadata_spinnup` 
    where isrc in (select isrc 
                     from `umg-data-science.detect_fraud_spinnup.spinnup_tracks`
                      group by isrc) 
    group by isrc) as b
on
   a.isrc = b.isrc;

--add metadata from FUGA to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
SELECT a.*, b.* EXCEPT(upc, isrc)
FROM `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
LEFT JOIN `umg-spinnup.fuga.sch_products_assets` as b
ON a.isrc = b.isrc;

--add audio features to track level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_tracks` AS
select a.*, b.* except(track_id)
from `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as a
left join
    `umg-data-science.discovar.spotify_tracks_audio_features` as b
on
    a.spotify_id = b.track_id;

-------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------

--ARTISTS SOCIAL

--get the data for social media platforms from a specific date

CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists_socials` as    
SELECT a.user_id, 
  b.soundcloud_followers,
  c.soundcloud_comments,
  d.soundcloud_plays,
  e.instagram_followers,
  f.facebook_likes,
  g.youtube_views,
  h.youtube_followers,
  i.twitter_followers,
  j.tumblr_followers 
FROM (SELECT CAST(user_id AS INT64) as user_id FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` 
GROUP BY user_id) AS a
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as soundcloud_followers
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'soundcloud' and metric_name = 'followers') as b
ON a.user_id = b.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as soundcloud_comments
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'soundcloud' and metric_name = 'comments') as c
ON a.user_id = c.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as soundcloud_plays
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'soundcloud' and metric_name = 'plays') as d
ON a.user_id = d.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as instagram_followers
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'instagram' and metric_name = 'followers') as e
ON a.user_id = e.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as facebook_likes
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'facebook' and metric_name = 'likes') as f
ON a.user_id = f.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as youtube_views
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'youtube' and metric_name = 'views') as g
ON a.user_id = g.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as youtube_followers
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'youtube' and metric_name = 'followers') as h
ON a.user_id = h.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as twitter_followers
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'twitter' and metric_name = 'followers') as i
ON a.user_id = i.spinnup_user_id
LEFT JOIN
  (SELECT spinnup_user_id, metric_value as tumblr_followers
  FROM (SELECT spinnup_user_id, service_name, metric_name, MAX(metric_value) as metric_value FROM
       (SELECT * FROM `umg-spinnup.social_ae.artist_metric` WHERE metric_date = "2020-03-30")
        group by spinnup_user_id, service_name, metric_name
       ORDER by spinnup_user_id, service_name, metric_name)
  WHERE service_name = 'tumblr' and metric_name = 'followers') as j
ON a.user_id = j.spinnup_user_id;

--add to artist level
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS
SELECT a.*, b.* except(user_id)
FROM `umg-data-science.detect_fraud_spinnup.spinnup_artists` AS a
LEFT JOIN 
    `umg-data-science.detect_fraud_spinnup.spinnup_artists_socials` AS b
ON CAST(a.user_id as int64) = b.user_id;