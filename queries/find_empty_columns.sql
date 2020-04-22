--CHECK PERCENTAGE OF NON NULL VALUES FROM STREAMING TABLE ON A SPECIFIC DAY
declare total_counts int64;
declare day date;
set day = '2020-03-29';
set total_counts = (select count(*) from (SELECT 
  *
FROM `umg-spinnup.spotify.spotify_trends`
WHERE report_date = day));

with streaming_onday as (SELECT 
  *
FROM `umg-spinnup.spotify.spotify_trends`
WHERE report_date = day)

SELECT 
  COUNT(report_date)/total_counts AS report_date, 
  COUNT(user_id)/total_counts AS user_id, 
  COUNT(product_id)/total_counts AS product_id, 
  COUNT(asset_id)/total_counts AS asset_id, 
  COUNT(user_country_code)/total_counts AS user_country_code, 
  COUNT(user_country_name)/total_counts AS user_country_name, 
  COUNT(user_dma_number)/total_counts AS user_dma_number, 
  COUNT(user_dma_name)/total_counts AS user_dma_name, 
  COUNT(user_region_code)/total_counts AS user_region_code, 
  COUNT(user_postal_code)/total_counts AS user_postal_code, 
  COUNT(user_gender)/total_counts AS user_gender, 
  COUNT(user_birth_year)/total_counts AS user_birth_year, 
  COUNT(user_age)/total_counts AS user_age, 
  COUNT(user_age_group)/total_counts AS user_age_group, 
  COUNT(track_artists)/total_counts AS track_artists, 
  COUNT(track_name)/total_counts AS track_name, 
  COUNT(partner_track_uri)/total_counts AS partner_track_uri, 
  COUNT(isrc)/total_counts AS isrc, 
  COUNT(upc)/total_counts AS upc, 
  COUNT(stream_date)/total_counts AS stream_date, 
  COUNT(stream_duration)/total_counts AS stream_duration, 
  COUNT(stream_length)/total_counts AS stream_length, 
  COUNT(stream_source)/total_counts AS stream_source, 
  COUNT(source_uri)/total_counts AS source_uri, 
  COUNT(device_type)/total_counts AS device_type, 
  COUNT(stream_country_code)/total_counts AS stream_country_code, 
  COUNT(stream_country_name)/total_counts AS stream_country_name, 
  COUNT(partner_access_type)/total_counts AS partner_access_type, 
  COUNT(partner_user_type)/total_counts AS partner_user_type, 
  COUNT(revenue_model)/total_counts AS revenue_model, 
  COUNT(consumer_group)/total_counts AS consumer_group, 
  COUNT(consumer_group_detail)/total_counts AS consumer_group_detail, 
  COUNT(engagement_style)/total_counts AS engagement_style, 
  COUNT(shuffle_play_flag)/total_counts AS shuffle_play_flag, 
  COUNT(repeat_play_flag)/total_counts AS repeat_play_flag, 
  COUNT(cached_play_flag)/total_counts AS cached_play_flag, 
  COUNT(completed_stream_flag)/total_counts AS completed_stream_flag, 
  COUNT(number_of_downloads)/total_counts AS number_of_downloads, 
  COUNT(number_of_streams)/total_counts AS number_of_streams, 
  COUNT(partner_name)/total_counts AS partner_name, 
  COUNT(load_datetime)/total_counts AS load_datetime, 
  COUNT(device_os)/total_counts AS device_os, 
  COUNT(number_of_listeners)/total_counts AS number_of_listeners, 
  COUNT(number_of_saves)/total_counts AS number_of_saves
FROM streaming_onday;