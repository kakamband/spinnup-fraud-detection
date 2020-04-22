--COUNTRY BREAKDOWN

DECLARE on_day DATE;
SET on_day = '2019-12-06';

--add streaming data for fraud
CREATE OR REPLACE TABLE `umg-data-science.detect_fraud_spinnup.spotify_track_country_streams` AS
SELECT isrc, 
  stream_country_code,
  MAX(stream_country_name) AS stream_country_name,
  SUM(number_of_streams) AS streams_market,
  SUM(number_of_streams)/COUNT(DISTINCT user_id) AS streams_per_user,
  AVG(IF(stream_length<stream_duration, stream_length/stream_duration, 1)) AS avg_stream_length_duration,
  SUM(IF(stream_length>30 AND stream_length<60, number_of_streams*1, 0))/SUM(number_of_streams) AS short_streams,
  SUM(IF(stream_length<30, number_of_streams*1, 0))/SUM(number_of_streams) AS unpaid_streams,
  SUM(IF(repeat_play_flag = true, number_of_streams*1, 0))/SUM(number_of_streams) AS repeated_streams,
  SUM(IF(completed_stream_flag = true, number_of_streams*1, 0))/SUM(number_of_streams) AS completed_streams,
  SUM(IF(partner_access_type = 'PREMIUM', number_of_streams*1, 0))/SUM(number_of_streams) AS premium_streams,
  SUM(IF(partner_access_type = 'FREE', number_of_streams*1, 0))/SUM(number_of_streams) AS free_streams,
  SUM(IF(partner_user_type = 'AD', number_of_streams*1, 0))/SUM(number_of_streams) AS ad_streams,
  SUM(IF(partner_user_type = 'PAID', number_of_streams*1, 0))/SUM(number_of_streams) AS paid_streams,
  SUM(IF(partner_user_type = 'TRIAL', number_of_streams*1, 0))/SUM(number_of_streams) AS trial_streams,
  SUM(IF(partner_user_type = 'PARTNER', number_of_streams*1, 0))/SUM(number_of_streams) AS partner_streams,
  SUM(IF(partner_user_type = 'PARTNER-FREE', number_of_streams*1, 0))/SUM(number_of_streams) AS partner_free_streams,
  SUM(IF(partner_user_type = 'NEW/UNCONFIGURED', number_of_streams*1, 0))/SUM(number_of_streams) AS unconfig_free_streams,
  SUM(IF(device_type = 'other', number_of_streams*1, 0))/SUM(number_of_streams) AS device_other_streams,
  SUM(IF(device_type = 'personal computer', number_of_streams*1, 0))/SUM(number_of_streams) AS device_pc_streams,
  SUM(IF(device_type = 'cell phone', number_of_streams*1, 0))/SUM(number_of_streams) AS device_phone_streams,
  SUM(IF(device_type = 'tablet', number_of_streams*1, 0))/SUM(number_of_streams) AS device_tablet_streams,
  SUM(IF(device_type = 'gaming console', number_of_streams*1, 0))/SUM(number_of_streams) AS device_gaming_console_streams,
  SUM(IF(device_type = 'connected audio device', number_of_streams*1, 0))/SUM(number_of_streams) AS device_connected_audio_streams,
  SUM(IF(device_type = 'smart tv device', number_of_streams*1, 0))/SUM(number_of_streams) AS device_smarttv_streams,
  SUM(IF(device_type = 'wearable', number_of_streams*1, 0))/SUM(number_of_streams) AS device_wearable_streams,
  SUM(IF(device_type = 'built-in car application', number_of_streams*1, 0))/SUM(number_of_streams) AS device_car_streams
FROM `umg-spinnup.spotify.spotify_trends`
WHERE report_date = on_day
GROUP BY isrc, stream_country_code;