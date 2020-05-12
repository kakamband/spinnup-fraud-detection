declare sday, end_date date;
declare weekly_streams, thres, thres_accounts, thres_tracks int64;
set end_date = '2020-04-24';
set sday = date_sub(end_date, interval 28 day);
set weekly_streams = 2000;
set thres = 50; --number of streams for one track on a day
--set thres_tracks = 50;
set thres_accounts = 40; --number of accounts visited in the last month

create or replace table`umg-data-science.detect_fraud_spinnup.f1_tracks_ts` (
`report_date` date,
`isrc` string,
--`streams` int64,
--`users` int64,
`streams_per_user` float64,
--`different_country_streams` float64,
`different_country_streams_pct` float64,
--`repeat_users` float64,
--`repeat_users_pct` float64,
--`repeat_streams` float64,
`repeat_streams_pct` float64,
--`focused_users` float64,
--`focused_users_pct` float64,
--`focused_streams` float64,
`focused_streams_pct` float64,
--`premium_streams` float64,
`premium_streams_pct` float64,
--`free_streams` float64,
--`free_streams_pct` float64,
--`leanfwd_streams` float64,
`leanfwd_streams_pct` float64,
--`leanback_streams` float64,
--`leanback_streams_pct` float64,
--`album_streams` float64,
`album_streams_pct` float64,
--`chart_streams` float64,
--`chart_streams_pct` float64,
--`radio_streams` float64,
--`radio_streams_pct` float64,
--`artist_streams` float64,
--`artist_streams_pct` float64,
--`search_streams` float64,
--`search_streams_pct` float64,
--`collection_streams` float64,
`collection_streams_pct` float64,
--`others_playlist_streams` float64,
`others_playlist_streams_pct` float64,
--`other_source_streams` float64,
`other_source_streams_pct` float64,
--`short_streams` float64,
`short_streams_pct` float64,
--`repeat_streams` float64,
--`repeat_streams_pct` float64,
--`completed_streams` float64,
`completed_streams_pct` float64,
--`shuffle_streams` float64,
`shuffle_streams_pct` float64,
--`cached_streams` float64,
--`cached_streams_pct` float64,
--`device_mobile` float64,
`device_mobile_pct` float64,
--`device_pc` float64,
`device_pc_pct` float64,
--`device_other` float64,
`device_other_pct` float64,
--`device_tablet` float64,
--`device_tablet_pct` float64,
--`device_console` float64,
--`device_console_pct` float64,
--`device_connected_audio` float64,
--`device_connected_audio_pct` float64,
--`device_smart_tv` float64,
--`device_smart_tv_pct` float64,
--`device_wearable` float64,
--`device_wearable_pct` float64,
--`device_car` float64,
--`device_car_pct` float64,
`no_vocals` float64,
`duration` int64,
`avg_listening` float64,
`avg_stream_length` float64
);

while (sday < end_date) 
do
	create or replace table `umg-data-science.detect_fraud_spinnup.f1_tracks_ts` as
 	with main_query as (
		with 
		onday_tracks as (select isrc 
						 from `umg-spinnup.spotify.spotify_trends` 
						 where report_date = sday
						 group by isrc),

        onday_users as (select user_id 
						 from `umg-spinnup.spotify.spotify_trends` 
						 where report_date = sday
						 group by user_id),

		past_week_streams as (select isrc, sum(number_of_streams) as streams
							  from (select isrc, number_of_streams
									from `umg-spinnup.spotify.spotify_trends` 
									where report_date between date_sub(sday, interval 6 day) and sday
									and stream_length >= 30
									and isrc in (select isrc from onday_tracks))
							  group by isrc),

        past_month_ondayusers as (select user_id, count(distinct upc) as num_products, 
                                                count(distinct isrc) as num_tracks, 
                                                count(distinct spinnup_artist_id) as num_accounts,
                                                SUM(IF(device_type = 'other', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_other_pct,
							    from (select a.user_id, a.isrc, a.upc, b.spinnup_artist_id, a.device_type
									  from 
                                                                  (select * from `umg-spinnup.spotify.spotify_trends` 
									              where report_date between date_sub(sday, interval 28 day) and sday
									              and stream_length >= 30
									              and user_id in (select user_id from onday_users)) as a
                                      left join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as b
                                      on a.isrc = b.isrc)
						group by user_id),
							
		f1_tracks as (select isrc
					  from past_week_streams
					  where streams > weekly_streams
					  group by isrc),
					
		f1_streaming as (select *
						 from `umg-spinnup.spotify.spotify_trends`
						 where report_date = sday
						 and stream_length >= 30
						 and isrc in (select isrc from f1_tracks)),
            
        focused_users as (select user_id
                          from past_month_ondayusers
						  where num_accounts >= thres_accounts --add num_tracks and/or num_products
                                      and device_other_pct >= 0.1
						  group by user_id),
                      
        focused_tracks as (select isrc
                           from f1_streaming
                           where user_id in (select user_id from focused_users)
                           group by isrc),
						
		repeat_users as (select user_id
						 from (select user_id, isrc, sum(number_of_streams) as streams
							    from f1_streaming
							    group by user_id, isrc)
						 where streams >= thres
						 group by user_id),
            
        repeat_tracks as (select isrc
                          from f1_streaming
                          where user_id in (select user_id from repeat_users)),

		track_repeat_users as (select isrc, count(distinct user_id) as num_repeat_users, sum(number_of_streams) as num_repeat_streams
							   from (select isrc, user_id, number_of_streams 
									 from (select *
                                           from f1_streaming
                                           where user_id in (select user_id from repeat_users)
                                           and isrc in (select isrc from repeat_tracks)))
							    group by isrc),
              
        track_focused_users as (select isrc, count(distinct user_id) as num_foc_users, sum(number_of_streams) as num_foc_streams
						        from (select isrc, user_id, number_of_streams
                                      from (select *
                                            from f1_streaming
                                            where user_id in (select user_id from focused_users)
                                            and isrc in (select isrc from focused_tracks)))
						        group by isrc),          

		clust_features as 
		(select isrc, 
            SUM(number_of_streams) as streams,
            COUNT(DISTINCT user_id) as users,
            SUM(number_of_streams)/COUNT(DISTINCT user_id) AS streams_per_user,
            SUM(IF(stream_country_code != user_country_code, number_of_streams*1, 0)) AS different_country_streams,
            SUM(IF(partner_access_type = 'PREMIUM', number_of_streams*1, 0)) AS premium_streams,
            SUM(IF(partner_access_type = 'FREE', number_of_streams*1, 0)) AS free_streams,
            SUM(IF(engagement_style = 'Lean Forward', number_of_streams*1, 0)) AS leanfwd_streams,
            SUM(IF(engagement_style = 'Lean Back', number_of_streams*1, 0)) AS leanback_streams,
            SUM(IF(stream_source = 'album', number_of_streams*1, 0)) AS album_streams,
            SUM(IF(stream_source = 'chart', number_of_streams*1, 0))  AS chart_streams,
            SUM(IF(stream_source = 'radio', number_of_streams*1, 0))  AS radio_streams,
            SUM(IF(stream_source = 'artist', number_of_streams*1, 0))  AS artist_streams,
            SUM(IF(stream_source = 'search', number_of_streams*1, 0))  AS search_streams,
            SUM(IF(stream_source = 'collection', number_of_streams*1, 0))  AS collection_streams,
            SUM(IF(stream_source = 'others_playlist', number_of_streams*1, 0))  AS others_playlist_streams,
            SUM(IF(stream_source = 'other', number_of_streams*1, 0))  AS other_source_streams,
            SUM(IF(stream_length>=30 AND stream_length<=70, number_of_streams*1, 0))  AS short_streams,
            SUM(IF(repeat_play_flag = true, number_of_streams*1, 0))  AS repeat_streams,
            SUM(IF(shuffle_play_flag = true, number_of_streams*1, 0))  AS shuffle_streams,
            SUM(IF(cached_play_flag = true, number_of_streams*1, 0))  AS cached_streams,
            SUM(IF(completed_stream_flag = true, number_of_streams*1, 0))  AS completed_streams,
            SUM(IF(device_type = 'other', number_of_streams*1, 0))  AS device_other_streams,
            SUM(IF(device_type = 'personal computer', number_of_streams*1, 0))  AS device_pc_streams,
            SUM(IF(device_type = 'cell phone', number_of_streams*1, 0))  AS device_mobile_streams,
            SUM(IF(device_type = 'tablet', number_of_streams*1, 0))  AS device_tablet_streams,
            SUM(IF(device_type = 'gaming console', number_of_streams*1, 0))  AS device_gaming_console_streams,
            SUM(IF(device_type = 'connected audio device', number_of_streams*1, 0))  AS device_connected_audio_streams,
            SUM(IF(device_type = 'smart tv device', number_of_streams*1, 0))  AS device_smarttv_streams,
            SUM(IF(device_type = 'wearable', number_of_streams*1, 0))  AS device_wearable_streams,
            SUM(IF(device_type = 'built-in car application', number_of_streams*1, 0))  AS device_car_streams,
            SUM(IF(stream_country_code != user_country_code, number_of_streams*1, 0))/SUM(number_of_streams) AS different_country_streams_pct,
            SUM(IF(partner_access_type = 'PREMIUM', number_of_streams*1, 0))/SUM(number_of_streams) AS premium_streams_pct,
            SUM(IF(partner_access_type = 'FREE', number_of_streams*1, 0))/SUM(number_of_streams) AS free_streams_pct,
            SUM(IF(engagement_style = 'Lean Forward', number_of_streams*1, 0))/SUM(number_of_streams) AS leanfwd_streams_pct,
            SUM(IF(engagement_style = 'Lean Back', number_of_streams*1, 0))/SUM(number_of_streams) AS leanback_streams_pct,
            SUM(IF(stream_source = 'album', number_of_streams*1, 0))/SUM(number_of_streams) AS album_streams_pct,
            SUM(IF(stream_source = 'chart', number_of_streams*1, 0))/SUM(number_of_streams)  AS chart_streams_pct,
            SUM(IF(stream_source = 'radio', number_of_streams*1, 0))/SUM(number_of_streams)  AS radio_streams_pct,
            SUM(IF(stream_source = 'artist', number_of_streams*1, 0))/SUM(number_of_streams)  AS artist_streams_pct,
            SUM(IF(stream_source = 'search', number_of_streams*1, 0))/SUM(number_of_streams)  AS search_streams_pct,
            SUM(IF(stream_source = 'collection', number_of_streams*1, 0))/SUM(number_of_streams)  AS collection_streams_pct,
            SUM(IF(stream_source = 'others_playlist', number_of_streams*1, 0))/SUM(number_of_streams)  AS others_playlist_streams_pct,
            SUM(IF(stream_source = 'other', number_of_streams*1, 0))/SUM(number_of_streams)  AS other_source_streams_pct,
            SUM(IF(stream_length>=30 AND stream_length<=70, number_of_streams*1, 0))/SUM(number_of_streams)  AS short_streams_pct,
            SUM(IF(repeat_play_flag = true, number_of_streams*1, 0))/SUM(number_of_streams)  AS repeat_streams_pct,
            SUM(IF(shuffle_play_flag = true, number_of_streams*1, 0))/SUM(number_of_streams)  AS shuffle_streams_pct,
            SUM(IF(cached_play_flag = true, number_of_streams*1, 0))/SUM(number_of_streams)  AS cached_streams_pct,
            SUM(IF(completed_stream_flag = true, number_of_streams*1, 0))/SUM(number_of_streams)  AS completed_streams_pct,
            SUM(IF(device_type = 'other', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_other_streams_pct,
            SUM(IF(device_type = 'personal computer', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_pc_streams_pct,
            SUM(IF(device_type = 'cell phone', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_mobile_streams_pct,
            SUM(IF(device_type = 'tablet', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_tablet_streams_pct,
            SUM(IF(device_type = 'gaming console', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_gaming_console_streams_pct,
            SUM(IF(device_type = 'connected audio device', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_connected_audio_streams_pct,
            SUM(IF(device_type = 'smart tv device', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_smarttv_streams_pct,
            SUM(IF(device_type = 'wearable', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_wearable_streams_pct,
            SUM(IF(device_type = 'built-in car application', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_car_streams_pct,
            AVG(IF(stream_length<stream_duration, stream_length/stream_duration, 1)) AS avg_listening,
            SUM(IF(stream_length<stream_duration, number_of_streams*stream_length, number_of_streams*stream_duration))/SUM(number_of_streams)  as avg_stream_length
            from f1_streaming
            group by isrc
		),
		
		audio as (select a.isrc, b.duration, b.instrumentalness
				  from f1_tracks as a
				  inner join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as b
				  on a.isrc=b.isrc)

		select * 
		from `umg-data-science.detect_fraud_spinnup.f1_tracks_ts`

		union all

		select sday as report_date,
			   a.isrc, 
			   --a.streams,
			   --a.users,
               a.streams_per_user,
               --a.different_country_streams,
			   a.different_country_streams_pct,
               --ifnull(b.num_repeat_users,0) as repeat_users,
			   --ifnull(b.num_repeat_users,0)/a.users as repeat_users_pct,
               --ifnull(b.num_repeat_streams,0) as repeat_streams,
			   ifnull(b.num_repeat_streams,0)/a.streams as repeat_streams_pct,
               --ifnull(c.num_foc_users,0) as focused_users,
			   --ifnull(c.num_foc_users,0)/a.users as focused_users_pct,
               --ifnull(c.num_foc_streams,0) as focused_streams,
			   ifnull(c.num_foc_streams,0)/a.streams as focused_streams_pct,
               --a.premium_streams,
			   a.premium_streams_pct,
			   --a.free_streams,
               --a.free_streams_pct,
               --a.leanfwd_streams,
			   a.leanfwd_streams_pct,
			   --a.leanback_streams,
               --a.leanback_streams_pct,
               --a.album_streams,
			   a.album_streams_pct,
			   --a.chart_streams,
               --a.chart_streams_pct,
			   --a.radio_streams,
               --a.radio_streams_pct,
               --a.artist_streams,
			   --a.artist_streams_pct,
			   --a.search_streams,
               --a.search_streams_pct,
               --a.collection_streams,
			   a.collection_streams_pct,
               --a.others_playlist_streams,
			   a.others_playlist_streams_pct,
               --a.other_source_streams,
			   a.other_source_streams_pct,
               --a.short_streams,
			   a.short_streams_pct,
			   --a.repeat_streams,
               --a.repeat_streams_pct,
               --a.completed_streams,
			   a.completed_streams_pct,
               --a.shuffle_streams,
               a.shuffle_streams_pct,
               --a.cached_streams,
               --a.cached_streams_pct,
               --a.device_mobile_streams as device_mobile,
			   a.device_mobile_streams_pct as device_mobile_pct,
               --a.device_pc_streams as device_pc,
			   a.device_pc_streams_pct as device_pc_pct,
               --a.device_other_streams as device_other,
			   a.device_other_streams_pct as device_other_pct,
               --a.device_tablet_streams as device_tablet,
               --a.device_tablet_streams as device_tablet_pct,
               --a.device_gaming_console_streams as device_console,
               --a.device_gaming_console_streams as device_console_pct,
               --a.device_connected_audio_streams as device_connected_audio,
               --a.device_connected_audio_streams as device_connected_audio_pct,
               --a.device_smarttv_streams as device_smart_tv,
               --a.device_smarttv_streams as device_smart_tv_pct,
               --a.device_wearable_streams as device_wearable,
               --a.device_wearable_streams as device_wearable_pct,
               --a.device_car_streams as device_car,
               --a.device_car_streams as device_car_pct
               d.instrumentalness as no_vocals,
			   d.duration,
			   a.avg_listening,
			   a.avg_stream_length
		from clust_features as a
		left join track_repeat_users as b
		on a.isrc = b.isrc
        left join track_focused_users as c
        on a.isrc = c.isrc
		left join audio as d
		on a.isrc = d.isrc
  )

  select * from main_query;
  set sday = date_add(sday, interval 1 day);
end while                    