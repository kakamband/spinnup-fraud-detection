declare sday, end_date date;
declare weekly_streams, thres, thres_tracks int64;
set end_date = '2020-03-30';
set sday = date_sub(end_date, interval 28 day);
set weekly_streams = 2000;
set thres = 50;
set thres_tracks = 10;

create or replace table`umg-data-science.detect_fraud_spinnup.f1_tracks28days` (
`stream_date` date,
`isrc` string,
`streams` int64,
`users` int64,
`streams_per_user` float64,
`pct_fraud_users` float64,
`pct_fraud_streams` float64,
`pct_focused_users` float64,
`premium_streams` float64,
`free_streams` float64,
`duration` int64,
`no_vocals` float64,
`avg_listening` float64,
`avg_stream_length` float64,
`short_streams` float64,
`repeated_streams` float64,
`completed_streams` float64,
`device_mobile` float64,
`device_pc` float64,
`device_other` float64
);

while (sday < end_date) 
do
	create or replace table `umg-data-science.detect_fraud_spinnup.f1_tracks28days` as
  with main_query as (
		with 
		onday_tracks as (select isrc 
						from `umg-spinnup.spotify.spotify_trends` 
						where stream_date = sday
						group by isrc),

		past_week_streams as (select isrc, sum(number_of_streams) as streams
							from (select isrc, number_of_streams
									from `umg-spinnup.spotify.spotify_trends` 
									where stream_date between date_sub(sday, interval 6 day) and sday
									and isrc in (select isrc from onday_tracks))
							group by isrc),
							
		f1_tracks as (select isrc
					from past_week_streams
					where streams > weekly_streams
					group by isrc),
					
		f1_streaming as (select *
						from `umg-spinnup.spotify.spotify_trends`
						where stream_date = sday
						and isrc in (select isrc from f1_tracks)),
            
    focused_users as (select user_id
						          from (select user_id, count(distinct isrc) as num_tracks
                            from f1_streaming
                            group by user_id)
						          where num_tracks >= thres_tracks
						          group by user_id),
						
		fraud_tracks as (select isrc
						from (select user_id, isrc, sum(number_of_streams) as streams
							from f1_streaming
							group by user_id, isrc)
						where streams >= thres
						group by isrc),

		fraud_users as (select user_id
						from (select user_id, isrc, sum(number_of_streams) as streams
							from f1_streaming
							group by user_id, isrc)
						where streams >= thres
						group by user_id),
					
		f2_streaming as (select *
						from f1_streaming
						where isrc in (select isrc from fraud_tracks)
						and user_id in (select user_id from fraud_users)),

		track_fraud_users as (select isrc, count(distinct user_id) as num_fraud_users, sum(number_of_streams) as num_fraud_streams
							from (select isrc, user_id, number_of_streams 
									from f2_streaming)
							group by isrc),
              
    track_focused_users as (select isrc, count(distinct user_id) as num_foc_users
						from	
							(select isrc, user_id
							from f1_streaming
							where user_id in (select user_id from focused_users)
							group by isrc, user_id)
						group by isrc),          
							
		clust_features as 
		(select isrc, 
		SUM(number_of_streams) as streams,
		COUNT(DISTINCT user_id) as users,
		SUM(number_of_streams)/COUNT(DISTINCT user_id) AS streams_per_user,
		SUM(IF(partner_access_type = 'PREMIUM', number_of_streams*1, 0))/SUM(number_of_streams) AS premium_streams,
		SUM(IF(partner_access_type = 'FREE', number_of_streams*1, 0))/SUM(number_of_streams) AS free_streams,
		AVG(IF(stream_length<stream_duration, stream_length/stream_duration, 1)) AS avg_listening,
		SUM(IF(stream_length<stream_duration, number_of_streams*stream_length, number_of_streams*stream_duration))/SUM(number_of_streams) as avg_stream_length,
		SUM(IF(stream_length>=30 AND stream_length<=70, number_of_streams*1, 0))/SUM(number_of_streams) AS short_streams,
		SUM(IF(repeat_play_flag = true, number_of_streams*1, 0))/SUM(number_of_streams) AS repeated_streams,
		SUM(IF(completed_stream_flag = true, number_of_streams*1, 0))/SUM(number_of_streams) AS completed_streams,
		SUM(IF(device_type = 'other', number_of_streams*1, 0))/SUM(number_of_streams) AS device_other_streams,
		SUM(IF(device_type = 'personal computer', number_of_streams*1, 0))/SUM(number_of_streams) AS device_pc_streams,
		SUM(IF(device_type = 'cell phone', number_of_streams*1, 0))/SUM(number_of_streams) AS device_mobile_streams,
		from f1_streaming
		group by isrc
		),
		
		audio as (select isrc, duration, instrumentalness from `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
		where isrc in (select isrc from f1_tracks))

		select * from `umg-data-science.detect_fraud_spinnup.f1_tracks28days`

		union all

		select sday as stream_date,
			a.isrc, a.streams,
						a.users, 
						a.streams_per_user,
						ifnull(b.num_fraud_users,0)/a.users as pct_fraud_users,
						ifnull(b.num_fraud_streams,0)/a.streams as pct_fraud_streams,
            ifnull(c.num_foc_users,0)/a.users as pct_focused_users,
						a.premium_streams,
						a.free_streams,
						d.duration,
						d.instrumentalness as no_vocals,
						a.avg_listening,
						a.avg_stream_length,
						a.short_streams,
						a.repeated_streams,
						a.completed_streams,
						a.device_mobile_streams as device_mobile,
						a.device_pc_streams as device_pc,
						a.device_other_streams as device_other
		from clust_features as a
		left join track_fraud_users as b
		on a.isrc = b.isrc
    left join track_focused_users as c
    on a.isrc = c.isrc
		left join audio as d
		on a.isrc = d.isrc
  )

	select * from main_query;
  set sday = date_add(sday, interval 1 day);
end while
                    
                    