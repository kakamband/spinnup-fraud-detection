with 
onday_tracks as (select isrc 
				from `umg-spinnup.spotify.spotify_trends` 
				where report_date = '{{ ds }}'
				group by isrc),

past_week_streams as (select isrc, sum(number_of_streams) as streams
					from (select isrc, number_of_streams
							from `umg-spinnup.spotify.spotify_trends` 
							where report_date between date_sub('{{ ds }}', interval 6 day) and '{{ ds }}'
							and stream_length >= 30
							and isrc in (select isrc from onday_tracks))
					group by isrc),
					
f1_tracks as (select isrc
			from past_week_streams
			where streams > 2000
			group by isrc),
			
f1_streaming as (select *
				from `umg-spinnup.spotify.spotify_trends`
				where report_date = '{{ ds }}'
				and stream_length >= 30
				and isrc in (select isrc from f1_tracks)),
	
focused_users as (select user_id
                          from (select user_id, count(distinct product_id) as num_sp_accounts, count(distinct isrc) as num_sp_tracks
                                from (select a.user_id, a.isrc, b.product_id
                                      	from f1_streaming as a
                                   	  	left join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as b
                                    	on a.isrc = b.isrc)
                                group by user_id)
						    where num_sp_accounts >= 10 and num_sp_tracks >= 50
						    group by user_id),
                      
focused_tracks as (select isrc
					from f1_streaming
					where user_id in (select user_id from focused_users)),
				
repeat_users as (select user_id
					from (select user_id, isrc, sum(number_of_streams) as streams
						from f1_streaming
						group by user_id, isrc)
					where streams >= 50
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

audio as (select isrc, duration, instrumentalness 
			from `umg-data-science.detect_fraud_spinnup.spinnup_tracks` 
			where isrc in (select isrc from f1_tracks))

select Date('{{ ds }}') as report_date,
	a.isrc, a.streams,
				a.users, 
				a.streams_per_user,
				ifnull(b.num_repeat_users,0)/a.users as pct_repeat_users,
				ifnull(b.num_repeat_streams,0)/a.streams as pct_repeat_streams,
				ifnull(c.num_foc_users,0)/a.users as pct_focused_users,
				ifnull(c.num_foc_streams,0)/a.streams as pct_focused_streams,
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
left join track_repeat_users as b
on a.isrc = b.isrc
left join track_focused_users as c
on a.isrc = c.isrc
left join audio as d
on a.isrc = d.isrc;