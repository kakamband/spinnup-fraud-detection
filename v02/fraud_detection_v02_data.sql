
with 
        onday_tracks as (select isrc 
                from `umg-spinnup.spotify.spotify_trends` 
                where report_date = '{{ ds }}'
                group by isrc),

        onday_users as (select user_id 
                from `umg-spinnup.spotify.spotify_trends` 
                where report_date = '{{ ds }}'
                group by user_id),

        past_week_streams as (select isrc, sum(number_of_streams) as streams
                        from (select isrc, number_of_streams
                        from `umg-spinnup.spotify.spotify_trends` 
                        where report_date between date_sub('{{ ds }}', interval 6 day) and '{{ ds }}'
                        and stream_length >= 30
                        and isrc in (select isrc from onday_tracks group by isrc))
                        group by isrc),
        
        f1_tracks as (select isrc
                from past_week_streams
                where streams > 2000 --threshold here
                group by isrc),
        
        f1_streaming as (select *
                from `umg-spinnup.spotify.spotify_trends`
                where report_date = '{{ ds }}'
                and isrc in (select isrc from f1_tracks group by isrc)
                and stream_length >= 30),

        onday_users_past_month_features as (select user_id, 
                                                count(distinct upc) as num_products, 
                                                count(distinct isrc) as num_tracks, 
                                                count(distinct spinnup_artist_id) as num_accounts,
                                                SUM(IF(device_type = 'other', number_of_streams*1, 0))/SUM(number_of_streams)  AS device_other_pct,
                                                from (select a.user_id, a.isrc, a.upc, a.number_of_streams, b.spinnup_artist_id, a.device_type
                                                        from (select * 
                                                        from `umg-spinnup.spotify.spotify_trends` 
                                                        where report_date between date_sub('{{ ds }}', interval 27 day) and '{{ ds }}'
                                                        and user_id in (select user_id from onday_users group by user_id)
                                                        and stream_length >= 30) as a
                                                        left join `umg-data-science.detect_fraud_spinnup.spinnup_tracks` as b
                                                        on a.isrc = b.isrc)
                                                group by user_id),

        avg_accounts as (select isrc, AVG(num_accounts) as avg_accounts_per_user
                                from  (select a.isrc, b.num_accounts
                                        from f1_streaming as a
                                        left join onday_users_past_month_features as b
                                        on a.user_id = b.user_id)
                                group by isrc),
                                        
        focused_users as (select user_id
                from onday_users_past_month_features
                where num_accounts >= 40 --threshold here
                and device_other_pct >= 0.1 --threshold here
                group by user_id),
                        
        focused_tracks as (select isrc
                                from f1_streaming
                                where user_id in (select user_id from focused_users group by user_id)
                                group by isrc),
                        
        repeat_users as (select user_id
                from (select user_id, isrc, sum(number_of_streams) as streams
                        from f1_streaming
                        group by user_id, isrc)
                where streams >= 50 --threshold here
                group by user_id),
        
        repeat_tracks as (select isrc
                                from f1_streaming
                                where user_id in (select user_id from repeat_users group by user_id)
                                group by isrc),

        track_repeat_users as (select isrc, count(distinct user_id) as num_repeat_users, sum(number_of_streams) as num_repeat_streams
                        from (select isrc, user_id, number_of_streams 
                                from (select *
                                        from f1_streaming
                                        where user_id in (select user_id from repeat_users group by user_id)
                                        and isrc in (select isrc from repeat_tracks group by isrc)))
                        group by isrc),
                
        track_focused_users as (select isrc, count(distinct user_id) as num_foc_users, sum(number_of_streams) as num_foc_streams
                                from (select isrc, user_id, number_of_streams
                                        from (select *
                                                from f1_streaming
                                                where user_id in (select user_id from focused_users)
                                                and isrc in (select isrc from focused_tracks)))
                        group by isrc),          

        s_features as (select isrc, 
                                SUM(number_of_streams) as streams,
                                COUNT(DISTINCT user_id) as users,
                                SUM(number_of_streams)/COUNT(DISTINCT user_id) AS streams_per_user,
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
                on a.isrc=b.isrc),

        dataset as (select
                Date('{{ ds }}') as report_date,
                a.isrc, 
                a.streams,
                a.users,
                a.streams_per_user,
                --a.different_country_streams,
                a.different_country_streams_pct,
                --ifnull(b.num_repeat_users,0) as repeater_users,
                ifnull(b.num_repeat_users,0)/a.users as repeater_users_pct,
                --ifnull(b.num_repeat_streams,0) as repeater_streams,
                ifnull(b.num_repeat_streams,0)/a.streams as repeater_streams_pct,
                --ifnull(c.num_foc_users,0) as focused_users,
                ifnull(c.num_foc_users,0)/a.users as focused_users_pct,
                --ifnull(c.num_foc_streams,0) as focused_streams,
                ifnull(c.num_foc_streams,0)/a.streams as focused_streams_pct,
                e.avg_accounts_per_user,
                --a.premium_streams,
                a.premium_streams_pct,
                --a.free_streams,
                a.free_streams_pct,
                --a.leanfwd_streams,
                a.leanfwd_streams_pct,
                --a.leanback_streams,
                a.leanback_streams_pct,
                --a.album_streams,
                a.album_streams_pct,
                --a.chart_streams,
                a.chart_streams_pct,
                --a.radio_streams,
                a.radio_streams_pct,
                --a.artist_streams,
                a.artist_streams_pct,
                --a.search_streams,
                a.search_streams_pct,
                --a.collection_streams,
                a.collection_streams_pct,
                --a.others_playlist_streams,
                a.others_playlist_streams_pct,
                --a.other_source_streams,
                a.other_source_streams_pct,
                --a.short_streams,
                a.short_streams_pct,
                --a.repeat_streams,
                a.repeat_streams_pct,
                --a.completed_streams,
                a.completed_streams_pct,
                --a.shuffle_streams,
                a.shuffle_streams_pct,
                --a.cached_streams,
                a.cached_streams_pct,
                --a.device_mobile_streams as device_mobile,
                a.device_mobile_streams_pct as device_mobile_pct,
                --a.device_pc_streams as device_pc,
                a.device_pc_streams_pct as device_pc_pct,
                --a.device_other_streams as device_other,
                a.device_other_streams_pct as device_other_pct,
                --a.device_tablet_streams as device_tablet,
                a.device_tablet_streams_pct as device_tablet_pct,
                --a.device_gaming_console_streams as device_console,
                a.device_gaming_console_streams_pct as device_console_pct,
                --a.device_connected_audio_streams as device_connected_audio,
                a.device_connected_audio_streams_pct as device_connected_audio_pct,
                --a.device_smarttv_streams as device_smart_tv,
                a.device_smarttv_streams_pct as device_smart_tv_pct,
                --a.device_wearable_streams as device_wearable,
                a.device_wearable_streams_pct as device_wearable_pct,
                --a.device_car_streams as device_car,
                a.device_car_streams_pct as device_car_pct,
                d.instrumentalness as no_vocals,
                d.duration,
                a.avg_listening,
                a.avg_stream_length
        from s_features as a
        left join track_repeat_users as b
        on a.isrc = b.isrc
        left join track_focused_users as c
        on a.isrc = c.isrc
        left join audio as d
        on a.isrc = d.isrc
        left join avg_accounts as e
        on a.isrc = e.isrc),


        drop_dup as (select *,
                        row_number() over(partition by report_date, isrc order by report_date, isrc) as row_num
                        from dataset)
        
        select * except(row_num) from drop_dup
        where row_num = 1;

