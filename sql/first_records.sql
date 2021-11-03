		 first_records as(
			select
			('2000-01-01 00:00:00'::timestamp with time zone) start_time,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12200000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVAB,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12220000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVBC,			
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12240000)
			 and ant."Timestamp" < '2021-02-01 23:00:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as kVCA,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12140000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IA,			 
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12160000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IB,			 			 
			 (select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12180000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as IC,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12320000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as MW,
			(select ant."Value" from "AnalogTransition" ant where ant."KeyTag" IN (12300000)
			 and ant."Timestamp" < '2021-02-01 23:30:00' and ant."Value" is not null     
       ORDER BY ant."Timestamp" DESC, ant."TimestampIndex" DESC limit 1) as Mvar
		 ),
		 down_records as (
			 SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min')     
			 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')+ interval '30 min' start_time,     
			 ant."Timestamp",ant."Value",ant."TimestampIndex"     
			 from "AnalogTransition" ant      
			 where (ant."KeyTag" IN  (12200000)
			 or ant."KeyTag" IN  (12220000)
			 or ant."KeyTag" IN  (12240000)			 
			 or ant."KeyTag" IN  (12140000)			 
			 or ant."KeyTag" IN  (12160000)			 
			 or ant."KeyTag" IN  (12180000)			 
			 or ant."KeyTag" IN  (12320000)
			 or ant."KeyTag" IN  (12300000)						
			 )     
			 and ant."Timestamp" >= '2021-02-01 23:00:00'      
			 and ant."Timestamp" <= '2021-02-02 23:59:59.999'
			 and ant."Value" = '0.00'     
			 ORDER BY  start_time ASC, ant."Timestamp" DESC
     ),