with      
     analog_transition_kvab as (     
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_kvbc as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12220000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12220000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_kvca as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,					
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12240000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC)
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12240000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),
		     analog_transition_ia as (     
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_ib as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,					
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_ic as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12180000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12180000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_pctpf as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" is not null     
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),

     analog_transition_kvab_down as (     
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12200000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" = '0.00'   
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_kvbc_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12220000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'    
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12220000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" = '0.00'   
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_kvca_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,					
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12240000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'     
         ORDER BY start_time ASC, ant."Timestamp" DESC)
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') as start_time,
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,    				 
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12240000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" = '0.00'      
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ), 
     analog_transition_ia_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,					
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12140000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" = '0.00'   
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
		 
     analog_transition_ib_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,					
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00' 
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12160000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" = '0.00'   
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_ic_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12180000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'    
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN (12180000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'       
         and ant."Value" = '0.00' 
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),     
     analog_transition_pctpf_down as (      
         (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'   
         ORDER BY start_time ASC, ant."Timestamp" DESC) 
				 UNION 
				 (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
				 (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') + interval '30 min' as report_time,						
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  (12280000)     
         and ant."Timestamp" < '2021-02-06 00:00:00'
         and ant."Value" = '0.00'   
         ORDER BY start_time DESC, ant."Timestamp" DESC limit 2)
     ),
		 join_records as (     
         select kvab.start_time, kvab."Timestamp" as kVAB_lastseentime, kvab.report_time, kvab."Value" as kVAB, kvbc."Value" as kVBC, kvca."Value" as kVCA,     
         (CASE WHEN ia."Value" <= 13.00  THEN NULL ELSE ia."Value" END) as IA,     
         (CASE WHEN ib."Value" <= 13.00  THEN NULL ELSE ib."Value" END) as IB,     
         (CASE WHEN ic."Value" <= 13.00  THEN NULL ELSE ic."Value" END) as IC,     
         (CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END)as pctPF,     
         kvab_down."Value" as kVAB_down,     
         kvab_down."Timestamp" as kVAB_downtime,     
         kvab_down."start_time" as kVAB_downtime_start_time,
				 kvbc_down."Value" as kVBC_down,     
         kvbc_down."Timestamp" as kVBC_downtime,     
         kvbc_down."start_time" as kVBC_downtime_start_time,   
				 kvca_down."Value" as kVCA_down,     
         kvca_down."Timestamp" as kVCA_downtime,     
         kvca_down."start_time" as kVCA_downtime_start_time,
         ia_down."Value" as IA_down,     
         ia_down."Timestamp" as IA_downtime,     
         ia_down."start_time" as IA_downtime_start_time,     
				 ib_down."Value" as IB_down,     
         ib_down."Timestamp" as IB_downtime,     
         ib_down."start_time" as IB_downtime_start_time,  
         ic_down."Value" as IC_down,     
         ic_down."Timestamp" as IC_downtime,     
         ic_down."start_time" as IC_downtime_start_time, 
         pctpf_down."Value" as pctPF_down,     
         pctpf_down."Timestamp" as pctPF_downtime,     
         pctpf_down."start_time" as pctPF_downtime_start_time 
         from analog_transition_kvab kvab     
         LEFT JOIN analog_transition_kvbc kvbc     
         ON kvab.start_time = kvbc.start_time     
         LEFT JOIN analog_transition_kvca kvca     
         ON kvab.start_time = kvca.start_time      
         LEFT JOIN analog_transition_ia ia     
         ON kvab.start_time = ia.start_time     
         LEFT JOIN analog_transition_ib ib     
         ON kvab.start_time = ib.start_time      
         LEFT JOIN analog_transition_ic ic     
         ON kvab.start_time = ic.start_time      
         LEFT JOIN analog_transition_pctpf pctpf     
         ON kvab.start_time = pctpf.start_time 
         LEFT JOIN analog_transition_kvab_down kvab_down     
         ON kvab.start_time = kvab_down.start_time    
         LEFT JOIN analog_transition_kvbc_down kvbc_down     
         ON kvab.start_time = kvbc_down.start_time    
         LEFT JOIN analog_transition_kvbc_down kvca_down     
         ON kvab.start_time = kvca_down.start_time    

         LEFT JOIN analog_transition_ia_down ia_down     
         ON kvab.start_time = ia_down.start_time     
				 
         LEFT JOIN analog_transition_ib_down ib_down     
         ON kvab.start_time = ib_down.start_time   
         LEFT JOIN analog_transition_ic_down ic_down     
         ON kvab.start_time = ic_down.start_time   				
         LEFT JOIN analog_transition_pctpf_down pctpf_down     
         ON kvab.start_time = pctpf_down.start_time     
     ),
		 prior_ia as (     
         SELECT start_time, report_time, 
				 (SELECT IA FROM join_records WHERE start_time < js.start_time AND IA IS NOT NULL ORDER BY start_time DESC LIMIT 1) as IA
         FROM join_records js WHERE IA IS NULL     
     ),
		 prior_ib as (     
         SELECT start_time, report_time, 
				 (SELECT IB FROM join_records WHERE start_time < js.start_time AND IB IS NOT NULL ORDER BY start_time DESC LIMIT 1) as IB
         FROM join_records js WHERE IB IS NULL     
     ),
		 prior_ic as (     
         SELECT start_time, report_time, 
				 (SELECT IC FROM join_records WHERE start_time < js.start_time AND IC IS NOT NULL ORDER BY start_time DESC LIMIT 1) as IC
         FROM join_records js WHERE IC IS NULL     
     ),
		 prior_pctpf as (     
         SELECT start_time, report_time, 
				 (SELECT pctPF FROM join_records WHERE start_time < js.start_time AND pctPF IS NOT NULL ORDER BY start_time DESC LIMIT 1) as pctPF
         FROM join_records js WHERE pctPF IS NULL     
     ),
     timeseries as (     
         select      
         (select '2021-02-06'::date - INTERVAL '30 MIN' ) + ( n || ' minutes')::interval start_time      
        from generate_series(0, (24*60), 30) n limit 49     
     ),
     result_records as (     
         select js.start_time, js.report_time,
         COALESCE(js.kVAB_down, js.kVAB) as kVAB,     
         COALESCE(js.kVBC_down, js.kVBC) as kVBC,     
         COALESCE(js.kVCA_down, js.kVCA) as kVCA,     
         COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down."Value", p_ia.IA))) as IA,     
         COALESCE(js.IB_down, COALESCE(js.IB, COALESCE(p_ib_down."Value", p_ib.IB))) as IB,     
         COALESCE(js.IC_down, COALESCE(js.IC, COALESCE(p_ic_down."Value", p_ic.IC))) as IC,     
         COALESCE(js.pctPF_down, COALESCE(js.pctPF, COALESCE(p_prior_pctpf_down."Value", p_prior_pctpf.pctPF))) as pctPF,     
         (select ((1.732*(((COALESCE(js.kVAB_down, js.kVAB)+COALESCE(js.kVAB_down, js.kVBC)+COALESCE(js.kVAB_down, js.kVCA))*1000)/3) *  
         ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down."Value", p_ia.IA))) +      
          COALESCE(js.IB_down, COALESCE(js.IB, COALESCE(p_ib_down."Value", p_ib.IB))) +       
          COALESCE(js.IC_down, COALESCE(js.IC, COALESCE(p_ic_down."Value", p_ic.IC))))/3) *  
         (COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.pctPF))/100))/1000000)) as MW,     
         (select ((1.732*(((COALESCE(js.kVAB_down, js.kVAB)+COALESCE(js.kVAB_down, js.kVBC)+COALESCE(js.kVAB_down, js.kVCA))*1000)/3) *   
         ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down."Value", p_ia.IA))) +  
         COALESCE(js.IB_down, COALESCE(js.IB, COALESCE(p_ia_down."Value", p_ib.IB))) +  
         COALESCE(js.IC_down, COALESCE(js.IC, COALESCE(p_ia_down."Value", p_ic.IC))))/3) *  
         (SIN(ACOS(COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.pctPF))/100))))/1000000)) as MVar      
         from join_records js     
         LEFT JOIN prior_ia p_ia     
         ON js.start_time = p_ia.start_time     
         LEFT JOIN prior_ib p_ib     
         ON js.start_time = p_ib.start_time     
         LEFT JOIN prior_ic p_ic     
         ON js.start_time = p_ic.start_time     
         LEFT JOIN prior_pctpf p_prior_pctpf     
         ON js.start_time = p_prior_pctpf.start_time     
         LEFT JOIN     
         analog_transition_ia_down p_ia_down      
         ON js.start_time = p_ia_down.start_time
         LEFT JOIN     
         analog_transition_ib_down p_ib_down      
         ON js.start_time = p_ib_down.start_time  				 
         LEFT JOIN
         analog_transition_ic_down p_ic_down      
         ON js.start_time = p_ic_down.start_time  				 
         LEFT JOIN     
         analog_transition_pctpf_down p_prior_pctpf_down     
         ON js.start_time = p_prior_pctpf_down.start_time     
     )
		 
		 select ts.start_time as start_time, rr.report_time as report_time,rr.kVAB as kVAB,rr.kVBC,rr.kVCA,rr.IA,rr.IB,rr.IC,rr.pctPF,rr.MW,rr.MVar from result_records rr 
		 full join timeseries ts on rr.report_time::timestamp WITHOUT TIME ZONE = ts.start_time  
		 where ts.start_time >= '2021-02-06 00:00:00' and ts.start_time <= '2021-02-06 23:59:59.999'
		 order by ts.start_time ASC
		 