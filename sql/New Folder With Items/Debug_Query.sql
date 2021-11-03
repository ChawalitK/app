
with      
     analog_transition_kvab as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_kvbc as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][2])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_kvca as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][3])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_kvab_down as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '30 min') end_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '60 min') extend_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][1])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_ia as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_ia_down as (     
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '30 min') end_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '60 min') extend_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_ib as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][5])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_ic as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'       
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_pctpf as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" is not null     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     analog_transition_pctpf_down as (      
         SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '30 min') end_time,     
         (date_trunc('hour', ant."Timestamp" + (30 ||' minutes')::interval) + date_part('minute', ant."Timestamp" + (30 ||' minutes')::interval)::int / 30 * interval '60 min') extend_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN  ('+",".join(str(x) for x in keytagid[index][9])+')     
         and ant."Timestamp" >= '2021-02-06 00:00:00'      
         and ant."Timestamp" <= '2021-02-06 23:59:59.999'
         and ant."Value" = '0.00'     
         ORDER BY start_time ASC,ant."Timestamp" ASC, ant."TimestampIndex" ASC      
     ),     
     join_records as (     
         select kvab.start_time,kvab."Value" as kVAB,kvbc."Value" as kVBC,kvca."Value" as kVCA,     
         (CASE WHEN ia."Value" <= '+setzero[index]+' THEN 0.00 ELSE ia."Value" END) as IA,     
         (CASE WHEN ib."Value" <= '+setzero[index]+' THEN 0.00 ELSE ib."Value" END) as IB,     
         (CASE WHEN ic."Value" <= '+setzero[index]+' THEN 0.00 ELSE ic."Value" END) as IC,     
         (CASE WHEN pctpf."Value" > 100 THEN pctpf."Value"/100 WHEN pctpf."Value" < -100 THEN pctpf."Value"/100 ELSE pctpf."Value" END)as pctPF,     
         kvab_down."Value" as kVAB_down,     
         kvab_down."Timestamp" as kVAB_downtime,     
         kvab_down."start_time" as kVAB_downtime_start_time,     
         kvab_down."end_time" as kVAB_downtime_end_time,     
         ia_down."Value" as IA_down,     
         ia_down."Timestamp" as IA_downtime,     
         ia_down."start_time" as IA_downtime_start_time,     
         ia_down."end_time" as IA_downtime_end_time,     
         pctpf_down."Value" as pctPF_down,     
         pctpf_down."Timestamp" as pctPF_downtime,     
         pctpf_down."start_time" as pctPF_downtime_start_time,     
         pctpf_down."end_time" as pctPF_downtime_end_time,     
         kvab."Timestamp" as kVAB_firstseentime      
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
         ON kvab.start_time = kvab_down.end_time     
         LEFT JOIN analog_transition_ia_down ia_down     
         ON kvab.start_time = ia_down.end_time     
         LEFT JOIN analog_transition_pctpf_down pctpf_down     
         ON kvab.start_time = pctpf_down.end_time     
     ),     
     prior_ia as (     
         select start_time,     
         (select (CASE WHEN "Value" <= '+setzero[index]+' THEN 0.00 ELSE "Value" END) as priorIA from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 days')     
         and ant."Value" is not null     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where IA IS NULL     
     ),     
     prior_ia_down as (     
         select start_time,     
         (select (CASE WHEN "Value" <= '+setzero[index]+' THEN 0.00 ELSE "Value" END) as priorIA from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 days')     
         and ant."Value" = '0.00'     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where IA IS NULL     
     ),     
     prior_ib as (     
         select start_time,     
         (select (CASE WHEN "Value" <= '+setzero[index]+' THEN 0.00 ELSE "Value" END) as priorIB from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][4])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 days')      
         and ant."Value" is not null     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where IB IS NULL     
     ),     
     prior_ic as (     
         select start_time,     
         (select (CASE WHEN "Value" <= '+setzero[index]+' THEN 0.00 ELSE "Value" END) as priorIC from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][6])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 days')      
         and ant."Value" is not null     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where IC IS NULL     
     ),     
     prior_pctpf as (     
         select start_time,     
         (select (CASE WHEN "Value" > 100 THEN "Value"/100 WHEN "Value" < -100 THEN "Value"/100 ELSE "Value" END) as priorpctPF from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][9])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 day')      
         and ant."Value" is not null     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where pctPF IS NULL     
     ),     
     prior_pctpf_down as (     
         select start_time,     
         (select (CASE WHEN "Value" > 100 THEN "Value"/100 WHEN "Value" < -100 THEN "Value"/100 ELSE "Value" END) as priorpctPF from (SELECT DISTINCT ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min'))     
         (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,     
         ant."Timestamp",ant."Value",ant."TimestampIndex"     
         from "AnalogTransition" ant      
         where ant."KeyTag" IN ('+",".join(str(x) for x in keytagid[index][9])+')     
         and ant."Timestamp" < js.start_time     
         and ant."Timestamp" > (js.start_time - INTERVAL '1 day')      
         and ant."Value" = '0.00'     
         ORDER BY start_time DESC,ant."Timestamp" ASC, ant."TimestampIndex" ASC limit 1) AS prior)     
         from join_records js where pctPF IS NULL     
     ),     
     result_records as (     
         select js.start_time,     
         COALESCE(js.kVAB_down, js.kVAB) as kVAB,     
         COALESCE(js.kVAB_down, js.kVBC) as kVBC,     
         COALESCE(js.kVAB_down, js.kVCA) as kVCA,     
         COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) as IA,     
         COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) as IB,     
         COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))) as IC,     
         COALESCE(js.pctPF_down, COALESCE(js.pctPF, COALESCE(p_prior_pctpf_down.priorpctPF, p_prior_pctpf.priorpctPF))) as pctPF,     
         (select ((1.732*(((COALESCE(js.kVAB_down, js.kVAB)+COALESCE(js.kVAB_down, js.kVBC)+COALESCE(js.kVAB_down, js.kVCA))*1000)/3) * '\
         ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) +      
          COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) +       
          COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))))/3) * '\
         (COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF))/100))/1000000)) as MW,     
         (select ((1.732*(((COALESCE(js.kVAB_down, js.kVAB)+COALESCE(js.kVAB_down, js.kVBC)+COALESCE(js.kVAB_down, js.kVCA))*1000)/3) *  '\
         ((COALESCE(js.IA_down, COALESCE(js.IA, COALESCE(p_ia_down.priorIA, p_ia.priorIA))) + '\
         COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ib.priorIB))) + '\
         COALESCE(js.IA_down, COALESCE(js.IB, COALESCE(p_ia_down.priorIA, p_ic.priorIC))))/3) * '\
         (SIN(ACOS(COALESCE(js.pctPF_down, COALESCE(js.pctPF, p_prior_pctpf.priorpctPF))/100))))/1000000)) as MVar      
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
         prior_ia_down p_ia_down      
         ON js.start_time = p_ia_down.start_time     
         LEFT JOIN     
         prior_pctpf_down p_prior_pctpf_down     
         ON js.start_time = p_prior_pctpf_down.start_time     
     ),     
     timeseries as (     
         select      
         (select ''+report_date+''::date ) + ( n || ' minutes')::interval start_time      
        from generate_series(0, (24*60), 30) n limit 48     
     )     
     select ts.start_time as start_time,rr.kVAB as kVAB,rr.kVBC,rr.kVCA,rr.IA,rr.IB,rr.IC,rr.pctPF,rr.MW,rr.MVar from result_records rr     
      full join timeseries ts on rr.start_time::timestamp WITHOUT TIME ZONE = ts.start_time  where ts.start_time >= '2021-02-06 00:00:00'     
      and ts.start_time <=   '2021-02-06 23:59:59.999' order by ts.start_time ASC'

