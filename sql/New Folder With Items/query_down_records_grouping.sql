


  SELECT DISTINCT
    ON ((date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min')) (date_trunc('hour', ant."Timestamp") + date_part('minute', ant."Timestamp")::int / 30 * interval '30 min') start_time,
    ant."Timestamp",
    ant."Value",
    ant."TimestampIndex" 
  from
    "AnalogTransition" ant 
  where
    ant."KeyTag" IN 
    (
      13680000
    )
    and ant."Timestamp" >= '2021-01-26 00:00:00' 
    and ant."Timestamp" <= '2021-05-26 23:59:59' 
  ORDER BY
    start_time ASC,
    ant."Timestamp" ASC,
    ant."TimestampIndex" ASC