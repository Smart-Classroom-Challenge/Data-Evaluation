-- get average co2 over a time buckets of 10 minutes
SELECT ts,
       mean,
       median,
       mean - LAG(mean) OVER (ORDER BY ts)     AS mean_diff,
       median - LAG(median) OVER (ORDER BY ts) AS median_diff
FROM (
         SELECT time_bucket('5 minutes', time)                   AS ts,
                round(avg(co2), 2)                               AS mean,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY co2) AS median
         FROM api_measurement
                  INNER JOIN api_measurementstation
                             ON api_measurementstation.id = api_measurement.fk_measurement_station_id
                  INNER JOIN api_classroom ON api_classroom.id = api_measurementstation.fk_classroom_id
         WHERE api_classroom.name = '{name}'
         GROUP BY ts
         ORDER BY ts
     ) buckets;

-- VIEW for Primarklasse_EG
CREATE VIEW timebuckets_Primarklasse_EG AS
SELECT ts,
       mean,
       median,
       mean - LAG(mean) OVER (ORDER BY ts)     AS mean_diff,
       median - LAG(median) OVER (ORDER BY ts) AS median_diff
FROM (
         SELECT time_bucket('5 minutes', time)                   AS ts,
                round(avg(co2), 2)                               AS mean,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY co2) AS median
         FROM api_measurement
                  INNER JOIN api_measurementstation
                             ON api_measurementstation.id = api_measurement.fk_measurement_station_id
                  INNER JOIN api_classroom ON api_classroom.id = api_measurementstation.fk_classroom_id
         WHERE api_classroom.name = 'Primarklasse_EG'
         GROUP BY ts
         ORDER BY ts
     ) buckets;