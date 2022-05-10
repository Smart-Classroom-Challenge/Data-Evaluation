-- overview:
-- https://docs.timescale.com/timescaledb/latest/how-to-guides/query-data/advanced-analytic-queries/#histogram
-- performance difference:
-- https://www.timescale.com/blog/timescaledb-vs-6a696248104e/

-- get median temperature for a measurement station
SELECT percentile_cont(0.5)
       WITHIN GROUP (ORDER BY temperature)
FROM smartclassroom_dev.public.api_measurement
WHERE fk_measurement_station_id = 1;

-- moving average
SELECT time,
       AVG(temperature) OVER (ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
           AS smooth_temp
FROM smartclassroom_dev.public.api_measurement
WHERE fk_measurement_station_id = 1
  and time > NOW() - INTERVAL '1 day'
ORDER BY time DESC;

-- delta values - only get values that have changed in result set
SELECT time, temperature
FROM (
         SELECT time,
                temperature,
                temperature - LAG(temperature) OVER (ORDER BY time) AS diff
         FROM smartclassroom_dev.public.api_measurement
         WHERE fk_measurement_station_id = 1) ht
WHERE diff IS NULL
   OR diff != 0;

-- get average temperature over a time buckets of 5 minutes
SELECT time_bucket('1 day', time) AS five_min, round(max(temperature), 2)
FROM smartclassroom_dev.public.api_measurement
WHERE fk_measurement_station_id = 1
GROUP BY five_min
ORDER BY five_min DESC
LIMIT 100;

-- histogram - get temperature values in 8 bins between 15°C and 35°C, which makes a bin range of 2.5°C
SELECT ms.name,
       COUNT(*),
       histogram(m.temperature, 15.0, 35.0, 8)
FROM smartclassroom_dev.public.api_measurement m
         INNER JOIN smartclassroom_dev.public.api_measurementstation ms ON m.fk_measurement_station_id = ms.id
WHERE time > NOW() - INTERVAL '14 days'
GROUP BY ms.name;

-- time ordering - should make a big difference between base Postgres and TimescaleDB
SELECT date_trunc('minute', time) AS minute, max(temperature)
FROM smartclassroom_dev.public.api_measurement
WHERE time > NOW() - INTERVAL '14 days' AND fk_measurement_station_id = 1
GROUP BY minute
ORDER BY minute ASC;
