"""Sql querys

    These querys to add 3 column (holiday_sunday, saturday, day_time) to the table journey
    and creat table sample_10 a sample 10% of the table journey

    Author: Roaa MASRI
"""
-- Table journey
/* Delete all rows that didn't have last_pt_coord/first_pt_coord
Delete from transport.journey WHERE last_pt_coord_lon is null; */

/* Delet all rows "type" not in ('best', 'fastest', 'rapid')
DELETE from transport.journey Where "type" not in ('best', 'fastest', 'rapid'); */

/* added 3 columns arrival_time, departure_time, journey_date
ALTER TABLE transport.journey
ADD COLUMN arrival_time time,
ADD COLUMN departure_time time,
ADD COLUMN journey_date date;

UPDATE transport.journey
SET arrival_time = to_timestamp("arrival_date_time")::time,
departure_time = to_timestamp("departure_date_time")::time,
journey_date = to_timestamp("arrival_date_time")::date; */

/* adding holiday_sunday, saturday :
ADD COLUMN holiday_Sunday boolean SET default FALSE,
ADD COLUMN saturday boolean SET default FALSE; */

/* calculate saturday column
UPDATE transport.journey
SET saturday = 'TRUE' where  journey_date in ('2018-09-01','2018-09-08','2018-09-15', '2018-09-22','2018-09-29'); 
*/

/* calculate holiday_Sunday column
UPDATE transport.journey
SET holiday_Sunday = 'TRUE' where  journey_date in ('2018-09-02','2018-09-09','2018-09-16', '2018-09-23', '2018-09-30');
*/

/* New column day_time
ALTER TABLE transport.journey
ADD COLUMN day_time varchar;

UPDATE transport.journey
SET day_time =
CASE  
	WHEN departure_time BETWEEN '07:00:00' AND '09:00:00' THEN 'morning_peak' 
	WHEN departure_time BETWEEN '17:00:00' AND '20:00:00' THEN 'evening_peak' 
	WHEN departure_time BETWEEN '12:00:00' AND '14:00:00' THEN 'midday'
END; */


/* create transport.indicateur4
create table transport.indicateur4 as ( select * from transport.journey);
*/

/* Delete data not at 09 month transport.indicateur4
Delete from transport.indicateur4
where date_part('month',journey_date) != '09';
38812*/

/*
ALTER TABLE transport.indicateur4
DROP COLUMN from_coord_lat,
DROP COLUMN from_coord_lon,
DROP COLUMN to_coord_lat,
DROP COLUMN to_coord_lon;
*/

/* DELETE duration <= 2min
Delete from transport.indicateur4
where duration <= 120;
*/
/* total number of rows
SELECT count(*) FROM transport.indicateur4;
2,493,903
*/

/* TABLE transport.min_duration
CREATE TABLE transport.min_duration as(
WITH min_duration as(
SELECT MIN(duration) as d,request_id as "id" FROM transport.indicateur4
GROUP BY request_id )
SELECT * FROM min_duration join transport.indicateur4 on min_duration.id = transport.indicateur4.request_id and
min_duration.d =transport.indicateur4.duration );
*/

/* DELET repeated request_id
ALTER TABLE transport.min_duration add column "id" serial;
DELETE FROM
    transport.min_duration a
        USING transport.min_duration b
WHERE
    a.id < b.id
    AND a.request_id = b.request_id;
ALTER TABLE transport.min_duration
DROP COLUMN "id";
ALTER TABLE transport.min_duration add column "id" serial;
*/

/* Create Sample (10%) to analyse
CREATE TABLE transport.sample as(
Select * from transport.min_duration TABLESAMPLE BERNOULLI (10)); 

ALTER TABLE transport.sample
DROP COLUMN "id";
ALTER TABLE transport.sample add column "id" serial;

-- preparing column to be completed from API
ALTER TABLE transport.sample
ADD COLUMN bike_distance int,
ADD COLUMN bike_time int,
ADD COLUMN car_distance int,
ADD COLUMN car_time int,
ADD COLUMN walk_distance int,
ADD COLUMN walk_time int;
*/