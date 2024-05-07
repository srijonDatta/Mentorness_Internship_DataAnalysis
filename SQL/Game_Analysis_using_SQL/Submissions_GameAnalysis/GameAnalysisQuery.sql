	
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------                                               Data Storing                                                                               ----------------------   
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- Creating the structure of the tables

CREATE TABLE player_details (
	P_ID int8 PRIMARY KEY,
	PName VARCHAR(50),
	L1_Status int4,
	L2_Status int4,
	L1_code VARCHAR(50),
	L2_code varchar(50)
);

select * from player_details;

CREATE TABLE level_details (
	P_ID int8,
	Dev_ID varchar(50),
	Time_Stamp timestamp,
	Stages_crossed int8,
	Level int4,
	Difficulty VARCHAR(50),
	Kill_Count int8,
	Headshots_Count int8,
	Score int8,
	Lives_Earned int2
);


-- Creating a staging table and named it 'staging_level_details' so that we can chanfe the datatype of the 'time_stamp' column and insert the values to the desired table, which in this case is 'level_details'

CREATE TABLE staging_level_details(
	P_ID int8,
	Dev_ID varchar(50),
	Time_Stamp text,
	Stages_crossed int8,
	Level int4,
	Difficulty VARCHAR(50),
	Kill_Count int8,
	Headshots_Count int8,
	Score int8,
	Lives_Earned int2
);


select * from staging_level_details;

SELECT * from level_details;


INSERT INTO level_details(p_id,dev_id,time_stamp,stages_crossed,"level",difficulty,kill_count,headshots_count,score,lives_earned)
SELECT  staging_level_details.p_id,
		staging_level_details.dev_id,
		TO_TIMESTAMP(staging_level_details.time_stamp, 'DD/MM/YY HH24:MI'),
		staging_level_details.stages_crossed,
		staging_level_details."level",
		staging_level_details.difficulty,
		staging_level_details.kill_count,
		staging_level_details.headshots_count,
		staging_level_details.score,
		staging_level_details.lives_earned
from staging_level_details;
	
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------                                               Queries                                                                                    ----------------------   
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




--  Q1. Extract 'P_ID', 'Dev_ID', 'PName', and 'Difficulty_level' of all players at level 0.

select level_details.p_id, level_details.dev_id, player_details.pname, level_details.difficulty
from level_details
join player_details on player_details.p_id = level_details.p_id
where level_details."level" = 0;


---------------------------  Query Varification ---------------------------

--select * from level_details
--where level = 0;

---------------------------------------------------------------------------




--  Q2. Find 'Level1_code' wise average 'Kill_Count' where 'lives_earned' is 2, and at least 3 stages are crossed.


with x as(
	select 
		player_details.p_id, 
		player_details.l1_code, 
		level_details.kill_count, 
		level_details.lives_earned,
		level_details.stages_crossed
	from player_details
	join level_details on player_details.p_id = level_details.p_id
	where level_details.lives_earned = 2 and level_details.stages_crossed > 2
	order by player_details.p_id)
	
select 
	x.p_id, 	
	x.l1_code,
-- 	x.kill_count,
	sum(x.kill_count) as Total,
	Round(avg(x.kill_count),2) as Average
from x	

group by 
	x.p_id, 	
	x.l1_code
-- 	x.kill_count
order by x.p_id;


---------------------------  Query Varification ---------------------------
-- select 
-- 		player_details.p_id, 
-- 		player_details.l1_code, 
-- 		level_details.kill_count, 
-- 		level_details.lives_earned,
-- 		level_details.stages_crossed
-- 	from player_details
-- 	join level_details on player_details.p_id = level_details.p_id
-- 	where level_details.lives_earned = 2 and level_details.stages_crossed > 2
-- 	order by player_details.p_id

---------------------------------------------------------------------------



-- Q3. Find the total number of stages crossed at each difficulty level for Level 2 with players using 'zm_series' devices. Arrange the result in decreasing order of the total number of stages crossed. 


---------------------------  Using Window Function ---------------------------

select 
	p_id,
	dev_id,
	stages_crossed,
	level,
	difficulty,
	sum(stages_crossed) over(partition by difficulty) as Total_stages_crossed
from level_details
where dev_id like '%zm%' and level = 2
order by Total_stages_crossed desc;

---------------------------------------------------------------------------



--------------------- Without sing Window Function ------------------------

with x as (
select 
	p_id,
	dev_id,
	stages_crossed,
	level,
	difficulty
from level_details
where dev_id like '%zm%' and level = 2
order by stages_crossed desc
)
select 
-- 	x.stages_crossed,
	x.difficulty,
	sum(x.stages_crossed) as Total_stages_crossed
from x
group by 
	difficulty;

---------------------------------------------------------------------------



-- Q4. Extract 'p_id' and the total number of unique dates for those players who have played games on multiple days.



SELECT 
    p_id, 
    COUNT(DISTINCT DATE(level_details.time_stamp)) AS unique_game_days
FROM 
    level_details
GROUP BY 
    p_id
HAVING 
    COUNT( DISTINCT DATE (level_details.time_stamp)) > 1;



---------------------------  Query Varification ---------------------------

-- select 
-- 	p_id,
-- 	DATE(time_stamp),
-- 	distinct count(DATE (time_stamp)) over(partition by p_id order by DATE (time_stamp)) as cnt
-- from level_details


---------------------------------------------------------------------------



-- Q5. Find 'p_id' and levelwise sum of 'kill_counts' where 'kill_count' is greater than the average kill count for Medium difficulty.



select 
	p_id,
	level,
	kill_count,
	sum(kill_count) over(partition by level order by p_id) as Level_wise_kill_counts
from level_details
where kill_count > (
					select 
					avg(kill_count) as Average_kill_count
					from level_details
					where difficulty like 'Medium');



---------------------------  Query Varification ---------------------------

-- average kill_count for medium difficulty

-- select 
-- 	avg(kill_count) as Average_kill_count
-- from level_details
-- where difficulty like 'Medium';



-- select 
-- 	p_id,
-- 	level,
-- 	kill_count
-- from level_details
-- where kill_count > (
-- 					select 
-- 					avg(kill_count) as Average_kill_count
-- 					from level_details
-- 					where difficulty like 'Medium')
-- order by level;


---------------------------------------------------------------------------




-- Q6. Find 'level' and its corresponding 'Level_code' wise sum of lives earned, excluding level 0. Arrange in ascending order of level.

select 
	level_details.p_id,
	player_details.p_id,
	level_details.level,
	player_details.l1_status,
	player_details.l2_status,
	player_details.l1_code,
	player_details.l2_code,
	level_details.lives_earned
from level_details
join player_details on level_details.p_id = player_details.p_id
where level_details.level <> 0
order by 
	level_details.level,
	level_details.p_id
	
	
	
-- Q7. Find the top 3 scores based on each 'Dev_id' and rank them in the increasing order using 'Row_Number'. Display the difficulty as well.


select * from (
	SELECT
		dev_id,
		score,
		difficulty,
		row_number() over(partition by level_details.dev_id order by level_details.score DESC) top_3_scores
	from 
		level_details)
where 
	top_3_scores <= 3



-- Q8. Find the 'first_login' datetime for each device id


select 
	dev_id,
	min(time_stamp) as First_login_time
from level_details
group by 
	dev_id
order by 
	dev_id



---------------------------  Query Varification ---------------------------


-- select 
-- 	dev_id,
-- 	min(time_stamp)
-- from level_details
-- where dev_id = 'bd_013'
-- group by dev_id


---------------------------------------------------------------------------


-- Q9. Find the top 5 scores based on each difficulty level and rank them in increasing order using `Rank`. Display `Dev_ID` as well.

select * from(
	select
		difficulty,
		score,
		dev_id,
		rank() OVER(PARTITION by difficulty order by score DESC) ranks
	from level_details
	)
where 
	ranks <= 5
order by
--      User defined oredering     --
	case difficulty                  
		when 'Low' then 1
		when 'Medium' then 2
		when 'High' then 3
	END, ranks
	


-- Q10. Find the device id that is first logged in (based on 'start_date_time') for each player id i.e. 'p_id'. Output should contain player id, device id and first login date time.


select 
	p_id,
	dev_id,
	time_stamp
from( 
	select 
		p_id,
		dev_id,
		time_stamp,
		row_number() over(partition by p_id order by time_stamp) RN
	from level_details)
where RN = 1
	


-- Q11. For each player and date, determine how many 'kill counts' were played by the player so far (a) using & (b) not using window function

-- Using window function:


with x as(
select
	p_id,
	date(time_stamp) time,
	sum(kill_count) total_kill_counts_for_each_player_by_date
from level_details
group by 1,2
order by p_id),
y as(
select
	p_id,
	date(time_stamp),
	sum(kill_count) over(partition by p_id) Total_kill_counts_per_player
from level_details
order by p_id),
distinct_y as(
select
	distinct p_id,
	Total_kill_counts_per_player
from y)

select x.p_id, x.time, x.total_kill_counts_for_each_player_by_date, distinct_y.Total_kill_counts_per_player from x join distinct_y on x.p_id = distinct_y.p_id 



-- Without using window function

WITH x AS (
    SELECT
        p_id,
        DATE(time_stamp) AS time,
        SUM(kill_count) AS total_kill_counts_for_each_player_by_date
    FROM
        level_details
    GROUP BY
        p_id,
        DATE(time_stamp)
    ORDER BY
        p_id
),
y AS (
    SELECT
        p_id,
        SUM(kill_count) AS total_kill_counts_per_player
    FROM
        level_details
    GROUP BY
        p_id
),
distinct_y AS (
    SELECT DISTINCT
        p_id,
        total_kill_counts_per_player
    FROM
        y
)
SELECT
    x.p_id,
    x.time,
    x.total_kill_counts_for_each_player_by_date,
    distinct_y.total_kill_counts_per_player
FROM
    x
JOIN
    distinct_y ON x.p_id = distinct_y.p_id;






-- Q12. Find the cumulative sum of stages crossed over 'start_datetime' for each 'p_id', excluding the most recent 'start_datetime'.


WITH ranked_stages AS (
    SELECT
        p_id,
        time_stamp,
        stages_crossed,
        ROW_NUMBER() OVER (PARTITION BY p_id ORDER BY time_stamp DESC) AS rn
    FROM
        level_details
)
SELECT
    p_id,
    time_stamp,
    stages_crossed,
    SUM(stages_crossed) OVER (PARTITION BY p_id ORDER BY time_stamp) AS cumulative_sum
FROM
    ranked_stages
WHERE
    rn > 1;




-- Q13. Extract the top 3 highest sums of scores for each 'Dev_id' and the corresponding 'p_id'.


with x as(
	select 
		p_id,
		dev_id,
		sum(score) as TOTAL,
		row_number() over(partition by dev_id order by sum(score) desc) as rn
	from level_details
	group by
		1,2
	order by 
		dev_id,
		3 DESC)
select * from x
where x.rn BETWEEN 1 and 3



-- Q14. Find players who scored more than 50% of the average score, scored by the sum of scores for each 'p_id'.

with x as(
select 
	p_id,
	score,
	round(avg(score) over(PARTITION by p_id),2) as average_scores_by_each_player 
from level_details
order by 
	p_id)


SELECT
	distinct x.p_id,
	player_details.pname,
	x.average_scores_by_each_player
from x
join player_details on x.p_id = player_details.p_id
where x.average_scores_by_each_player > (SELECT
									  	 	0.5 * avg(score) as average_scores
									   from
									  	level_details)
order by 
	3 desc



-- Q15. Create a stored procedure to find the top `n` `headshots_count` based on each `Dev_ID` and rank them in increasing order using `Row_Number`. Display the difficulty as well.






-- Using Procedure ------------------------------------------------------------


CREATE OR REPLACE PROCEDURE get_top_headshots(IN limit_n INT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Create a temporary table to store the results
    CREATE TEMP TABLE IF NOT EXISTS top_headshots AS
    SELECT 
        Dev_ID, 
        headshots_count, 
        difficulty, 
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY headshots_count ASC) AS rank
    FROM 
        level_details;

    -- Select only the top `n` rows for each Dev_ID based on the provided limit
--     RETURN table as
    SELECT 
        Dev_ID, 
        headshots_count, 
        difficulty
    FROM 
        top_headshots
    WHERE 
        rank <= limit_n
    ORDER BY 
        Dev_ID, rank;

    -- Optionally, you could drop or keep the temporary table depending on the requirement
    -- DROP TABLE top_headshots;
END;
$$;

-- DROP PROCEDURE get_top_head_shots(int)



-- Using user defined FUNCTIONS ---------------------------------------------------------

CREATE OR REPLACE FUNCTION GetTopHeadshots(n INTEGER)
RETURNS TABLE(D_ID varchar(50), H_Count BIGINT, difficulty varchar(50), rn BIGINT)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH RankedHeadshots AS (
        SELECT 
            ld.Dev_ID, 
            ld.headshots_count, 
            ld.difficulty,
            ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY headshots_count DESC) AS rn
        FROM 
            level_details ld
    )
    SELECT 
        r.Dev_ID, 
        r.headshots_count, 
        r.difficulty,
        r.rn
    FROM 
        RankedHeadshots r
    WHERE 
        r.rn <= n
    ORDER BY 
        r.Dev_ID, r.rn;
END;
$$;



-- checking the output by calling the function
SELECT * FROM GetTopHeadshots(5);








