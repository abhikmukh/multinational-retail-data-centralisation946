with 
my_cte (year, new_time)
as
(select year, to_timestamp(CONCAT(year, '/', month, '/', day, '/',timestamp), 
						   'YYYY/MM/DD/HH24:MI:ss') as new_time from dim_date_times),
second_cte 
as
(select year,
lead(new_time, 0)
over
(order by new_time) as timeframe
from my_cte
group by year, new_time),
third_cte  
as
(select year, timeframe, 
lead(timeframe, 1) over (order by year) as next_value
from second_cte),
fourth_cte
as (
select year,
	(next_value - timeframe) as difference
	from third_cte)
select year, avg(difference) from fourth_cte
group by year order by avg desc;

