WITH new_df AS (
    SELECT user_id, order_cost, order_id, DATE_TRUNC('day', TO_TIMESTAMP(order_time)) AS date
    FROM orders
), 
activate_date as (select user_id, min(date) as activated_date
				from new_df
				group by user_id  
), 
Noactivate_date as (select user_id, min(date) as noactivated_date
				   from new_df 
					where date not in (select activated_date from activate_date)
					group by user_id
),	   
reactivate_date as (			  
select n.user_id, n.noactivated_date as reactivated_date from 
Noactivate_date n left join activate_date ac on n.user_id = ac.user_id
where n.noactivated_date >= interval '90' day + ac.activated_date
),
user_count_gvm_reactivated as (
select count(distinct r.user_id) users_count_reactivated, r.reactivated_date as date, sum(n.order_cost) gmv360d_reactivated
	from new_df n
	join reactivate_date r
	on r.user_id = n.user_id
		where r.user_id in (select user_id from reactivate_date) 
			and date between r.reactivated_date and r.reactivated_date + interval '360' day 
		group by r.reactivated_date
),
user_count_gvm_activated as (
select count(distinct ac.user_id) users_count_new, ac.activated_date as date, sum(n.order_cost) gmv360d_new
	from new_df n
	join activate_date ac
	on n.user_id = ac.user_id
		where ac.user_id in (select user_id from activate_date) 
			and date between ac.activated_date and ac.activated_date + interval '360' day 
		group by ac.activated_date
)
select date, users_count_new, gmv360d_new, users_count_reactivated, gmv360d_reactivated
from user_count_gvm_activated
	FULL OUTER JOIN user_count_gvm_reactivated
		USING(date)
order by date
	
