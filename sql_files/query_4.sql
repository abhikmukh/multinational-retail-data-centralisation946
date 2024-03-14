with my_cte (store_code, store_type, new_store_type)
as
(select store_code, store_type,
case
when store_type = 'Web Portal' then 'Web Portal'
else 'Offline'
end as new_store_type
from dim_store_details)
select count(*) as numbers_of_sales, sum(ot.product_quantity) as pqc, cte.new_store_type
from orders_table ot, my_cte cte
where ot.store_code = cte.store_code
group by cte.new_store_type
;



