
with my_cte (store_type, total_sales)
as
(select sd.store_type as store_type, (sum(ot.product_quantity * dp.product_price) + 0.0) as total_sales 

from dim_store_details sd, orders_table ot, dim_products dp
where 
ot.product_code = dp.product_code
and
ot.product_code = dp.product_code
group by sd.store_type)
select store_type, total_sales, 
(total_sales * 100) / sum(total_sales) over () as percentage 
from my_cte 
group by store_type, total_sales
