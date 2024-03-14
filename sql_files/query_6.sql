select  dt.month, dt.year, sum(ot.product_quantity * dp.product_price) as total_sales
from orders_table ot, dim_products dp, dim_date_times dt
where ot.product_code = dp.product_code
and
ot.date_uuid = dt.date_uuid
group by month, year order by total_sales desc limit 10
; 