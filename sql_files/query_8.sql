
select ds.store_type, ds.country_code, sum(ot.product_quantity * dp.product_price) as total_sales 
from dim_store_details ds, orders_table ot, dim_products dp
where country_code = 'DE'
and 
ot.product_code = dp.product_code
and 
ds.store_code = ot.store_code
group by ds.store_type, ds.country_code
order by total_sales;