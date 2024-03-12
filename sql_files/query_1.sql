select country_code, count(*) as store_numbers from dim_store_details group by country_code order by store_numbers desc;

