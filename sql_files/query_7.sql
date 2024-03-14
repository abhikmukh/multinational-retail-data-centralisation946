select country_code, sum(staff_numbers) as total_staff_numbers from dim_store_details
group by country_code 
order by total_staff_numbers desc ;