/* get category, count, avg by month */
select 
	strftime('%Y-%m',transaction_date), 
	category, round(sum(Debit), 2), 
	count(debit), round(sum(debit) / count(debit), 2) 
from 
	transactions 
where 
	credit='' 
group by 
	category, 
	strftime('%Y', Transaction_date), 
	strftime('%m',Transaction_date) 
order by 
	transaction_date
