-- Q1.Retrieve the duplicates based on PK in the below table
SELECT * FROM customers_sales
	WHERE customer_id NOT IN (
	SELECT customer_id from customers_sales
	GROUP BY customer_id
	HAVING COUNT(customer_id)>1)

-- Q2.Find the highest-paid employee in Each Department
SELECT
*
FROM employee_tbl
WHERE  (department_id, salary)     
IN     (SELECT department_id, MAX(salary)      
         FROM employee_tbl      
         GROUP BY department_id);

-- Q3.Find the Total Revenue Generated Each Month
SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, 
SUM(order_total) AS total_revenue
FROM orders_tbl
GROUP BY month
ORDER BY month;
