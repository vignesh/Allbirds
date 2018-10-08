from loadDatabase import connect

# Report on quantity sold by month and product_type. Order sale date is in the " created_at_pacific_timestamp" column.
def queryOne():

	connection = connect()
	cursor = connection.cursor()

	cursor.execute("SELECT \
					SUM(skus_quantity.quantity) AS orders_sold, \
					skus_quantity.product_type AS product_type, \
					EXTRACT(YEAR FROM orders.created_at_pacific_timestamp) AS YEAR, \
					EXTRACT(MONTH FROM orders.created_at_pacific_timestamp) AS MONTH \
					FROM (SELECT \
						order_line_items.quantity AS quantity, \
						order_line_items.order_id AS order_id, \
						skus.product_type \
						FROM order_line_items \
						FULL JOIN skus \
						ON order_line_items.sku = skus.sku) AS skus_quantity \
					FULL JOIN orders \
					ON skus_quantity.order_id = orders.id \
					GROUP BY product_type, YEAR, MONTH \
					ORDER BY YEAR, MONTH ")

	result = cursor.fetchall()
	return result

# List email addresses of customers that ordered Runners before the first time they ordered Loungers.
def queryTwo():

	connection = connect()
	cursor = connection.cursor()

	cursor.execute("SELECT \
					runners.email \
					FROM \
						(SELECT \
						sku_email.email, \
						sku_email.created_at_pacific_timestamp, \
						skus.product_type AS product_type \
						FROM (SELECT \
						    orders.email AS email, \
						    orders.created_at_pacific_timestamp, \
						    order_line_items.sku \
						    FROM orders \
						    FULL JOIN order_line_items \
						    ON orders.id = order_line_items.order_id) AS sku_email \
						FULL JOIN skus \
						ON skus.sku = sku_email.sku \
						WHERE product_type = 'Runners') AS runners \
					LEFT JOIN \
						(SELECT \
						sku_email.email, \
						sku_email.created_at_pacific_timestamp, \
						skus.product_type AS product_type \
						FROM (SELECT \
						    orders.email AS email, \
						    orders.created_at_pacific_timestamp, \
						    order_line_items.sku \
						    FROM orders \
						    FULL JOIN order_line_items \
						    ON orders.id = order_line_items.order_id) AS sku_email \
						FULL JOIN skus \
						ON skus.sku = sku_email.sku \
						WHERE product_type = 'Loungers') AS loungers \
					ON runners.email = loungers.email \
					WHERE runners.created_at_pacific_timestamp < loungers.created_at_pacific_timestamp")

	result = cursor.fetchall()
	return result

# List email addresses that ordered Runners twice before the first time they ordered Loungers.
def queryThree():

	connection = connect()
	cursor = connection.cursor()

	cursor.execute("SELECT \
					runners.email \
					FROM \
						(SELECT \
						runners_one.email, \
						runners_two.created_at_pacific_timestamp \
						FROM \
						(SELECT \
							sku_email.email, \
							sku_email.created_at_pacific_timestamp, \
							skus.product_type AS product_type \
							FROM (SELECT \
							    orders.email AS email, \
							    orders.created_at_pacific_timestamp, \
							    order_line_items.sku \
							    FROM orders \
							    FULL JOIN order_line_items \
							    ON orders.id = order_line_items.order_id) AS sku_email \
							FULL JOIN skus \
							ON skus.sku = sku_email.sku \
							WHERE product_type = 'Runners') AS runners_one \
						LEFT JOIN \
						(SELECT \
							sku_email.email, \
							sku_email.created_at_pacific_timestamp, \
							skus.product_type AS product_type \
							FROM (SELECT \
							    orders.email AS email, \
							    orders.created_at_pacific_timestamp, \
							    order_line_items.sku \
							    FROM orders \
							    FULL JOIN order_line_items \
							    ON orders.id = order_line_items.order_id) AS sku_email \
							FULL JOIN skus \
							ON skus.sku = sku_email.sku \
							WHERE product_type = 'Runners') AS runners_two \
						ON runners_one.email = runners_two.email \
						WHERE runners_one.created_at_pacific_timestamp < runners_two.created_at_pacific_timestamp) AS runners \
					LEFT JOIN \
						(SELECT \
						sku_email.email, \
						sku_email.created_at_pacific_timestamp, \
						skus.product_type AS product_type \
						FROM (SELECT \
						    orders.email AS email, \
						    orders.created_at_pacific_timestamp, \
						    order_line_items.sku \
						    FROM orders \
						    FULL JOIN order_line_items \
						    ON orders.id = order_line_items.order_id) AS sku_email \
						FULL JOIN skus \
						ON skus.sku = sku_email.sku \
						WHERE product_type = 'Loungers') AS loungers \
					ON runners.email = loungers.email \
					WHERE runners.created_at_pacific_timestamp < loungers.created_at_pacific_timestamp")
	
	result = cursor.fetchall()
	return result


if __name__ == "__main__":
	queryOne()
	queryTwo()
	queryThree()
	

"""
1.	Report on quantity sold by month and product_type. Order sale date is in the " created_at_pacific_timestamp" column.
SELECT
	SUM(skus_quantity.quantity) AS orders_sold, 
	skus_quantity.product_type AS product_type,
	EXTRACT(YEAR FROM orders.created_at_pacific_timestamp) AS YEAR,
	EXTRACT(MONTH FROM orders.created_at_pacific_timestamp) AS MONTH
FROM (SELECT 
		order_line_items.quantity AS quantity,
		order_line_items.order_id AS order_id,
		skus.product_type
		FROM order_line_items
		FULL JOIN skus
		ON order_line_items.sku = skus.sku) AS skus_quantity
FULL JOIN orders
ON skus_quantity.order_id = orders.id
GROUP BY product_type, YEAR, MONTH
ORDER BY YEAR, MONTH

2.	List email addresses of customers that ordered Runners before the first time they ordered Loungers.
SELECT
runners.email
FROM
	(SELECT
	sku_email.email,
	sku_email.created_at_pacific_timestamp,
	skus.product_type AS product_type
	FROM (SELECT
	    orders.email AS email,
	    orders.created_at_pacific_timestamp,
	    order_line_items.sku
	    FROM orders
	    FULL JOIN order_line_items
	    ON orders.id = order_line_items.order_id) AS sku_email
	FULL JOIN skus
	ON skus.sku = sku_email.sku
	WHERE product_type = 'Runners') AS runners
LEFT JOIN 
	(SELECT
	sku_email.email,
	sku_email.created_at_pacific_timestamp,
	skus.product_type AS product_type
	FROM (SELECT
	    orders.email AS email,
	    orders.created_at_pacific_timestamp,
	    order_line_items.sku
	    FROM orders
	    FULL JOIN order_line_items
	    ON orders.id = order_line_items.order_id) AS sku_email
	FULL JOIN skus
	ON skus.sku = sku_email.sku
	WHERE product_type = 'Loungers') AS loungers
ON runners.email = loungers.email
WHERE runners.created_at_pacific_timestamp < loungers.created_at_pacific_timestamp

3.	List email addresses that ordered Runners twice before the first time they ordered Loungers.
SELECT
runners.email
FROM
	(SELECT
	runners_one.email,
	runners_two.created_at_pacific_timestamp
	FROM
	(SELECT
		sku_email.email,
		sku_email.created_at_pacific_timestamp,
		skus.product_type AS product_type
		FROM (SELECT
		    orders.email AS email,
		    orders.created_at_pacific_timestamp,
		    order_line_items.sku
		    FROM orders
		    FULL JOIN order_line_items
		    ON orders.id = order_line_items.order_id) AS sku_email
		FULL JOIN skus
		ON skus.sku = sku_email.sku
		WHERE product_type = 'Runners') AS runners_one
	LEFT JOIN
	(SELECT
		sku_email.email,
		sku_email.created_at_pacific_timestamp,
		skus.product_type AS product_type
		FROM (SELECT
		    orders.email AS email,
		    orders.created_at_pacific_timestamp,
		    order_line_items.sku
		    FROM orders
		    FULL JOIN order_line_items
		    ON orders.id = order_line_items.order_id) AS sku_email
		FULL JOIN skus
		ON skus.sku = sku_email.sku
		WHERE product_type = 'Runners') AS runners_two
	ON runners_one.email = runners_two.email
	WHERE runners_one.created_at_pacific_timestamp < runners_two.created_at_pacific_timestamp) AS runners
LEFT JOIN 
	(SELECT
	sku_email.email,
	sku_email.created_at_pacific_timestamp,
	skus.product_type AS product_type
	FROM (SELECT
	    orders.email AS email,
	    orders.created_at_pacific_timestamp,
	    order_line_items.sku
	    FROM orders
	    FULL JOIN order_line_items
	    ON orders.id = order_line_items.order_id) AS sku_email
	FULL JOIN skus
	ON skus.sku = sku_email.sku
	WHERE product_type = 'Loungers') AS loungers
ON runners.email = loungers.email
WHERE runners.created_at_pacific_timestamp < loungers.created_at_pacific_timestamp

4.	How (if at all) would you change this schema to better support queries of this kind?
- Create database views to hide complex queries 
"""