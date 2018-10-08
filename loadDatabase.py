import sys
import psycopg2
import random
import string
from datetime import datetime, timedelta

# Connect to database
def connect():	

	try:
		connection = psycopg2.connect("dbname='allbirds' user='Vignesh' host='localhost'")
		return connection
	except Exception as e:
		print e
		sys.exit()

# Populate orders table 
def populateOrders(numberOfOrders):

	connection = connect()
	cursor = connection.cursor()
	
	ids = generateIds(numberOfOrders)
	emails = generateEmails(numberOfOrders)
	timestamps = generateTimestamps(numberOfOrders)

	for order in range(0, numberOfOrders-1):
		orderId = ids[order]
		orderEmail = random.choice(emails)
		orderTimestamp = timestamps[order]
		cursor.execute("insert into orders (id, email, created_at_pacific_timestamp) values (%s, %s, %s);", \
		((orderId,), (orderEmail,), (orderTimestamp,)))
	
	connection.commit()

# Generate random ids for order ids and order line ids
def generateIds(numberOfOrders):

	ids = random.sample(xrange(10000000), numberOfOrders)
	return ids

# Generate random emails of size 70% of number of orders. This is done to get orders from the same email (user)
def generateEmails(numberOfOrders):

	emails = []
	domains = ["@gmail.com", "@yahoo.com", "@hotmail.com", "aol.com", "msn.com", "outlook.com"]
	numberOfEmails = int(numberOfOrders * 0.7)
	for order in range(0, numberOfEmails):
		letters = string.ascii_lowercase
   		username = ''.join(random.choice(letters) for i in range(10))
   		domain = random.choice(domains)
   		email = username + domain
   		emails.append(email)
   	return emails

# Generate random timestamps from 2015 to 2018
def generateTimestamps(numberOfOrders):

	timestamps = []
	for order in range(0, numberOfOrders):
	  	timestamp = datetime.strftime(datetime(\
	  	random.randint(2015, 2018), \
	  	random.randint(1, 12), \
	  	random.randint(1, 28), \
	  	random.randrange(23), \
	  	random.randrange(59), \
	  	random.randrange(59), \
	  	random.randrange(1000000)), \
	  	'%Y-%m-%d %H:%M:%S')
	  	timestamps.append(timestamp)
	return timestamps

# Populate skus table
def populateSkus():

	connection = connect()
	cursor = connection.cursor()

	productTypes = ["Runners", "Loungers", "Skippers"]
	sizes = ["{:02d}".format(number) for number in range(4, 15)]
	productNames = ["Wool", "Tree"]
	colorNames = ["Charcoal", "Chalk", "Cobalt", "Peacock", "Coffee", "Zin", "Canary", "Navy"]
	colorHexes = {"Charcoal":"#4C4341", "Chalk":"#E0E0E0", "Cobalt":"#0E4487", "Peacock":"#12806D", \
	"Coffee":"#5C4030", "Zin":"#45171F", "Canary":"#DAD89F", "Navy":"#494F58"}

	for productType in productTypes:
		for size in sizes:
			for productName in productNames:
				for colorName in colorNames:
					colorHex = colorHexes[colorName]
					sku = productType[:3].upper() + str(size) + productName[:2].upper() + colorHex[:3]
					cursor.execute("insert into skus (sku, product_type, product_name, size, color_name, color_hex) values (%s, %s, %s, %s, %s, %s);", \
					((sku,), (productType,), (productName,), (size,), (colorName,), (colorHex,)))
	
	connection.commit()

# Populate order line items table
def populateOrderLineItems(numberOfOrders):

	connection = connect()
	cursor = connection.cursor()

	cursor.execute("SELECT id FROM orders")
	orders = cursor.fetchall()
	cursor.execute("SELECT sku FROM skus")
	skus = cursor.fetchall()

	orderLineNumbers = generateIds(numberOfOrders)
	quantities = ["{:02d}".format(number) for number in range(1, 4)]
	price = 95

	for i, order in enumerate(orders):
		orderId = order[0]
		orderLineNumber = orderLineNumbers[i]
		quantity =  random.choice(quantities)
		sku = random.choice(skus)[0]
		cursor.execute("insert into order_line_items (order_id, order_line_number, quantity, price, sku) values (%s, %s, %s, %s, %s);", \
		((orderId,), (orderLineNumber,), (quantity,), (price,), (sku,)))
	
	connection.commit()

# Populate allbirds database based on number of orders
def populateDatabase(numberOfOrders):
	populateOrders(numberOfOrders)
	populateSkus()
	populateOrderLineItems(numberOfOrders)

if __name__ == "__main__":
	populateDatabase(5000)
