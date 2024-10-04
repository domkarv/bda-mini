from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI
db = client["bda"]  # Replace with your database name
collection = db["ecommerce"]  # Replace with your collection name

# ### #
# Aggregation pipeline for top selling products by quantity
pipeline_products = [
    {
        "$group": {
            "_id": "$StockCode",  # Group by StockCode
            "total_quantity": {
                "$sum": "$Quantity"  # Calculate total quantity sold
            },
        }
    },
    {
        "$sort": {"total_quantity": -1}  # Sort by total_quantity in descending order
    },
    {
        "$limit": 10  # Limit the result to top 10 products
    },
]

# Execute the aggregation
result_products = collection.aggregate(pipeline_products)

# Display results
print("Top Selling Products by Quantity:")
for doc in result_products:
    print(f"Product: {doc['_id']}, Total Quantity Sold: {doc['total_quantity']}")


# ### #
# Aggregation pipeline for top customers by spending
pipeline_customers = [
    {
        "$group": {
            "_id": "$CustomerID",  # Group by CustomerID
            "total_spending": {
                "$sum": {
                    "$multiply": ["$Quantity", "$UnitPrice"]  # Calculate total spending
                }
            },
        }
    },
    {
        "$sort": {"total_spending": -1}  # Sort by total_spending in descending order
    },
    {
        "$limit": 10  # Limit the result to top 10 customers
    },
]

# Execute the aggregation
result_customers = collection.aggregate(pipeline_customers)

# Display results
print("\nTop Customers by Spending:")
for doc in result_customers:
    print(f"CustomerID: {doc['_id']}, Total Spending: {doc['total_spending']:.2f}")


# ### #
# Aggregation pipeline for average order value by customer
pipeline_avg_order = [
    {
        "$group": {
            "_id": "$CustomerID",  # Group by CustomerID
            "total_spending": {
                "$sum": {
                    "$multiply": ["$Quantity", "$UnitPrice"]  # Calculate total spending
                }
            },
            "order_count": {
                "$sum": 1  # Count the number of orders
            },
        }
    },
    {
        "$project": {
            "CustomerID": "$_id",  # Include CustomerID
            "average_order_value": {
                "$divide": [
                    "$total_spending",
                    "$order_count",
                ]  # Calculate average order value
            },
        }
    },
    {
        "$sort": {
            "average_order_value": -1
        }  # Sort by average_order_value in descending order
    },
    {
        "$limit": 10  # Limit the result to top 10 customers
    },
]

# Execute the aggregation
result_avg_order = collection.aggregate(pipeline_avg_order)

# Display results
print("\nTop Customers by Average Order Value:")
for doc in result_avg_order:
    print(
        f"CustomerID: {doc['CustomerID']}, Average Order Value: {doc['average_order_value']:.2f}"
    )


# ### #
# Aggregation pipeline for frequent countries by number of orders
pipeline_countries = [
    {
        "$group": {
            "_id": "$Country",  # Group by Country
            "order_count": {
                "$sum": 1  # Count the number of orders
            },
        }
    },
    {
        "$sort": {"order_count": -1}  # Sort by order_count in descending order
    },
    {
        "$limit": 10  # Limit the result to top 10 countries
    },
]

# Execute the aggregation
result_countries = collection.aggregate(pipeline_countries)

# Display results
print("\nTop Countries by Number of Orders:")
for doc in result_countries:
    print(f"Country: {doc['_id']}, Number of Orders: {doc['order_count']}")


# ### #
# Aggregation pipeline for order frequency by hour
pipeline_hours = [
    {
        "$group": {
            "_id": {
                "$hour": {"$dateFromString": {"dateString": "$InvoiceDate"}}
            },  # Extract hour from InvoiceDate
            "order_count": {
                "$sum": 1  # Count the number of orders
            },
        }
    },
    {
        "$sort": {"order_count": -1}  # Sort by order_count in descending order
    },
    {
        "$limit": 10  # Limit the result to top 10 hours
    },
]

# Execute the aggregation
result_hours = collection.aggregate(pipeline_hours)

# Display results
print("\nTop Hours by Number of Orders:")
for doc in result_hours:
    print(f"Hour: {doc['_id']}, Number of Orders: {doc['order_count']}")
