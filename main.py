from pymongo import MongoClient
import matplotlib.pyplot as plt

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
result_products = list(collection.aggregate(pipeline_products))

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
result_customers = list(collection.aggregate(pipeline_customers))

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
result_avg_order = list(collection.aggregate(pipeline_avg_order))

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
result_countries = list(collection.aggregate(pipeline_countries))

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
result_hours = list(collection.aggregate(pipeline_hours))

# Display results
print("\nTop Hours by Number of Orders:")
for doc in result_hours:
    print(f"Hour: {doc['_id']}, Number of Orders: {doc['order_count']}")

# ### #
# Plotting the results

# Top Selling Products by Quantity
product_names = [doc["_id"] for doc in result_products]
total_quantities = [doc["total_quantity"] for doc in result_products]

plt.figure(figsize=(10, 6))
plt.bar(product_names, total_quantities, color="skyblue")
plt.title("Top Selling Products by Quantity")
plt.xlabel("Product (Stock Code)")
plt.ylabel("Total Quantity Sold")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Top Customers by Spending
customer_ids = [doc["_id"] for doc in result_customers]
total_spendings = [doc["total_spending"] for doc in result_customers]

plt.figure(figsize=(10, 6))
plt.bar(customer_ids, total_spendings, color="lightgreen")
plt.title("Top Customers by Spending")
plt.xlabel("Customer ID")
plt.ylabel("Total Spending")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Top Customers by Average Order Value
avg_customer_ids = [doc["CustomerID"] for doc in result_avg_order]
avg_order_values = [doc["average_order_value"] for doc in result_avg_order]

plt.figure(figsize=(10, 6))
plt.bar(avg_customer_ids, avg_order_values, color="salmon")
plt.title("Top Customers by Average Order Value")
plt.xlabel("Customer ID")
plt.ylabel("Average Order Value")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Top Countries by Number of Orders
countries = [doc["_id"] for doc in result_countries]
order_counts = [doc["order_count"] for doc in result_countries]

plt.figure(figsize=(10, 6))
plt.bar(countries, order_counts, color="gold")
plt.title("Top Countries by Number of Orders")
plt.xlabel("Country")
plt.ylabel("Number of Orders")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Top Hours by Number of Orders
hours = [doc["_id"] for doc in result_hours]
hour_order_counts = [doc["order_count"] for doc in result_hours]

plt.figure(figsize=(10, 6))
plt.bar(hours, hour_order_counts, color="violet")
plt.title("Top Hours by Number of Orders")
plt.xlabel("Hour of Day")
plt.ylabel("Number of Orders")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
