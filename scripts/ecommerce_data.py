from faker import Faker
import pandas as pd
import random

fake = Faker()

customers = []

for i in range(10000):
    customers.append({
        "customer_id": i+1,
        "name": fake.name(),
        "email": fake.email(),
        "signup_date": fake.date(),
        "country": random.choice(["India","USA","UK","Germany", "Canada", "Japan", "China"]),
        "marketing_source": random.choice(["Google Ads","Facebook Ads","Instagram", "Referral", "Email Campaign", "Organic"])
    })

df = pd.DataFrame(customers)
df.to_csv(r"C:\Users\Vidhesha\Desktop\ecommerce-analytics-platform\data\raw_data\customers.csv", index=False)

print("-------customers.csv generated Successfully--------")

products = []

categories = ["Electronics", "Sports", "Fashion", "Home", "Fitness"]

for i in range(500):
    products.append({
        "product_id": i+1,
        "product_name": fake.word().capitalize() + " Product",
        "category": random.choice(categories),
        "price": random.randint(10,5000)
    })

products_df = pd.DataFrame(products)

products_df.to_csv(
r"C:\Users\Vidhesha\Desktop\ecommerce-analytics-platform\data\raw_data\products.csv",
index=False
)

print("---------products.csv generated Successfully------------- ")

orders = []

for i in range(100000):
    quantity = random.randint(1, 10)
    price = random.randint(10, 5000)

    orders.append({
        "order_id": i+1,
        "customer_id": random.randint(1,10000),
        "product_id": random.randint(1,500),
        "order_timestamp": fake.date_time_this_year(),
        "price": price,
        "quantity": quantity,
        "order_total": price * quantity,
        "order_status": random.choice(["Completed","Cancelled","Returned"])
    })

orders_df = pd.DataFrame(orders)

orders_df.to_csv(
r"C:\Users\Vidhesha\Desktop\ecommerce-analytics-platform\data\raw_data\orders.csv",
index=False
)

print("---------orders.csv generated Successfully------------")

payments = []

for i in range(100000):
    payments.append({
        "payment_id": i+1,
        "order_id": i+1,
        "payment_method": random.choice(["Credit Card","Debit Card","UPI","Wallet", "Net Banking"]),
        "payment_amount": random.randint(100, 50000),
        "payment_status": random.choice(["success","failed_insufficient_funds","failed_invalid_card","failed_network_error"]),
        "payment_timestamp": fake.date_time_this_year()
    })

payments_df = pd.DataFrame(payments)

payments_df.to_csv(
r"C:\Users\Vidhesha\Desktop\ecommerce-analytics-platform\data\raw_data\payments.csv",
index=False
)

print("------------payments.csv generated Successfully-----------")

events = []

for i in range(200000):
    events.append({
        "event_id": i+1,
        "customer_id": random.randint(1,10000),
        "product_id":random.randint(1,500),
        "event_type": random.choice([
        "page_view",
        "product_view",
        "add_to_cart",
        "checkout"
        ]),
        "event_timestamp": fake.date_time_this_year(),
        "device_type": random.choice(["mobile","desktop","tablet"]),
        "session_duration": random.randint(5,600)
    })

events_df = pd.DataFrame(events)

events_df.to_csv(
r"C:\Users\Vidhesha\Desktop\ecommerce-analytics-platform\data\raw_data\app_events.csv",
index=False
)

print("---------app_events.csv generated successfully---------")
