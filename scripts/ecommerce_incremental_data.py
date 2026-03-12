from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()

DATA_PATH = "/root/projects/ecommerce-analytics-platform/data/raw_data/"


# Read existing CSV files
orders_df = pd.read_csv(DATA_PATH + "orders.csv")
payments_df = pd.read_csv(DATA_PATH + "payments.csv")
customers_df = pd.read_csv(DATA_PATH + "customers.csv")
events_df = pd.read_csv(DATA_PATH + "app_events.csv")


# Get last IDs
last_order_id = orders_df["order_id"].max()
last_payment_id = payments_df["payment_id"].max()
last_customer_id = customers_df["customer_id"].max()
last_event_id = events_df["event_id"].max()

current_max_customer = last_customer_id
today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Generate new customers
new_customers = []
num_customers = random.randint(25, 50)

for i in range(1, num_customers + 1):
    new_customers.append({
        "customer_id": last_customer_id + i,
        "name": fake.name(),
        "email": fake.email(),
        "signup_date": datetime.now().date(),
	"country": random.choice(["India","USA","UK","Germany", "Canada", "Japan", "China"]),
        "marketing_source": random.choice(["Google Ads","Facebook Ads","Instagram", "Referral", "Email Campaign", "Organic"])
    })

new_customers_df = pd.DataFrame(new_customers)


# Generate new orders
new_orders = []
num_orders = random.randint(1000, 1050)

for i in range(1, num_orders + 1):

    order_id = last_order_id + i
    customer_id = random.randint(1, current_max_customer + num_customers)
    product_id = random.randint(1,500)
    price = round(random.uniform(10, 5000), 2)
    quantity = random.randint(0,10)

    new_orders.append({
        "order_id": order_id,
        "customer_id": customer_id,
	"product_id": product_id,
	"order_timestamp": today_start + timedelta(seconds=random.randint(0, 86400)),
        "price": price,
	"quantity": quantity,
	"order_total": price * quantity,
	"order_status": random.choice(["Completed","Cancelled","Returned"])
    })

new_orders_df = pd.DataFrame(new_orders)


# Generate payments
new_payments = []
num_payments = num_orders

for i in range(1, num_payments + 1):

    payment_id = last_payment_id + i
    order_id = last_order_id + i
    status = random.choice(["success","failed_insufficient_funds","failed_invalid_card","failed_network_error"])
    method = random.choice(["Credit Card","Debit Card","UPI","Wallet", "Net Banking"])
    amount = amount = new_orders[i-1]["order_total"]
    timestamp = today_start + timedelta(seconds=random.randint(0, 86400))

    new_payments.append({
        "payment_id": payment_id,
        "order_id": order_id,
	"payment_method": method,
	"payment_amount": amount,
        "payment_status": status,
        "payment_timestamp": timestamp
    })

new_payments_df = pd.DataFrame(new_payments)


# Generate app events
new_events = []
num_events = random.randint(5000, 5050)
event_types = ["page_view", "product_view", "add_to_cart", "checkout"]

for i in range(1, num_events + 1):

    new_events.append({
        "event_id": last_event_id + i,
        "customer_id": random.randint(1, current_max_customer + num_customers),
	"product_id": random.randint(1, 500),
        "event_type": random.choice(event_types),
        "event_timestamp": today_start + timedelta(seconds=random.randint(0, 86400)),
	"device_type": random.choice(["mobile", "desktop", "tablet"]),
	"session_duration": random.randint(5,600)
    })

new_events_df = pd.DataFrame(new_events)


# Append to CSV
INCREMENTAL_PATH = DATA_PATH + "incremental/"

new_customers_df.to_csv(INCREMENTAL_PATH + f"customers_{file_timestamp}.csv", index=False)
new_orders_df.to_csv(INCREMENTAL_PATH + f"orders_{file_timestamp}.csv", index=False)
new_payments_df.to_csv(INCREMENTAL_PATH + f"payments_{file_timestamp}.csv", index=False)
new_events_df.to_csv(INCREMENTAL_PATH + f"app_events_{file_timestamp}.csv", index=False)


print("Incremental data generated successfully!")
print(f"Customers added: {num_customers}")
print(f"Payments added: {num_payments}")
print(f"Orders added: {num_orders}")
print(f"Events added: {num_events}")
