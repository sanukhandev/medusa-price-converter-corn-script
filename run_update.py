import json
import time
import psycopg2
import pandas as pd
import uuid
# Function to convert SGD to another currency


# CONFIG
BASE_CURRENCY = 'SGD'


def generate_id(prefix):
    unique_id = uuid.uuid4().hex.upper()[0:24]
    return f"{prefix}{unique_id}"


def convert_sgd(amount, rate):
    return ((amount/100) * rate)*100


time_start = time.time()

# Load exchange rates from latest.json
with open('latest.json', 'r') as file:
    data = json.load(file)
    usd_rate = data['rates']['USD']
    cad_rate = data['rates']['CAD']
    aud_rate = data['rates']['AUD']

# Database connection
conn_string = "<Removed_for_security>"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Fetch SGD prices for the specific variant

query_non_SGD = """
SELECT ma.id FROM money_amount ma WHERE ma.currency_code != 'sgd'
"""
query = """
SELECT ma.id, ma.amount, pvma.variant_id 
FROM money_amount ma
JOIN product_variant_money_amount pvma ON pvma.money_amount_id = ma.id
WHERE ma.currency_code = 'sgd' 
"""

# delete all non sgd prices
cursor.execute(query_non_SGD)
non_sgd_prices = cursor.fetchall()
for non_sgd_price in non_sgd_prices:
    non_sgd_id = non_sgd_price[0]
    cursor.execute(
        f"DELETE FROM product_variant_money_amount WHERE money_amount_id = '{non_sgd_id}'")
    cursor.execute(f"DELETE FROM money_amount WHERE id = '{non_sgd_id}'")

cursor.execute(query)
sgd_prices = cursor.fetchall()


# Prepare SQL for inserting new prices
insert_price_sql = """
INSERT INTO money_amount (id,currency_code, amount, created_at, updated_at)
VALUES (%s,%s, %s, NOW(), NOW())
RETURNING id;
"""

# Prepare SQL for associating prices with the specific product variant
insert_variant_sql = """
INSERT INTO product_variant_money_amount (id,money_amount_id, variant_id, created_at, updated_at)
VALUES (%s,%s, %s, NOW(), NOW());
"""

# Process each SGD price
for sgd_price in sgd_prices:
    sgd_id, amount, variant_id = sgd_price

    # Convert amounts
    usd_amount = convert_sgd(amount, usd_rate)
    cad_amount = convert_sgd(amount, cad_rate)
    aud_amount = convert_sgd(amount, aud_rate)

    # Insert USD, CAD, AUD prices and get their new IDs
    cursor.execute(insert_price_sql, (generate_id('ma_'), 'usd', usd_amount))
    usd_id = cursor.fetchone()[0]

    cursor.execute(insert_price_sql, (generate_id('ma_'), 'cad', cad_amount))
    cad_id = cursor.fetchone()[0]

    cursor.execute(insert_price_sql, (generate_id('ma_'), 'aud', aud_amount))
    aud_id = cursor.fetchone()[0]

    # Associate new prices with the specific product variant
    cursor.execute(insert_variant_sql,
                   (generate_id('pvma_'), usd_id, variant_id))
    cursor.execute(insert_variant_sql,
                   (generate_id('pvma_'), cad_id, variant_id))
    cursor.execute(insert_variant_sql,
                   (generate_id('pvma_'), aud_id, variant_id))

# Commit changes
conn.commit()

# Close connection
cursor.close()
conn.close()

time_end = time.time()

print(
    f"Time taken: {(time_end - time_start)/60} minutes and {(time_end - time_start) % 60} seconds")
