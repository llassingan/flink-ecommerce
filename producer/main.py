import random
import json
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime

fake = Faker()

# buat generate fake transaction 
def generate_sales_transaction():
    # init fake user 
    user = fake.simple_profile() 
    # return json transaction 
    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
    }

# delivery_report for producer 
def delivery_report(err,msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to: {msg.topic} [{msg.partition()}]")

def main():
    topic = 'financial_trx'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()
    # will run for  2 min 
    while ((datetime.now()-curr_time).seconds <120):
        try:
            transaction = generate_sales_transaction()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productPrice']

            print(transaction)

            producer.produce(topic,
                             key = transaction['transactionId'],
                             value = json.dumps(transaction),
                             on_delivery = delivery_report)
            producer.poll(0)

            time.sleep(5)

        except BufferError:
            print("Buffer error.. waiting..")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ =="__main__":
    main()