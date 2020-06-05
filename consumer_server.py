import time, sys
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

consumer.subscribe(['com.crime.police.calls'])

for message in consumer:
    try:
        print(message.value)
        time.sleep(1)
    except KeyboardInterrupt as e:
        print("shutting down")
        sys.exit()

