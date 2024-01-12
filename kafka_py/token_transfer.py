import psycopg2
from kafka import KafkaProducer
import json
import time

while True:
    if int(time.time()) % 90 == 0:
        print('Producer Start')
        producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'), compression_type='gzip')

        connect = psycopg2.connect(
        database="postgres", user='student_token_transfer', password='svbk_2023', host='34.126.75.56', port= '5432'
        )
        connect.autocommit = True
        cursor = connect.cursor()

        with open('from_block.txt', 'r') as f:
            from_block = f.read()
        print('Postgresql Querying...')
        cursor.execute('select * from chain_0x1.token_transfer where block_number > ' + from_block + ' order by block_number limit 50000')
        records = cursor.fetchall()
        print('Producing ' + str(len(records)) + ' records...')
        data_feature = [desc[0] for desc in cursor.description]
        for i in range(len(records)):
            record = records[i]
            message = {data_feature[i] : record[i] for i in [0, 3, 4, 5]}
            producer.send('token_tranfer', value=message)
            producer.flush()
            if i % 10000 == 0:
                print('Produced ' + str(i) + ' records')
        
        last_block = str(message['block_number'])
        print('Produced Successfully to Block: ' + last_block)
        connect.close()
        producer.close()
        with open('from_block.txt', 'w') as f:
            f.write(last_block)
    else:
        print('Waiting...')
        time.sleep(90 - int(time.time()) % 90)
