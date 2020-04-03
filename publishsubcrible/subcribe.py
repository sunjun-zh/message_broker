# -*-coding:utf-8-*-
import time
import random
import pika
from message_broker.settings import Config

def connect():
    credentials = pika.PlainCredentials(username=Config.USERNAME, password=Config.PASSWORD, erase_on_connect=False)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.MQ_HOST,
                                                                   port=Config.MQ_PORT, credentials=credentials))

    channel = connection.channel()
    channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)

    result = channel.queue_declare(queue='', exclusive=True, durable=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs', queue=queue_name)
    channel.basic_consume(

        queue=queue_name,
        on_message_callback=call_back
    )

    return connection, channel


def call_back(ch, method, properties, body):
    print(f'receive >> {body}')
    time.sleep(random.random())
    print(f'done--{body.decode()}')

    ch.basic_ack(delivery_tag=method.delivery_tag)


def close(connection):
    connection.close()


def run():
    con, ch = connect()
    print('开始等待消息')
    ch.start_consuming()
    # close(con)


if __name__ == '__main__':
    run()
