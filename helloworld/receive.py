# -*-coding:utf-8-*-
import pika

from message_broker.settings import Config


def connect():
    credentials = pika.PlainCredentials(username=Config.USERNAME, password=Config.PASSWORD, erase_on_connect=False)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.MQ_HOST,
                                                                   port=Config.MQ_PORT, credentials=credentials))

    channel = connection.channel()
    channel.queue_declare(queue='hello')

    channel.basic_consume(
        queue='hello',
        auto_ack=True,
        on_message_callback=call_back
    )

    return connection, channel


def call_back(ch, method, properties, body):
    print(f'channel: {ch} \n method: {method} \n properties: {properties}')
    print(f'{body.decode()}')



def close(connection):
    connection.close()


def run():
    con, ch = connect()
    print('开始等待消息')
    ch.start_consuming()
    # close(con)


if __name__ == '__main__':
    run()
