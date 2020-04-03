# -*-coding:utf-8-*-
import pika

from message_broker.settings import Config

def connect():
    credentials = pika.PlainCredentials(username=Config.USERNAME, password=Config.PASSWORD, erase_on_connect=False)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.MQ_HOST,
                                                                   port=Config.MQ_PORT, credentials=credentials))


    channel = connection.channel()
    channel.queue_declare(queue='hello')
    return connection, channel


def sent(channel):
    channel.basic_publish(exchange='',
                          routing_key='hello', body='帅哥')
    print(f'消息发送成功')


def close(connection):
    connection.close()


def run():
    con, ch = connect()
    sent(ch)
    close(con)

if __name__ == '__main__':
    run()
