# -*-coding:utf-8-*-
import pika

from message_broker.settings import Config
def add(m, n):
    return m+n


def connect():
    credentials = pika.PlainCredentials(username=Config.USERNAME, password=Config.PASSWORD, erase_on_connect=False)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=Config.MQ_HOST,
                                                                   port=Config.MQ_PORT, credentials=credentials))


    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue', durable=True)

    channel.basic_consume(
        queue='rpc_queue',
        on_message_callback=on_request
    )

    return connection, channel


def on_request(ch, method, props, body):
    m, n = body.decode().split(',')
    response = add(int(m), int(n))

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id,),
        body=str(response)
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)


def close(connection):
    connection.close()


def run():
    con, ch = connect()
    print('开始等待消息')
    ch.start_consuming()
    # close(con)


if __name__ == '__main__':
    run()
