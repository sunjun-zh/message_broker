# -*-coding:utf-8-*-
import uuid
import pika
from message_broker.settings import Config

class RpcClient():
    def __init__(self):

        credentials = pika.PlainCredentials(username=Config.USERNAME, password=Config.PASSWORD, erase_on_connect=False)
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=Config.MQ_HOST,
                                                                       port=Config.MQ_PORT, credentials=credentials))


        self.ch = self.conn.channel()

        result = self.ch.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.ch.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, m, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        self.ch.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                delivery_mode=2,
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            ),
            body=str(m) + ',' + str(n)
        )

        while self.response is None:
            self.conn.process_data_events()
        return int(self.response)


if __name__ == '__main__':
    client_rpc = RpcClient()
    print(f' requesting add(30, 20)')
    response = client_rpc.call(30, 20)
    print(f'结果: {response}')
