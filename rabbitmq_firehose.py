"""
A tool to connect to RabbitMQ's firehose feature and print what
is going on.

Note: You need to enable RabbitMQ's firehose functionality by running
`rabbitmqctl trace_on`
"""
import argparse
import getpass
import pika
import sys


class PasswordPromptAction(argparse.Action):
    def __call__(self, parse, args, values, option_string=None):
        password = getpass.getpass()
        setattr(args, self.dest, password)


parser = argparse.ArgumentParser(description="Connect to RabbitMQ and dump the output of the firehose.")
parser.add_argument("-u", "--user",
                    dest="rabbitmq_user",
                    action="store",
                    type=str,
                    default=None,
                    help="RabbitMQ User")
parser.add_argument("-p", "--password",
                    dest="rabbitmq_pass",
                    action=PasswordPromptAction,
                    type=str,
                    default=None,
                    nargs=0)
args = parser.parse_args()


def connect_to_rabbitmq(username, password, host=None, port=None):
    if host is None:
        host = "localhost"
    if port is None:
        port = 5672

    credentials = pika.PlainCredentials(username=username, password=password)
    connection_parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials
        )
    connection = pika.BlockingConnection(connection_parameters)
    return connection



def connect_and_print_firehose(connection):

    channel = connection.channel()

    queue_name = "firehose"
    result = channel.queue_declare(queue=queue_name, exclusive=False)

    channel.queue_bind(exchange='amq.rabbitmq.trace',
                       queue=queue_name,
                       routing_key="#")

    print ' [*] Waiting for logs. To exit press CTRL+C'

    def callback(ch, method, properties, body):
        print " [x] %r:%r:%r:%r" % (
            method.routing_key,
            properties.headers["node"],
            properties.headers["routing_keys"],
            body,
            )

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

if __name__ == "__main__":
    rabbitmq_user = args.rabbitmq_user
    rabbitmq_pass = args.rabbitmq_pass

    rabbitmq_connection = connect_to_rabbitmq(rabbitmq_user, rabbitmq_pass)
    connect_and_print_firehose(rabbitmq_connection)
