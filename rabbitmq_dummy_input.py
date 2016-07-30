"""
A very simple script to place dummy data into rabbitmq. This is useful
for testing the firehose.
"""
import argparse
import getpass
import pika


class PasswordPromptAction(argparse.Action):
    def __call__(self, parse, args, values, option_string=None):
        password = getpass.getpass()
        setattr(args, self.dest, password)


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


parser = argparse.ArgumentParser(description="Connect to RabbitMQ and place some dummy events into the queue.")
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
parser.add_argument("-q", "--queue",
                    dest="queue_name",
                    action="store",
                    type=str,
                    default="")
args = parser.parse_args()


if __name__ == "__main__":
    rabbitmq_user = args.rabbitmq_user
    rabbitmq_pass = args.rabbitmq_pass
    rabbitmq_queue = args.queue_name

    rabbitmq_connection = connect_to_rabbitmq(rabbitmq_user, rabbitmq_pass)
    channel = rabbitmq_connection.channel()
    channel.queue_declare(queue=rabbitmq_queue)

    channel.basic_publish(exchange='', routing_key='hello', body="Hello, world!")
    rabbitmq_connection.close()
