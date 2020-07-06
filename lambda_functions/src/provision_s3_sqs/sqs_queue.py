import boto3
import json

MAIN_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/783904232032/Main-Queue'
QUEUE_FOR_DEAD_LETTER = 'Dead-Letter-Queue-For-Main'
DEAD_LETTER_MAIN_QUEUE = 'Main-Queue'
DEAD_LETTER_QUEUE_ARN = 'arn:aws:sqs:us-east-2:783904232032:Dead-Letter-Queue-For-Main'


def sqs_client():
    sqs = boto3.client('sqs', region_name='us-east-2')
    """ :type : pyboto3.sqs """
    return sqs


def create_queue_for_dead_letter():
    return sqs_client().create_queue(
        QueueName=QUEUE_FOR_DEAD_LETTER
    )


def create_main_queue():
    redrive_policy = {
        "deadLetterTargetArn": "arn:aws:sqs:us-east-2:783904232032:Dead-Letter-Queue-For-Main",
        "maxReceiveCount": 3
    }
    return sqs_client().create_queue(
        QueueName=DEAD_LETTER_MAIN_QUEUE,
        Attributes={
            "DelaySeconds": "0",
            "MaximumMessageSize": "262144",
            "VisibilityTimeout": "900",
            "MessageRetentionPeriod": "345600",
            "ReceiveMessageWaitTimeSeconds": "20",
            "RedrivePolicy": json.dumps(redrive_policy)
        }
    )


def find_queue():
    return sqs_client().list_queues(
        QueueNamePrefix='Queue'
    )


def list_queues():
    return sqs_client().list_queues()


def queue_attributes():
    return sqs_client().get_queue_attributes(
        QueueUrl=MAIN_QUEUE_URL,
        AttributeNames=['All']
    )


if __name__ == '__main__':
    print(create_queue_for_dead_letter())
    print(create_main_queue())
    print(find_queue())
    print(list_queues())
    print(queue_attributes())
