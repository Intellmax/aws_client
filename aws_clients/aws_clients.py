import boto3
import logging
import os
from aws_clients import config
from typing import NoReturn, Optional, Dict, List


logging.basicConfig(level=logging.INFO)


class StorageClient:
    """
    Class for operating with s3 AWS storage
    """
    def __init__(self, aws_key: str, aws_secret: str, aws_region: str):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region
        )

    def buckets(self) -> list:
        """Show list of buckets"""
        bucket_list = [buc.get("Name") for buc in self.s3_client.list_buckets()["Buckets"]]
        logging.info(f"List of all buckets {bucket_list}")
        return bucket_list

    def create_bucket(self, name: str) -> None:
        """Create Bucket"""
        self.s3_client.create_bucket(Bucket=name)

    def delete_bucket(self, name: str) -> None:
        """Delete Bucket"""
        self.s3_client.delete_bucket(Bucket=name)

    def ls_bucket(self, name: str) -> list:
        """Check Bucket's content"""
        list_objects = self.s3_client.list_objects(Bucket=name)
        content = [file.get("Key") for file in list_objects.get('Contents')] if 'Contents' in list_objects else []
        return content

    def upload_file(self, filepath: str, bucket: str, folder: str = '') -> NoReturn:
        """Upload file to bucket"""
        folder = folder + '/' if folder else ''
        self.s3_client.upload_file(Filename=filepath,
                                   Bucket=bucket,
                                   Key=folder + os.path.basename(filepath))

    def delete_file(self, bucket: str, filename: str) -> NoReturn:
        """Delete file from bucket"""
        self.s3_client.delete_object(Bucket=bucket,
                                     Key=filename)


class SNSClient:
    """
    Class for operating with AWS SNS
    """
    def __init__(self, aws_key: str, aws_secret: str, aws_region: str):
        self.sns_client = boto3.client(
            "sns",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region)

    def topics(self) -> List[Optional[Dict]]:
        """Get topic list"""
        topics = self.sns_client.list_topics().get('Topics')
        logging.info(f"All topics {topics}")
        return topics

    def create_topic(self, name: str) -> None:
        """Create topic"""
        self.sns_client.create_topic(Name=name)

    def delete_topic(self, topic_arn: str) -> None:
        """Delete topic"""
        self.sns_client.delete_topic(TopicArn=topic_arn)

    def send_message(self, message: str, topic_arn: str, subject: str = "Default") -> None:
        """Send message to topic"""
        self.sns_client.publish(TopicArn=topic_arn,
                                Message=message,
                                Subject=subject)

    def subscriptions(self, topic_arn: str) -> list:
        """See list of subscribed queues of topic"""
        subscriptions = self.sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        logging.info(f"List of subscriptions of topic {subscriptions}")
        return subscriptions.get('Subscriptions')

    def topic_info(self, topic_arn: str) -> dict:
        """Get topic attributes"""
        topic_info = self.sns_client.get_topic_attributes(TopicArn=topic_arn)
        logging.info(f"Topic INFO {topic_info}")
        return topic_info

    def subscribe(self, topic_arn: str, q_arn: str, protocol: str = 'sqs', ) -> None:
        """Subscribe Q to topic"""
        self.sns_client.subscribe(TopicArn=topic_arn,
                                  Protocol=protocol,
                                  Endpoint=q_arn,
                                  ReturnSubscriptionArn=True)

    def unsubscribe(self, sub_arn: str) -> None:
        """Unsubscribe Q from topic. SubArn can be found via subscriptions endpoint"""
        self.sns_client.unsubscribe(SubscriptionArn=sub_arn)


class SQSClient:
    """
    Class for operating with AWS SQS
    """
    def __init__(self, aws_key: str, aws_secret: str, aws_region: str):
        self.sqs_client = boto3.client(
            "sqs",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            region_name=aws_region)

    def queues(self) -> Optional[Dict]:
        """Get list of all queues"""
        qs = self.sqs_client.list_queues().get('QueueUrls')
        logging.info(f"All queues {qs}")
        return qs

    def create_q(self, name: str) -> None:
        """Create Q"""
        self.sqs_client.create_queue(QueueName=name)

    def delete_q(self, q_url: str) -> None:
        """Delete Q"""
        self.sqs_client.delete_queue(QueueUrl=q_url)

    def q_url(self, q_name: str) -> str:
        """Get Q url"""
        url = self.sqs_client.get_queue_url(QueueName=q_name)
        logging.info(f"Q URL {url}")
        return url.get('QueueUrl')

    def q_attributes(self, q_url: str) -> dict:
        """Get Q attributes"""
        q_attrs = self.sqs_client.get_queue_attributes(QueueUrl=q_url, AttributeNames=['All'])
        return q_attrs

    def send_message(self, q_url: str, message: str) -> None:
        """Send message to Q"""
        self.sqs_client.send_message(QueueUrl=q_url,
                                     MessageBody=message)

    def message_list(self, q_url: str, max_count: int = 1) -> dict:
        """Get messages for Q"""
        assert 0 < max_count <= 10, "Should be in range 0-10"
        response = self.sqs_client.receive_message(QueueUrl=q_url,
                                                   MaxNumberOfMessages=max_count)
        logging.info(f"Messages for q {response}")
        return response


if __name__ == "__main__":
    credentials = dict(aws_key=config.aws_access_key_id,
                       aws_secret=config.aws_secret_key,
                       aws_region=config.region_name)

    # {'storageclient': <__main__.StorageClient object at 0x10ab3c7d0>, 'snsclient' .... }
    clients = {client.__name__.lower(): client(**credentials) for client in [StorageClient, SNSClient, SQSClient]}
    clients.get('storageclient').buckets()
    clients.get('storageclient').create_bucket('testbuc123900')
    topics = clients.get('snsclient').topics()
    print(topics)
    clients.get('sqsclient').queues()
    clients.get('storageclient').upload_file('test_file.jpeg', 'testbuck765')
