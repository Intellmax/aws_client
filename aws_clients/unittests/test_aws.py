import os
import unittest
import logging
import sys
import validators
from aws_clients import config
from aws_clients.aws_clients import StorageClient, SNSClient, SQSClient


class TestS3(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.INFO)
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter('ignore')
        self.test_bucket_name = 'test-bucket-745-123-935622'
        self.test_file = 'test_file.jpeg'
        self.s3_client = StorageClient(aws_key=config.aws_access_key_id,
                                       aws_secret=config.aws_secret_key,
                                       aws_region=config.region_name)

    def test_s3_bucket_create(self):
        self.s3_client.create_bucket(self.test_bucket_name)
        self.assertIn(self.test_bucket_name, self.s3_client.buckets())

    def test_s3_bucket_list(self):
        buckets = self.s3_client.buckets()
        self.assertIsInstance(buckets, list)
        self.assertIsNotNone(buckets)

    def test_s3_file_load(self):
        self.s3_client.upload_file(self.test_file, self.test_bucket_name)
        list_files = self.s3_client.ls_bucket(self.test_bucket_name)
        self.assertIn(self.test_file, list_files)

    def test_s3_file_remove(self):
        self.s3_client.delete_file(self.test_bucket_name, self.test_file)
        list_files = self.s3_client.ls_bucket(self.test_bucket_name)
        self.assertNotIn(self.test_file, list_files)

    def test_s3_remove_bucket(self):
        self.s3_client.delete_bucket(self.test_bucket_name)
        buckets = self.s3_client.buckets()
        self.assertNotIn(self.test_bucket_name, buckets)


class TestSnsSqs(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.INFO)
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter('ignore')
        self.topic_name = 'TestTopicTesting'
        self.q_name = 'TestQTesting'
        self.q_message = 'Hi, this is test message'
        self.sns_client = SNSClient(aws_key=config.aws_access_key_id,
                                    aws_secret=config.aws_secret_key,
                                    aws_region=config.region_name)
        self.sqs_client = SQSClient(aws_key=config.aws_access_key_id,
                                    aws_secret=config.aws_secret_key,
                                    aws_region=config.region_name)

    def test_create_sns_topic(self):
        self.sns_client.create_topic(self.topic_name)
        topic_list = self.sns_client.topics()
        self.assertIsNotNone(topic_list)
        topic_list_names = [i.get('TopicArn').split(':')[-1] for i in topic_list if i]
        self.assertIn(self.topic_name, topic_list_names)

    def test_create_sqs_q(self):
        self.sqs_client.create_q(self.q_name)
        q_list_names = [os.path.basename(i) for i in self.sqs_client.queues()]
        self.assertIn(self.q_name, q_list_names)

    def test_message_to_q(self):
        queue_url = self.sqs_client.q_url(self.q_name)
        self.assertTrue(validators.url(queue_url))
        self.sqs_client.send_message(queue_url, self.q_message)
        messages = self.sqs_client.message_list(queue_url)
        messages_content = [i.get('Body') for i in messages.get('Messages')]
        self.assertIn(self.q_message, messages_content)

    def test_q_subscriptions_to_topic(self):
        topic_arn = self.sns_client.topics()[0].get('TopicArn')
        queue_url = self.sqs_client.q_url(self.q_name)
        q_arn = self.sqs_client.q_attributes(queue_url).get('Attributes').get('QueueArn')
        self.sns_client.subscribe(topic_arn, q_arn)
        subs = self.sns_client.subscriptions(topic_arn)
        subs_topic_names = [i.get('TopicArn') for i in subs]
        self.assertIn(topic_arn, subs_topic_names)
        sub_arn = subs[0].get('SubscriptionArn')
        self.sns_client.unsubscribe(sub_arn)
        subs = self.sns_client.subscriptions(topic_arn)
        subs_topic_names = [i.get('TopicArn') for i in subs]
        self.assertNotIn(topic_arn, subs_topic_names)

    def test_remove_all(self):
        queue_url = self.sqs_client.q_url(self.q_name)
        self.sqs_client.delete_q(queue_url)
        topic_arn = self.sns_client.topics()[0].get('TopicArn')
        self.sns_client.delete_topic(topic_arn)
        topic_list = self.sns_client.topics()
        self.assertNotIn(topic_arn, topic_list)
