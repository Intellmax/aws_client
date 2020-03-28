"""
Run via console:
1) aws_project $ python3 -m pytest aws_clients/pytests/pytests.py -p no:cacheprovider
OR
2) pytests $ python3 -m pytest pytests.py

"""

import mock
from aws_clients.aws_clients import StorageClient, SNSClient, SQSClient
from aws_clients import config

s3_client = StorageClient(aws_key=config.aws_access_key_id,
                          aws_secret=config.aws_secret_key,
                          aws_region=config.region_name)
sns_client = SNSClient(aws_key=config.aws_access_key_id,
                       aws_secret=config.aws_secret_key,
                       aws_region=config.region_name)
sqs_client = SQSClient(aws_key=config.aws_access_key_id,
                       aws_secret=config.aws_secret_key,
                       aws_region=config.region_name)


@mock.patch('aws_clients.aws_clients.StorageClient.buckets')
def test_s3_bucket_list(mock_buckets):
    buckets = s3_client.buckets()
    assert len(buckets) == 0
    mock_buckets.assert_called_once()


@mock.patch('aws_clients.aws_clients.StorageClient.create_bucket')
@mock.patch('aws_clients.aws_clients.StorageClient.buckets')
def test_s3_create_bucket(mock_buckets, mock_create_bucket):
    s3_client.create_bucket('TestBucket')
    s3_client.buckets()
    mock_buckets.assert_called_once()
    mock_create_bucket.assert_called_with('TestBucket')


@mock.patch('aws_clients.aws_clients.StorageClient.upload_file')
@mock.patch('aws_clients.aws_clients.StorageClient.ls_bucket')
def test_s3_file_load(mock_buckets, mock_upload):
    s3_client.upload_file('testFile', 'TestBucket')
    s3_client.ls_bucket('TestBucket')
    mock_buckets.assert_called_once_with('TestBucket')
    mock_upload.assert_called_once()


@mock.patch('aws_clients.aws_clients.StorageClient.delete_file')
def test_s3_delete_file(mock_delete):
    s3_client.delete_file('TestBucket', 'testFile')
    mock_delete.assert_called_once_with('TestBucket', 'testFile')


@mock.patch('aws_clients.aws_clients.SNSClient.topics')
def test_sns_topics(mock_topics):
    topic_list = sns_client.topics()
    assert len(topic_list) == 0
    mock_topics.assert_called_once()


@mock.patch('aws_clients.aws_clients.SNSClient.create_topic')
@mock.patch('aws_clients.aws_clients.SNSClient.topics')
def test_sns_create_topic(mock_topics, mock_create_topic):
    sns_client.create_topic('TestTopic')
    mock_create_topic.assert_called_once_with('TestTopic')
    sns_client.topics()
    mock_topics.assert_called_once()


@mock.patch('aws_clients.aws_clients.SQSClient.queues')
def test_sqs_qs(mock_qs):
    qs = sqs_client.queues()
    assert len(qs) == 0
    mock_qs.assert_called_once()
    assert isinstance(mock_qs, mock.mock.MagicMock)


@mock.patch('aws_clients.aws_clients.SQSClient.create_q')
def test_sqs_create_q(mock_create_q):
    sqs_client.create_q('TestQ')
    mock_create_q.assert_called_once_with('TestQ')
    assert isinstance(mock_create_q, mock.mock.MagicMock)
