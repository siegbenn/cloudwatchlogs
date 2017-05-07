import boto3
import time
from multiprocessing import Pool


class CloudWatchObject:

  def __init__(self, cloud_watch_dict):
    self.creation_time = cloud_watch_dict['creationTime']
    self.arn = cloud_watch_dict['arn']
    self.stored_bytes = cloud_watch_dict['storedBytes']


class LogGroup(CloudWatchObject):

  def __init__(self, log_group_dict):
    super().__init__(log_group_dict)
    self.name = log_group_dict['logGroupName']
    self.metric_filter_count = log_group_dict['metricFilterCount']
    self.log_streams = self.get_log_streams()

  def get_log_streams(self):
    client = boto3.client('logs')
    log_streams_response = client.describe_log_streams(logGroupName=self.name)['logStreams']
    return [LogStream(log_stream_dict, self) for log_stream_dict in log_streams_response]


class LogStream(CloudWatchObject):

  event_limit = 1000

  def __init__(self, log_stream_dict, log_group):
    super().__init__(log_stream_dict)
    self.log_group = log_group
    self.name = log_stream_dict['logStreamName']
    self.first_event_timestamp = log_stream_dict['firstEventTimestamp']
    self.last_event_timestamp = log_stream_dict['lastEventTimestamp']
    self.last_ingestion_time = log_stream_dict['lastIngestionTime']
    self.last_event_check_timestamp = self.get_timestamp()

  def get_timestamp(self):
    return int(time.time()) * 1000

  def get_log_events(self):
    client = boto3.client('logs')
    check_timestamp = self.get_timestamp()

    log_events_response = client.get_log_events(
      logGroupName=self.log_group.name,
      logStreamName=self.name,
      limit=self.event_limit,
      startTime=self.last_event_check_timestamp,
    )

    events = [LogEvent(log_event_dict) for log_event_dict in log_events_response['events']]
    event_count = len(events)

    next_forward_token = log_events_response['nextForwardToken']

    while event_count == self.event_limit:
      log_events_response = client.get_log_events(
        nextToken=next_forward_token
      )
      event_count = len(log_events_response['events'])
      events.append([LogEvent(log_event_dict) for log_event_dict in log_events_response['events']])

      # print(log_events_response['nextForwardToken'])
    self.last_event_check_timestamp = check_timestamp

    print(self.log_group.name, self.name, self.last_event_check_timestamp, len(events))
    return events

  def write_log_events(self, log_events):
    with open(self.name + '-0.log', 'a') as log_file:
      for event in log_events:
        log_file.write(event.message + '\n')

  def get_and_append_log_events(self):
    self.write_log_events(self.get_log_events())

class LogEvent:

  def __init__(self, log_event_dict):
    self.timestamp = log_event_dict['timestamp']
    self.message = log_event_dict['message']
    self.ingestion_time = log_event_dict['ingestionTime']


client = boto3.client('logs')
log_groups_response = client.describe_log_groups()['logGroups']
log_groups = [LogGroup(log_group_dict) for log_group_dict in log_groups_response]

pool = Pool()

while True:

  for log_group in log_groups:
    pool.map_async(LogStream.get_and_append_log_events, log_group.log_streams)

  time.sleep(30)