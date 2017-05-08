#!/usr/bin/python3

import boto3
import time
import os
from multiprocessing import Pool

__author__ = 'bennett.e.siegel@gmail.com'

class CloudWatchObject:
  """Parent CloudWatchLogs object, data common to both LogGroup and LogStream.
    """
  def __init__(self, cloud_watch_dict):

    # Creation time of the resource.
    self.creation_time = cloud_watch_dict['creationTime']
    # Amazon Resource Name (ARN). Uniquely identifies AWS resources.
    self.arn = cloud_watch_dict['arn']
    # Running total of stored bytes in the resource.
    self.stored_bytes = cloud_watch_dict['storedBytes']


class LogGroup(CloudWatchObject):
  """Log Groups: represents a type of log (ex. Apache Server Logs).
     Log Groups have zero or more Log Streams.
    """
  def __init__(self, log_group_dict):
    # Inherit instance variables from CloudWatchObject.
    super().__init__(log_group_dict)
    # The name of the Log Group.
    self.name = log_group_dict['logGroupName']
    # Count of metric filters.
    self.metric_filter_count = log_group_dict['metricFilterCount']
    # List of LogStream objects that belong to the LogGroup.
    self.log_streams = self.get_log_streams()

  def get_log_streams(self):
    """Gets a list of LogStream objects that belong to the LogGroup.
    """

    # Initialize new client to avoid SSL problems from multithreading on shared sessions.
    client = boto3.client('logs')

    # Request LogStreams from AWS.
    log_streams_response = client.describe_log_streams(logGroupName=self.name)['logStreams']

    # Return list of LogStream objects.
    return [LogStream(log_stream_dict, self) for log_stream_dict in log_streams_response]

  @staticmethod
  def get_log_groups():
    """Gets a list of all LogGroups objects on the AWS account.
    """
    client = boto3.client('logs')

    # Request LogGroups from AWS.
    log_groups_response = client.describe_log_groups()['logGroups']

    # Return list of LogGroup objects.
    return [LogGroup(log_group_dict) for log_group_dict in log_groups_response]


class LogStream(CloudWatchObject):
  """Log Streams: represents a single source generating logs (ex. a single server).
     Log Streams have zero or more Log Events.
    """

  # Max amount of LogEvents to retrieve in one request.
  event_limit = 1000

  # Log file rotation size.
  log_file_limit = 1048576

  def __init__(self, log_stream_dict, log_group):
    # Inherit instance variables from CloudWatchObject.
    super().__init__(log_stream_dict)
    # LogGroup this LogStream belongs to.
    self.log_group = log_group
    # Name of the LogStream.
    self.name = log_stream_dict['logStreamName']
    # Timestamp of the first event in the LogStream.
    self.first_event_timestamp = log_stream_dict['firstEventTimestamp']
    # Timesamp of the last event in the LogStream.
    self.last_event_timestamp = log_stream_dict['lastEventTimestamp']
    # Timestamp of the last log ingestion.
    self.last_ingestion_time = log_stream_dict['lastIngestionTime']
    # Set last checked timestamp to now so no historical events are requested.
    self.last_event_check_timestamp = self.get_timestamp()

  def get_timestamp(self):
    """Converts the current time into a millisecond timestamp.
    """
    return int(time.time()) * 1000

  def get_log_events(self):
    """Gets all events since the last time they were polled.
    """
    client = boto3.client('logs')

    # Set the timestamp we will start from next poll.
    check_timestamp = self.get_timestamp()

    # Request LogEvents.
    log_events_response = client.get_log_events(
      logGroupName=self.log_group.name,
      logStreamName=self.name,
      limit=self.event_limit,
      startTime=self.last_event_check_timestamp,
    )

    # Create LogEvents list from response.
    events = [LogEvent(log_event_dict) for log_event_dict in log_events_response['events']]

    # Token used if another request is required to get all LogEvents.
    next_forward_token = log_events_response['nextForwardToken']

    # While we get LogEvents equal to event_limit, continue requesting.
    event_count = len(events)
    while event_count == self.event_limit:
      log_events_response = client.get_log_events(
        nextToken=next_forward_token
      )
      event_count = len(log_events_response['events'])

      # Add LogEvents to our event list.
      events.append([LogEvent(log_event_dict) for log_event_dict in log_events_response['events']])

    # Update the polling timestamp.
    self.last_event_check_timestamp = check_timestamp

    print('Found ' + str(len(events)) + ' LogEvents for LogStream ' + self.log_group.name + ' ' + self.name)
    return events

  def write_log_events(self, log_events):
    """Writes LogEvents to a log file. Rotates log files if they larger than log_file_limit.
    """
    # Create log file name.
    # Replace / with - so LogGroup names can be written to current directory.
    file_name = self.log_group.name.replace('/', '-') + "-" + self.name + '-0.log'

    # Append LogEvents to log file.
    with open(file_name, 'a') as log_file:
      for event in log_events:
        log_file.write(event.message + '\n')
    print('Wrote ' + str(len(log_events)) + ' LogEvents to ' + file_name)

    # Rotate log file if it's bigger than limit
    log_file_size = os.path.getsize(file_name)

    if log_file_size > self.log_file_limit:
      rotated_file_name = file_name.split('.')[0] + '-' + str(int(time.time())) + ".log"
      print('Rotating ' + file_name + ' to ' + rotated_file_name)
      os.rename(file_name, rotated_file_name)

  def get_and_append_log_events(self):
    """Gets all events since the last time they were polled.
    """
    log_events = self.get_log_events()

    if len(log_events) > 0:
      self.write_log_events(log_events)

class LogEvent:
  """Log Events: represents one item in a Log Stream (ex. single log line).
    """
  def __init__(self, log_event_dict):
    # Timestamp of the LogEvent
    self.timestamp = log_event_dict['timestamp']
    # LogEvent message contents.
    self.message = log_event_dict['message']
    # When the LogEvent was ingested
    self.ingestion_time = log_event_dict['ingestionTime']

class CloudWatchLogsMonitor:

  """The default number of seconds between polling for new CloudWatch Log events.
     TODO: Add config file.
  """

  # Default interval to poll CloudWatch Logs (seconds).
  default_polling_interval = 30

  def __init__(self):
    # All LogGroups for the AWS account.
    self.log_groups = LogGroup.get_log_groups()

  def run(self):
    print('Starting CloudWatchLogsMonitor.')

    # Initialize pool for multithreading.
    pool = Pool()

    while True:
      for log_group in self.log_groups:
        # For every log group get and append log events to log file.
        # This is run in parallel and is non-blocking.
        # for log_stream in log_group.log_streams:
        #   print(log_stream.get_and_append_log_events())
        pool.map_async(LogStream.get_and_append_log_events, log_group.log_streams)

      # Sleep for the polling interval.
      time.sleep(self.default_polling_interval)
