#!/usr/bin/python3

import boto3
import time
import os
from multiprocessing.pool import ThreadPool

__author__ = 'bennett.e.siegel@gmail.com'


class CloudWatchObject:
    """Parent CloudWatchLogs object, data common to both LogGroup and LogStream.
      """

    def __init__(self, cloud_watch_dict):
        # Creation time of the resource.
        self.creation_time = cloud_watch_dict['creationTime']
        # Amazon Resource Name (ARN). Uniquely identifies AWS resources.
        self.arn = cloud_watch_dict['arn']
        # Used to determine if the object has been added since the agent started. This way won't miss LogEvents
        # or update LogStreams for a new LogGroup initially.
        self.added = False

    # Compare ARN to determine equality.
    # This is a unique AWS signature.
    # Used to update LogGroups and LogStreams
    def __eq__(self, other):
        return self.arn == other.arn

    # Hash ARN.
    def __hash__(self):
        return hash(self.arn)

    # ARN as object string.
    def __repr__(self):
        return '%r' % self.arn

    @staticmethod
    def update_cloud_watch_obj_list(old_list, new_list):
        """Updates list of objects. Adds that exist in the new_list.
           Removes objects that only exist in the old_list.
        """

        # Add new.
        for new_item in new_list:
            if new_item not in old_list:
                new_item.added = True
                old_list.append(new_item)

        # Remove deleted.
        for old_item in old_list:
            if old_item not in new_list:
                old_list.remove(old_item)

        return old_list


class LogGroup(CloudWatchObject):
    """Log Groups: represents a type of log (ex. Apache Server Logs).
       Log Groups have zero or more Log Streams.
      """

    def __init__(self, log_group_dict):
        # Inherit instance variables from CloudWatchObject.
        super().__init__(log_group_dict)
        # The name of the Log Group.
        self.name = log_group_dict['logGroupName']
        # List of LogStream objects that belong to the LogGroup.
        self.log_streams = LogStream.get_log_streams(self)

    @staticmethod
    def get_log_groups():
        """Gets a list of all LogGroups objects on the AWS account.
        """
        client = boto3.client('logs')

        # Request LogGroups from AWS.
        log_groups_response = client.describe_log_groups()['logGroups']

        # Return list of LogGroup objects.
        return [LogGroup(log_group_dict) for log_group_dict in log_groups_response]

    @staticmethod
    def update_log_groups(log_groups):
        """Adds new LogGroups and removes deleted LogGroups to a list of LogGroups.
        """

        # Get new list of LogGroups.
        new_groups = LogGroup.get_log_groups()

        # Update LogGroup list.
        return CloudWatchObject.update_cloud_watch_obj_list(log_groups, new_groups)


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
        # Timestamp of the last event in the LogStream.
        self.last_event_timestamp = log_stream_dict['lastEventTimestamp']
        # Timestamp of the last log ingestion.
        self.last_ingestion_time = log_stream_dict['lastIngestionTime']
        # Set last checked timestamp to now so no historical events are requested.
        self.last_event_check_timestamp = self.get_timestamp()

    @staticmethod
    def get_log_streams(log_group):
        """Gets a list of LogStream objects that belong to the passed in LogGroup.
        """

        # Initialize new client to avoid SSL problems from multi-threading on shared sessions.
        client = boto3.client('logs')

        # Request LogStreams from AWS.
        log_streams_response = client.describe_log_streams(logGroupName=log_group.name)['logStreams']

        # Return list of LogStream objects.
        return [LogStream(log_stream_dict, log_group) for log_stream_dict in log_streams_response]

    @staticmethod
    def update_log_streams(log_streams, log_group):
        """Adds new LogStreams and removes deleted LogStreams to a list of LogStreams.
        """

        # Get new list of LogStreams.
        new_streams = LogStream.get_log_streams(log_group)

        # Update LogStream list.
        return CloudWatchObject.update_cloud_watch_obj_list(log_streams, new_streams)

    @staticmethod
    def get_timestamp():
        """Converts the current time into a millisecond timestamp.
        """

        # Convert timestamp to int after multiply by 1000 to get millisecond timestamp in int.
        return int(time.time() * 1000)

    def get_log_events(self):
        """Gets all events since the last time they were polled.
        """
        client = boto3.client('logs')

        # Set the timestamp we will start from next poll.
        # and limit current poll to.
        end_timestamp = self.get_timestamp()

        # Request LogEvents.

        # Check if LogStream was added while agent was running. If so, get LogEvents from LogStream creation time.
        # So we don't miss any.
        if self.added:
            self.last_event_check_timestamp = self.creation_time
            self.added = False

        log_events_response = client.get_log_events(
            startTime=self.last_event_check_timestamp,
            endTime=end_timestamp,
            logGroupName=self.log_group.name,
            logStreamName=self.name,
            limit=self.event_limit,
            startFromHead=True
        )

        # Create LogEvents list from response.
        events = [LogEvent(log_event_dict) for log_event_dict in log_events_response['events']]

        # Token used if another request is required to get all LogEvents.
        next_forward_token = log_events_response['nextForwardToken']

        event_count = len(events)

        # While we get LogEvents equal to event_limit, continue requesting.
        while event_count >= self.event_limit:
            log_events_response = client.get_log_events(
                startTime=self.last_event_check_timestamp,
                endTime=end_timestamp,
                logGroupName=self.log_group.name,
                logStreamName=self.name,
                limit=self.event_limit,
                nextToken=next_forward_token,
                startFromHead=True
            )

            # Set length and next forward token for while loop.
            event_count = len(log_events_response['events'])
            next_forward_token = log_events_response['nextForwardToken']

            # Add LogEvents to our event list.
            events += [LogEvent(log_event_dict) for log_event_dict in log_events_response['events']]

        # Set starting point for next poll
        self.last_event_check_timestamp = end_timestamp

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

        # Write log events to file.
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

    def update(self):
        """Updates the list of LogGroups and their LogStreams.
        """
        print('Updating LogGroups and LogStreams')

        # Add new LogGroups. Remove old LogGroups.
        log_groups = LogGroup.update_log_groups(self.log_groups)

        # Ideally, LogStreams should only be updated for maintained (not new) LogGroups.
        # This is because LogStreams are initialized at LogGroup object creation.
        for log_group in log_groups:
            if not log_group.added:
                # Add new LogStreams. Remove old LogStreams.
                log_group.log_streams = LogStream.update_log_streams(log_group.log_streams, log_group)
            else:
                # Set added to false to we update LogStreams next time.
                log_group.added = False

        self.log_groups = log_groups

    def run(self):
        """Starts CloudWatch Logs collection.
        """
        print('Starting CloudWatchLogsMonitor.')

        # Initialize pool for multithreading.
        # Use ThreadPool for shared memory (used for keeping track of last polled timestamp)
        pool = ThreadPool()

        while True:

            # Check for new LogGroups and LogStreams.
            self.update()

            for log_group in self.log_groups:
                # For every log group get and append log events to log file.
                # This is run in parallel and is non-blocking.
                pool.map_async(LogStream.get_and_append_log_events, log_group.log_streams)

                # These lines run the agent synchronously.
                # You need to comment out the pool.map_async line above if using synchronous loop.
                # for log_stream in log_group.log_streams:
                #     LogStream.get_and_append_log_events(log_stream)

            # Sleep for the polling interval.
            time.sleep(self.default_polling_interval)
