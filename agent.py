#!/usr/bin/python3

from cloud_watch_logs import CloudWatchLogsMonitor

__author__ = 'bennett.e.siegel@gmail.com'

if __name__ == '__main__':

  print("Starting Agent.")
  # Create an instance of CloudWatchLogsMonitor.
  cloud_watch_logs_monitor = CloudWatchLogsMonitor()
  # Run CloudWatchLogsMonitor.
  cloud_watch_logs_monitor.run()