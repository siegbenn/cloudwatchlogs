-- How to Start --

This project uses Python 3.

You must have a valid AWS credentials file at ~/.aws/credentials

1) Install requirements.txt
2) $ python3 agent.py

-- Notes --

- Uses CloudWatch Logs API to discover all log streams in an AWS account.
- Continuously pulls events from each log stream, writing them to disk (one file per stream).
- Logs files are rotated base on file size. This can be configured in the cloud_watch_logs.py module along with polling frequency.

-- TODO --
- Configuration file.
