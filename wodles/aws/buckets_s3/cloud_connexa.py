
import json
import sys
import re

import aws_bucket
from aws_tools import debug

class AWSCloudConnexaBucket(aws_bucket.AWSCustomBucket):

    def __init__(self, **kwargs):
        db_table_name = 'openvpncloudconnexa'
        aws_bucket.AWSCustomBucket.__init__(self, db_table_name, **kwargs)
        self.date_format = '%Y-%m-%d'
        self.check_prefix = False
        debug(f"+++ AWSCloudConnexaBucket initialized", 3)

    def get_base_prefix(self):
        base_path = '{}AWSLogs/{}'.format(self.prefix, self.suffix)
        if self.aws_organization_id:
            base_path = '{base_prefix}{aws_organization_id}/'.format(
                base_prefix=base_path,
                aws_organization_id=self.aws_organization_id)

        return base_path

    def _parse_log_marker(self, key):
        # Define the regex pattern to match the date part
        pattern = r'\d{4}-\d{2}-\d{2}'

        # Search for the date pattern in the filename
        match = re.search(pattern, key)

        if match:
            date_string = match.group(0)
            return f'{self.prefix}{date_string}'
        else:
            print("rock111: Date not found in the filename.")

    def build_s3_filter_args(self, aws_account_id, aws_region, iterating=False, custom_delimiter='', **kwargs):
        filter_marker = ''
        last_key = None
        if self.reparse:
            if self.only_logs_after:
                filter_marker = self.marker_only_logs_after(aws_region, aws_account_id)
            else:
                filter_marker = self.marker_custom_date(aws_region, aws_account_id, self.default_date)
        else:
            query_last_key = self.db_cursor.execute(
                self.sql_find_last_key_processed.format(table_name=self.db_table_name), {
                    'bucket_path': self.bucket_path,
                    'aws_region': aws_region,
                    'prefix': f'{self.prefix}%',
                    'aws_account_id': aws_account_id,
                    **kwargs
                })
            try:
                filter_marker = query_last_key.fetchone()[0]
            except (TypeError, IndexError):
                # if DB is empty for a region
                filter_marker = self.marker_only_logs_after(aws_region, aws_account_id) if self.only_logs_after \
                    else self.marker_custom_date(aws_region, aws_account_id, self.default_date)

        filter_args = {
            'Bucket': self.bucket,
            'MaxKeys': 1000,
            'Prefix': self.get_full_prefix(aws_account_id, aws_region)
        }

        # if nextContinuationToken is not used for processing logs in a bucket
        if not iterating:
            filter_args['StartAfter'] = filter_marker
            if self.only_logs_after:
                only_logs_marker = self.marker_only_logs_after(aws_region, aws_account_id)
                log_marker_from_db = self._parse_log_marker(filter_marker)
                debug(f"+++ log_marker_from_db: {log_marker_from_db}", 2)
                if log_marker_from_db:
                    filter_args['StartAfter'] = only_logs_marker if only_logs_marker > log_marker_from_db else log_marker_from_db
                else:
                    filter_args['StartAfter'] = only_logs_marker

            if custom_delimiter:
                prefix_len = len(filter_args['Prefix'])
                filter_args['StartAfter'] = filter_args['StartAfter'][:prefix_len] + \
                                            filter_args['StartAfter'][prefix_len:].replace('/', custom_delimiter)
            debug(f"+++ Marker: {filter_args['StartAfter']}", 2)

        return filter_args

    def load_information_from_file(self, log_key):
        """Load data from a OpenVPN log files."""
        debug(f"DEBUG: +++ AWSOpenVPNCloudConnexaBucket:load_information_from_file {log_key}", 3)

        def json_event_generator(data):
            while data:
                json_data, json_index = decoder.raw_decode(data)
                data = data[json_index:]
                yield json_data

        content = []
        decoder = json.JSONDecoder()
        with self.decompress_file(self.bucket, log_key=log_key) as f:
            for line in f.readlines():
                try:
                    for event in json_event_generator(line.rstrip()):
                        event['source'] = 'CloudConnexa'
                        content.append(event)

                except json.JSONDecodeError as Einst:
                    print("ERROR: Events from {} file could not be loaded.".format(log_key.split('/')[-1]))
                    print("ERROR: {}".format(Einst))
                    if not self.skip_on_error:
                        sys.exit(9)

        return json.loads(json.dumps(content))

    def marker_only_logs_after(self, aws_region, aws_account_id):
        debug(f"+++ AWSOpenVPNCloudConnexaBucket:load_information_from_file {aws_region}/{aws_account_id}", 3)
        debug(f"+++ AWSOpenVPNCloudConnexaBucket:load_information_from_file get_full_prefix={self.get_full_prefix(aws_account_id, aws_region)}", 3)
        return '{init}{only_logs_after}'.format(
            init=self.get_full_prefix(aws_account_id, aws_region),
            only_logs_after=self.only_logs_after.strftime(self.date_format)
        )

    def get_alert_msg(self, aws_account_id, log_key, event, error_msg=""):
        """ Override to send the json read from the bucklet for OpenVPN entries. """
        debug(f"+++ AWSOpenVPNCloudConnexaBucket:get_alert_msg {aws_account_id}, {log_key}, {event}, {error_msg};", 3)
        msg = event.copy()
        msg.update(
            {
                'aws': {
                    'log_info': {
                        'aws_account_alias': self.account_alias,
                        'log_file': log_key,
                        's3bucket': self.bucket
                    }
                }
            }
        )
        debug(f"+++ AWSOpenVPNCloudConnexaBucketget_alert_msg 01 {msg}", 3)
        msg['aws'].update({
                    'source': event['source']
                }
            )
        debug(f"+++ AWSOpenVPNCloudConnexaBucketget_alert_msg return {msg}", 3)
        return msg
        
