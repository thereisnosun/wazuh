
import json
import sys

import aws_bucket
from aws_tools import debug

class AWSCloudConnexaBucket(aws_bucket.AWSCustomBucket):

    def __init__(self, **kwargs):
        db_table_name = 'cloud_connexa'
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

    def _prepare_log_key(self, key):
        parts_n = key.find("-")
        date_part = key[parts_n:]
        date_components = date_part.split("-")
        return f'CloudConnexa/{date_components[1]}-{date_components[2]}-{date_components[3]}'

    def mark_complete(self, aws_account_id, aws_region, log_file, **kwargs):
        if not self.reparse:
            try:
                self.db_cursor.execute(self.sql_mark_complete.format(table_name=self.db_table_name), {
                    'bucket_path': self.bucket_path,
                    'aws_account_id': aws_account_id,
                    'aws_region': aws_region,
                    'log_key': self._prepare_log_key(log_file['Key']),
                    'created_date': self.get_creation_date(log_file)})
            except Exception as e:
                debug("+++ Error marking log {} as completed: {}".format(log_file['Key'], e), 2)

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
        
