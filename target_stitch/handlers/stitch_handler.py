import singer
import backoff
import requests
import boto
from boto.s3.key import Key
import time
import json
import io
import os
import sys
import pytz

import uuid
import hashlib

from transit.writer import Writer
from decimal import Decimal
from requests.exceptions import RequestException, HTTPError
from target_stitch.timings import TIMINGS
from target_stitch.exceptions import TargetStitchException
from jsonschema import SchemaError, ValidationError, Draft4Validator, FormatChecker
from target_stitch.handlers.common import ensure_multipleof_is_decimal, marshall_decimals, MAX_NUM_GATE_RECORDS, serialize_gate_messages, determine_table_version, generate_sequence
from jsonschema.exceptions import UnknownType

LOGGER = singer.get_logger().getChild('target_stitch')
MESSAGE_VERSION=2
PIPELINE_VERSION='2'
STITCH_SPOOL_URL = "{}/spool/private/v1/clients/{}/batches"

#experiments have shown that payloads over 1MB are more efficiently transfered via S3
S3_THRESHOLD_BYTES=(1 * 1024 * 1024)
SYNTHETIC_PK='__sdc_primary_key'
TIME_EXTRACTED='_sdc_extracted_at'

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

def json_dump(d):
    try:
        return json.dumps(d, cls=DecimalEncoder).encode('utf-8')
    except Exception as ex:
        LOGGER.info("could not json dump: %s", d)
        raise ex

def now():
    return singer.utils.strftime(singer.utils.now())

def _log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        'Error sending data to Stitch. Sleeping %d seconds before trying again: %s',
        details['wait'], exc)

class StitchHandler: # pylint: disable=too-few-public-methods
    '''Sends messages to Stitch.'''

    def __init__(self, token, client_id, connection_ns, stitch_url, spool_host, spool_s3_bucket):
        self.token = token
        self.client_id = client_id
        self.connection_ns = connection_ns
        self.stitch_url = stitch_url
        self.spool_host = spool_host
        self.session = requests.Session()
        self.s3_conn = boto.connect_s3()
        self.bucket_name = spool_s3_bucket
        self.bucket = self.s3_conn.get_bucket(self.bucket_name, validate=False)
        self.send_methods = {}

    def post_to_s3(self, json_string, num_records, table_name):
        key_name = self.generate_s3_key(json_string)
        k = Key(self.bucket)
        k.key = key_name
        LOGGER.info("Sending batch with %d messages/(%d) bytes for table %s to s3 %s",
                    num_records, len(json_string), table_name, key_name)

        with TIMINGS.mode('post_to_s3'):
            start_persist = time.time()
            k.set_contents_from_string(json_string)
            persist_time = int((time.time() - start_persist) * 1000)

        return (key_name, persist_time)

    def serialize_s3_upsert_messages(self, records, schema, table_name, key_names, table_version ):
        pipeline_messages = []
        for idx, msg in enumerate(records):
            with TIMINGS.mode('build_pipeline_messages'):
                pipeline_message = {'message_version' : MESSAGE_VERSION,
                                    'pipeline_version' : PIPELINE_VERSION,
                                    "timestamps" : {"_rjm_received_at" :  int(time.time() * 1000)},
                                    'body' : {
                                        'client_id' : int(self.client_id),
                                        'namespace' : self.connection_ns,
                                        'table_name' : table_name,
                                        'action'     : 'upsert',
                                        'sequence'   : generate_sequence(idx),
                                        'key_names'  : key_names,
                                        'data': msg
                                    }}
                if table_version is not None:
                    pipeline_message['body']['table_version'] = table_version
                pipeline_messages.append(pipeline_message)
        return pipeline_messages


    def generate_s3_key(self, data):
        return  "{:07d}/{}-{}-{}".format(
            int(self.client_id),
            uuid.uuid4(),
            hashlib.sha1(data).hexdigest(),
            singer.utils.strftime(singer.utils.now(), "%Y%m%d-%H%M%S%f")
        )

    def headers(self):
        '''Return the headers based on the token'''
        return {
            'Authorization': 'Bearer {}'.format(self.token),
            'Content-Type': 'application/json'
        }

    @backoff.on_exception(backoff.expo,
                          RequestException,
                          giveup=singer.utils.exception_is_4xx,
                          max_tries=8,
                          on_backoff=_log_backoff)
    def post_to_gate(self, data):
        '''Send the given data to Stitch, retrying on exceptions'''
        ssl_verify = os.environ.get("TARGET_STITCH_SSL_VERIFY") != 'false'
        response = self.session.post(self.stitch_url,
                                     headers=self.headers(),
                                     data=data,
                                     verify=ssl_verify)
        response.raise_for_status()
        return response

    @backoff.on_exception(backoff.expo,
                          RequestException,
                          giveup=singer.utils.exception_is_4xx,
                          max_tries=8,
                          on_backoff=_log_backoff)
    def post_to_spool(self, body):
        '''Send the given data to the spool, retrying on exceptions'''
        response = self.session.post(STITCH_SPOOL_URL.format(self.spool_host, self.client_id),
                                     headers=self.headers(),
                                     json=body,
                                     verify=False)
        LOGGER.info("SPOOL RESPONSE %s: %s", response.status_code, response.content)
        response.raise_for_status()
        return response

    def handle_batch(self, messages, buffer_size_bytes, schema, key_names, bookmark_names=None):
        with TIMINGS.mode('handle_batch'):
            table_name = messages[0].stream
            if table_name not in self.send_methods:
                if ((buffer_size_bytes >= S3_THRESHOLD_BYTES) or (len(messages) > MAX_NUM_GATE_RECORDS)):
                    self.send_methods[table_name] = 's3'
                else:
                    self.send_methods[table_name] = 'gate'

            # self.send_methods[table_name] = 's3'
            if (self.send_methods.get(table_name) == 's3'):
                self.handle_s3(messages, schema, key_names, bookmark_names=None)
            else:
                self.handle_gate(messages, schema, key_names, bookmark_names=None)

        TIMINGS.log_timings()

    def handle_s3_upserts(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info("handling batch of %s upserts for table %s to s3", len(messages), messages[0].stream)
        table_name = messages[0].stream

        num_records = len(messages)

        with TIMINGS.mode('ensure_multipleof_is_decimal'):
            schema = ensure_multipleof_is_decimal(schema)

        with TIMINGS.mode('make_validator'):
            validator = Draft4Validator(schema, format_checker=FormatChecker())

        LOGGER.info("validating records")
        records = []
        with TIMINGS.mode('validate_records'):
            for msg in messages:
                record = marshall_decimals(schema, msg.record)
                try:
                    validator.validate(record)
                    if key_names:
                        for key in key_names:
                            if key not in record:
                                raise ValueError("Record({}) is missing key property {}.".format(record, key))
                    else:
                        record[SYNTHETIC_PK] = str(uuid.uuid4())

                    if msg.time_extracted:
                        record[TIME_EXTRACTED] = singer.utils.strftime(
                            msg.time_extracted.replace(tzinfo=pytz.UTC))

                except ValidationError as exc:
                    raise ValueError('Record({}) does not conform to schema. Please see logs for details.'
                                     .format(record)) from exc
                except (SchemaError, UnknownType) as exc:
                    raise ValueError('Schema({}) is invalid. Please see logs for details.'
                                     .format(schema)) from exc

                records.append(record)
            if not key_names:
                key_names = [SYNTHETIC_PK]

        with TIMINGS.mode('bookmark_data'):
            if bookmark_names:
                # We only support one bookmark key
                bookmark_key = bookmark_names[0]
                bookmarks = [r[bookmark_key] for r in records]
                bookmark_min = min(bookmarks)
                bookmark_max = max(bookmarks)
                bookmark_metadata = [{
                    "key": bookmark_key,
                    "min_value": bookmark_min,
                    "max_value": bookmark_max,
                }]
            else:
                bookmark_metadata = None

        table_version = determine_table_version(messages[0])
        pipeline_messages = self.serialize_s3_upsert_messages(records, schema, table_name, key_names, table_version)
        json_string = json_dump(schema)
        json_string += b"\n"
        json_string += b"\n".join(json_dump(m) for m in pipeline_messages)
        key_name, persist_time = self.post_to_s3(json_string, num_records, table_name)

        with TIMINGS.mode('post_to_spool'):
            body = {
                "namespace"    : self.connection_ns,
                "table_name"   : table_name,
                "table_version": table_version,
                "action": "upsert",
                "max_time_extracted": now(),
                "bookmark_metadata": bookmark_metadata,
                "s3_key": key_name,
                "s3_bucket": self.bucket_name,
                "num_records": num_records,
                "num_bytes": len(json_string),
                "format": "json+schema",
                "format_version": "1.0.0",
                "persist_duration_millis": persist_time,
            }
            self.post_to_spool(body)

    def handle_s3_activate_version(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info("handling activate_version for table %s to s3", messages[0].stream)
        table_name = messages[0].stream
        table_version = determine_table_version(messages[0])
        pipeline_message = {
            "message_version" : MESSAGE_VERSION,
            "pipeline_version" :  PIPELINE_VERSION,
            "timestamps" : {"_rjm_received_at" : int(time.time() * 1000)},
            "body" : {"client_id" : self.client_id,
                      "namespace" : "perftest",
                      "table_name" : table_name,
                      "action" : "switch_view",
                      "sequence" : generate_sequence(1)}
            }
        data = json_dump(pipeline_message)

        key_name, persist_time = self.post_to_s3(data, 1, table_name)

        with TIMINGS.mode('post_to_spool'):
            body = {
                "namespace"    : self.connection_ns,
                "table_name"   : table_name,
                "table_version": table_version,
                "action": "switch_view",
                "max_time_extracted": now(),
                "bookmark_metadata": None,
                "s3_key": key_name,
                "s3_bucket": self.bucket_name,
                "num_records": 1,
                "num_bytes": len(data),
                "format": "json+schema",
                "format_version": "1.0.0",
                "persist_duration_millis": persist_time,
            }
            self.post_to_spool(body)

    def handle_s3(self, messages, schema, key_names, bookmark_names=None):
        activate_versions = []
        upserts = []
        with TIMINGS.mode('message_classifiation'):
            for msg in messages:
                if isinstance(msg, singer.ActivateVersionMessage):
                    activate_versions.append(msg)
                elif isinstance(msg, singer.RecordMessage):
                    upserts.append(msg)
                else:
                    raise Exception('unrecognized message type')
        if upserts:
            self.handle_s3_upserts(upserts, schema, key_names, bookmark_names)
        if activate_versions:
            self.handle_s3_activate_version(activate_versions, schema, key_names, bookmark_names)

    def handle_gate(self, messages, schema, key_names, bookmark_names=None):
        '''Handle messages by sending them to Stitch.

        If the serialized form of the messages is too large to fit into a
        single request this will break them up into multiple smaller
        requests.
        '''

        LOGGER.info("Sending batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.stitch_url)
        with TIMINGS.mode('serializing'):
            bodies = serialize_gate_messages(messages,
                                             schema,
                                             key_names,
                                             bookmark_names)

        LOGGER.debug('Split batch into %d requests', len(bodies))
        for i, body in enumerate(bodies):
            with TIMINGS.mode('post_to_gate'):
                LOGGER.debug('Request %d of %d is %d bytes', i + 1, len(bodies), len(body))
                try:
                    response = self.post_to_gate(body)
                    LOGGER.debug('Response is %s: %s', response, response.content)

                # An HTTPError means we got an HTTP response but it was a
                # bad status code. Try to parse the "message" from the
                # json body of the response, since Stitch should include
                # the human-oriented message in that field. If there are
                # any errors parsing the message, just include the
                # stringified response.
                except HTTPError as exc:
                    try:
                        response_body = exc.response.json()
                        if isinstance(response_body, dict) and 'message' in response_body:
                            msg = response_body['message']
                        elif isinstance(response_body, dict) and 'error' in response_body:
                            msg = response_body['error']
                        else:
                            msg = '{}: {}'.format(exc.response, exc.response.content)
                    except: # pylint: disable=bare-except
                        LOGGER.exception('Exception while processing error response')
                        msg = '{}: {}'.format(exc.response, exc.response.content)

                    raise TargetStitchException('Error persisting data for table "{}": {}'.format(
                                                messages[0].stream, msg))

                # A RequestException other than HTTPError means we
                # couldn't even connect to stitch. The exception is likely
                # to be very long and gross. Log the full details but just
                # include the summary in the critical error message. TODO:
                # When we expose logs to Stitch users, modify this to
                # suggest looking at the logs for details.
                except RequestException as exc:
                    LOGGER.exception(exc)
                    raise TargetStitchException('Error connecting to Stitch')
