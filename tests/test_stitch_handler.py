import io
import json
import unittest
from unittest import mock
from transit.reader import Reader
import datetime
import pytz
from decimal import Decimal

import singer
from target_stitch.handlers import stitch_handler, common
from target_stitch.exceptions import BatchTooLargeException

test_client_id = 1
test_namespace = "test"

def mk_schema(stream, schema, key_properties=["name"]):
    return json.dumps({
        "type": "SCHEMA",
        "stream": stream,
        "key_properties": key_properties,
        "schema": {
            "type": "object",
            "properties": schema,
        },
    })


def mk_version(stream, version):
    return json.dumps({
        "type": "ACTIVATE_VERSION",
        "stream": stream,
        "version": version,
    })


def mk_record(stream, record, version=None):
    msg = {
        "type": "RECORD",
        "stream": stream,
        "record": record,
    }
    if version:
        msg["version"] = version

    return json.dumps(msg)


def decode_transit(data):
    buf = io.BytesIO(data)
    reader = Reader("msgpack")
    return dict(reader.read(buf))


class TestStitchHandler(unittest.TestCase):
    def setUp(self):
        with mock.patch('boto.connect_s3'):
            self.handler = stitch_handler.StitchHandler(
                token="token",
                client_id=test_client_id,
                connection_ns=test_namespace,
                stitch_url="gate",
                spool_host="spool",
                spool_s3_bucket="bucket",
            )

    def test_post_to_gate(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        with mock.patch.object(self.handler, 'post_to_gate') as mock_post:
            self.handler.handle_batch(
                messages,
                1,
                schema.schema,
                schema.key_properties,
                schema.bookmark_properties)

        actual = json.loads(mock_post.call_args[0][0])
        self.assertEqual("test1", actual["table_name"])
        self.assertEqual(schema.schema, actual["schema"])
        self.assertEqual(schema.key_properties, actual["key_names"])
        self.assertEqual(1, len(actual["messages"]))
        self.assertEqual(record_data, actual["messages"][0]["data"])
        self.assertEqual("upsert", actual["messages"][0]["action"])
        self.assertIn("sequence", actual["messages"][0])

    def test_post_to_s3(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"
        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        # ensure we sent the activate version and messages separately
        self.assertEqual(2, len(mock_post.call_args_list))

        actual = mock_post.call_args_list[0][0][0]
        expected = {
            "table_name": "test1",
            "table_version": 1,
            "action": "upsert",
            "s3_bucket": "bucket",
            "s3_key": key_name,
            "persist_duration_millis": 1,
            "format": "transit+msgpack",
            "bookmark_metadata": None,
        }
        self.assertDictContainsSubset(expected, actual)

        actual = mock_post.call_args_list[1][0][0]
        expected = {
            "table_name": "test1",
            "table_version": 1,
            "action": "switch_view",
            "s3_bucket": "bucket",
            "s3_key": key_name,
            "persist_duration_millis": 1,
            "format": "transit+msgpack",
            "bookmark_metadata": None,
        }
        self.assertDictContainsSubset(expected, actual)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        self.assertEqual(2, actual["message_version"])
        self.assertEqual("2", actual["pipeline_version"])

        expected = {
            "namespace": "test",
            "table_name": "test1",
            "action": "upsert",
            "client_id": test_client_id,
        }

        self.assertIsNotNone(actual["body"]["table_version"])
        self.assertDictContainsSubset(expected, dict(actual["body"]))
        self.assertIn("sequence", actual["body"])
        self.assertIn("key_names", actual["body"])
        self.assertDictEqual(record_data, dict(actual["body"]["data"]))

    def test_same_path(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_gate') as mock_post_gate:
            with mock.patch.object(self.handler, 'post_to_spool') as mock_post_spool:
                with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

                    self.handler.handle_batch(
                        messages,
                        1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

        mock_post_gate.assert_not_called()
        self.assertEqual(4, len(mock_post_spool.call_args_list))
        self.assertEqual(4, len(mock_post_s3.call_args_list))

    def test_invalid_schema(self):
        schema_data = {"name": {"type": "orange"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                with self.assertRaises(ValueError):
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

    def test_invalid_record(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": 1}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                with self.assertRaises(ValueError):
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

    def test_missing_key_property(self):
        schema_data = {"name": {"type": ["null", "string"]}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                with self.assertRaises(ValueError):
                    self.handler.handle_batch(messages,
                                              stitch_handler.S3_THRESHOLD_BYTES + 1,
                                              schema.schema,
                                              schema.key_properties,
                                              schema.bookmark_properties)

    def test_datetimes(self):
        schema_data = {
            "name": {"type": "string"},
            "date": {"type": "string", "format": "date-time"},
        }
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {
            "name": "test1-0",
            "date": "2018-01-01T00:00:00Z",
        }
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"
        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        expected = {
            "name": "test1-0",
            "date": datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC),
        }
        self.assertIsNotNone(actual["body"]["table_version"])
        self.assertDictEqual(expected, dict(actual["body"]["data"]))
        self.assertEqual(['name'], list(actual['body']['key_names']))

    def test_synthetic_pks(self):
        schema_data = {
            "name": {"type": "string"},
            "money": {"type": "number", "multipleOf": 0.01},
        }
        schema = singer.parse_message(mk_schema("test1", schema_data, None))
        record_data = {
            "name": "test1-0",
            "money": 10.42,
        }
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        # import pdb
        # pdb.set_trace()
        expected = {
            "name": "test1-0",
            "money": Decimal("10.42"),
        }
        self.assertDictContainsSubset(expected, dict(actual["body"]["data"]))
        self.assertEqual([stitch_handler.SYNTHETIC_PK], list(actual['body']['key_names']))
        self.assertIsNotNone( actual['body']['data'][stitch_handler.SYNTHETIC_PK])

    def test_time_extracted(self):
        schema_data = {
            "name": {"type": "string"},
            "money": {"type": "number", "multipleOf": 0.01},
        }
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {
            "name": "test1-0",
            "money": 10.42,
        }
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        # set time extracted on the record message so it gets added
        messages[0].time_extracted = datetime.datetime.utcnow()
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        expected = {
            "name": "test1-0",
            "money": Decimal("10.42"),
        }
        self.assertDictContainsSubset(expected, dict(actual["body"]["data"]))
        self.assertIsNotNone(actual["body"]["data"].get(stitch_handler.TIME_EXTRACTED))

    def test_decimals(self):
        schema_data = {
            "name": {"type": "string"},
            "money": {"type": "number", "multipleOf": 0.01},
        }
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {
            "name": "test1-0",
            "money": 10.42,
        }
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        expected = {
            "name": "test1-0",
            "money": Decimal("10.42")
        }
        self.assertIsNotNone(actual["body"]["table_version"])
        self.assertDictEqual(expected, dict(actual["body"]["data"]))

    def test_gate_partition_by_number_bytes(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        messages = [singer.parse_message(record), singer.parse_message(record)]

        max_gate_bytes = common.MAX_NUM_GATE_BYTES
        common.MAX_NUM_GATE_BYTES = 250
        actual = common.serialize_gate_messages(
            messages=messages,
            schema=schema.schema,
            key_names=schema.key_properties,
            bookmark_names=None)
        self.assertEqual(2, len(actual), actual)
        common.MAX_NUM_GATE_BYTES = max_gate_bytes

    def test_gate_partition_by_number_messages(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        messages = [singer.parse_message(record), singer.parse_message(record)]

        max_gate_records = common.MAX_NUM_GATE_RECORDS
        common.MAX_NUM_GATE_RECORDS = 1
        actual = common.serialize_gate_messages(
            messages=messages,
            schema=schema.schema,
            key_names=schema.key_properties,
            bookmark_names=None)
        self.assertEqual(2, len(actual), actual)
        common.MAX_NUM_GATE_RECORDS = max_gate_records

    def test_gate_partition_failure(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        messages = [singer.parse_message(record), singer.parse_message(record)]

        max_gate_records = common.MAX_NUM_GATE_RECORDS
        common.MAX_NUM_GATE_RECORDS = 1
        max_gate_bytes = common.MAX_NUM_GATE_BYTES
        common.MAX_NUM_GATE_BYTES = 1

        with self.assertRaises(BatchTooLargeException):
            actual = common.serialize_gate_messages(
                messages=messages,
                schema=schema.schema,
                key_names=schema.key_properties,
                bookmark_names=None)

        common.MAX_NUM_GATE_RECORDS = max_gate_records
        common.MAX_NUM_GATE_BYTES = max_gate_bytes
