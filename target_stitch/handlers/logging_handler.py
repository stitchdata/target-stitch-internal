import singer
from target_stitch.handlers.common import serialize_gate_messages

LOGGER = singer.get_logger().getChild('target_stitch')

def ensure_multipleof_is_float(schema):
    '''Ensure multipleOf (if exists) points to a Decimal.

        Recursively walks the given schema (which must be dict), converting
        every instance of the multipleOf keyword to a Decimal.

        This modifies the input schema and also returns it.

        '''
    if 'multipleOf' in schema:
        schema['multipleOf'] = float(schema['multipleOf'])

    if 'properties' in schema:
        for k, v in schema['properties'].items():
            ensure_multipleof_is_float(v)

    if 'items' in schema:
        ensure_multipleof_is_float(schema['items'])

    return schema

class LoggingHandler:  # pylint: disable=too-few-public-methods
    '''Logs records to a local output file.'''
    def __init__(self, output_file):
        self.output_file = output_file

    def handle_batch(self, messages, buffer_size_bytes, schema, key_names, bookmark_names=None):
        '''Handles a batch of messages by saving them to a local output file.

        Serializes records in the same way StitchHandler does, so the
        output file should contain the exact request bodies that we would
        send to Stitch.
        '''
        schema = ensure_multipleof_is_float(schema)
        LOGGER.info("Saving batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.output_file.name)

        for i, body in enumerate(serialize_gate_messages(messages,
                                                         schema, key_names,
                                                         bookmark_names)):
            LOGGER.debug("Request body %d is %d bytes", i, len(body))
            self.output_file.write(body)
            self.output_file.write('\n')
