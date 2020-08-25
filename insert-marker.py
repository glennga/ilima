import __init__
import logging
import argparse
import json
import socket
import datetime
import uuid

logger = logging.getLogger(__name__)


def _insert_marker(comment: str, **kwargs):
    # Generate our entry.
    entry = {
        "id": str(uuid.uuid4()),
        "startTime": str(datetime.datetime.now()),
        "endTime": str(str(datetime.datetime.now())),
        "markerComment": comment,
        "isMarker": True
    }

    # Record the comment to our log file.
    logger.info(f'Comment being logged: {comment}')

    # Record the comment to our cluster.
    results_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    results_socket.connect((kwargs['analysisAsterixCluster']['nodeController']['address'],
                           kwargs['analysisAsterixCluster']['feedSocketPort']))
    logger.debug('Recording entry to cluster through feed.')
    if not results_socket.sendall(json.dumps(entry).encode('ascii')) is None:
        logger.warning('Analysis cluster did not accept record!')
    else:
        logger.info('Result successfully sent to cluster.')

    results_socket.close()


if __name__ == '__main__':
    logger.info('Running insert-marker.py.')
    parser = argparse.ArgumentParser(description='Log a marker w/ a specified comment.')
    parser.add_argument('comment', type=str, help='Comment to associated with the marker to-be-generated.')
    parser.add_argument('--config', type=str, default='config.json', help='Path to the configuration file.')
    command_line_args = parser.parse_args()
    with open(command_line_args.config) as config_file:
        config_json = json.load(config_file)

    # noinspection PyBroadException
    try:
        _insert_marker(command_line_args.comment, **config_json)
    except Exception as uncaught_exception:
        logger.error('Uncaught exception! ' + str(uncaught_exception))

    logger.info('Exiting insert-marker.py.')
