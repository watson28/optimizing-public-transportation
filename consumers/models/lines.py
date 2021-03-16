"""Contains functionality related to Lines"""
import json
import logging

from models import Line


logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self._lines = [Line('red'), Line('green'), Line('blue')]

    def process_new_arrival_message(self, message):
        value = message.value()
        line = self._get_station_by_color(value['line'])
        line.process_new_arrival_message(message)

    def process_station_update_message(self, message):
        value = json.loads(message.value())
        line = self._get_station_by_color(value['line'])
        line.process_station_update_message(message)

    def process_turnstile_update_message(self, message):
        for line in self._lines:
            line.process_turnstile_update_message(message)

    def _get_station_by_color(self, color: str):
        try:
            return next((line for line in self._lines if line.color == color))
        except StopIteration as exception:
            raise ValueError(f'Not existing line with color {color}') from exception

    def get_lines(self):
        return self._lines
