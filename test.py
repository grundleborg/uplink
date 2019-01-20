#!/usr/bin/python

import random
import requests
import string
import time
import uuid

while True:
    time.sleep(0.03)

    data = {
        "source": str(uuid.uuid4()),
        "schema": "events",
        "data": {
            "things": ''.join(random.choices(['event_post_create', 'event_post_delete', 'event_post_flag', 'event_channel_change', 'event_rhs_close'])),
            "foo": ''.join(random.choices(string.ascii_lowercase + string.digits, k=26)),
            "fizz": '{}'.format(time.time()),
            "float": 123.456,
            "splunk": ''.join(random.choices(string.ascii_lowercase + string.digits, k=26)),
        }
    }

    requests.post("http://localhost:8000/log", json=data)


