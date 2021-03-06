#!/usr/bin/python

import random
import requests
import string
import time
import uuid

while True:
    time.sleep(0.0003)

    data = {
        "source": str(uuid.uuid4()),
        "schema": "events",
        "warehouse": "dev",
        "client_timestamp": int(round(time.time() * 1000)),
        "data": {
            "event_key": ''.join(random.choices(['event_post_create', 'event_post_delete', 'event_post_flag', 'event_channel_change', 'event_rhs_close'])),
            "randum_string_id": ''.join(random.choices(string.ascii_lowercase + string.digits, k=26)),
            "time": '{}'.format(time.time()),
            "float": 123.456,
            "another_raudnm_str": ''.join(random.choices(string.ascii_lowercase + string.digits, k=26)),
        }
    }

    requests.post("http://localhost:8000/v0/log", json=data)


