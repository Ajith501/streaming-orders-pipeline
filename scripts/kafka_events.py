import json
import random
from datetime import datetime, timezone
import time

statuses = ["created", "completed", "cancelled"]
customers = [f"C{i}" for i in range(1, 200)]

i = 0

while True:   #  infinite stream

    event = {
        "order_id": f"O{i}",
        "customer_id": random.choice(customers),
        "amount": random.randint(10, 10000),
        "status": random.choice(statuses),

        # proper timestamp
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    # flush ensures real-time streaming
    print(json.dumps(event), flush=True)

    i += 1

    # control speed
    time.sleep(0.002)