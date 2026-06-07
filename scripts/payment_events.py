import json
import random
from datetime import datetime, timezone
import time

methods = ["card", "wallet", "upi"]
countries = ["US", "UK", "DE", "IN"]
customers = [f"C{i}" for i in range(1, 200)]

i = 0

while True:

    amount = random.choice([
        random.randint(5, 20),
        random.randint(20, 100),
        random.randint(100, 500)
    ])

    status = "success" if random.random() < 0.9 else "failed"

    event = {
        "payment_id": f"P{i}",
        "order_id": f"O{i}",
        "customer_id": random.choice(customers),
        "amount": amount,
        "method": random.choice(methods),
        "status": status,
        "country": random.choice(countries),

        # ✅ FIXED TIMESTAMP
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    print(json.dumps(event), flush=True)

    i += 1
    time.sleep(0.05)