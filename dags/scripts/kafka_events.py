import json
import random
from datetime import datetime, timezone

for i in range(5000):
    event = {
        "order_id": str(100000+i),
        "customer_id": f"C{random.randint(1,1000)}",
        "amount": random.randint(50,500),
        "status": "CREATED",
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    print(json.dumps(event))