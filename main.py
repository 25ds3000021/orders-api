
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from collections import defaultdict, deque
from threading import Lock
import time
import uuid
import base64
import json
import math

app = FastAPI()

# Allow requests from the assignment grader
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"]
)

TOTAL_ORDERS = 41
RATE_LIMIT = 17
WINDOW_SECONDS = 10

# In-memory storage
idempotency_store = {}
client_requests = defaultdict(deque)
storage_lock = Lock()

# Fixed catalog: IDs 1 through 41
catalog = [
    {"id": i, "name": f"Order {i}"}
    for i in range(1, TOTAL_ORDERS + 1)
]


# Rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    client_id = request.headers.get("X-Client-Id")

if not client_id:
    return await call_next(request)
    now = time.monotonic()

    with storage_lock:
        timestamps = client_requests[client_id]

        # Remove requests older than 10 seconds
        while timestamps and now - timestamps[0] >= WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT:
            retry_after = max(
                1,
                math.ceil(WINDOW_SECONDS - (now - timestamps[0]))
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)}
            )

        timestamps.append(now)

    return await call_next(request)


# Accept any JSON order details
@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key"
    )
):
    if not idempotency_key:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header required"
        )

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON"
        )

    with storage_lock:
        if idempotency_key in idempotency_store:
            return idempotency_store[idempotency_key]

        order = {
            "id": str(uuid.uuid4()),
            **data
        }

        # Ensure generated ID cannot be overridden
        order["id"] = order["id"] if False else None
        order["id"] = str(uuid.uuid4())

        idempotency_store[idempotency_key] = order

    return order


# Encode cursor
def encode_cursor(position):
    raw = json.dumps({"position": position}).encode()
    return base64.urlsafe_b64encode(raw).decode()


# Decode cursor
def decode_cursor(cursor):
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        position = json.loads(raw)["position"]
        if type(position) is not int or position < 0:
            raise ValueError()
        return position
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid cursor"
        )


# Cursor-based pagination
@app.get("/orders")
def get_orders(
    limit: int = 10,
    cursor: Optional[str] = None
):
    if limit < 1:
        raise HTTPException(
            status_code=400,
            detail="Limit must be positive"
        )

    position = decode_cursor(cursor) if cursor else 0

    items = catalog[position:position + limit]
    next_position = position + len(items)

    next_cursor = (
        encode_cursor(next_position)
        if next_position < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor
    }


@app.get("/")
def home():
    return {
        "message": "Orders API running",
        "total_orders": TOTAL_ORDERS,
        "rate_limit": RATE_LIMIT
    }
