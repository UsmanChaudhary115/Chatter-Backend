from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
import json, time

app = FastAPI(title="Chatter Relay", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

VALID_USERS = {"usman", "asma"}
connections: dict[str, WebSocket] = {}

# In-memory message queue for offline users: user_id → [(msg, timestamp), ...]
message_queue: dict[str, list[tuple]] = defaultdict(list)

# Seen messages to prevent duplicates: (recipient, msg_id) → timestamp
seen_messages: dict[tuple[str, str], float] = {}


@app.websocket("/ws/{user_id}")
async def relay(websocket: WebSocket, user_id: str):
    if user_id not in VALID_USERS:
        await websocket.close(code=4001, reason="Unknown user")
        return

    await websocket.accept()
    connections[user_id] = websocket

    # Purge stale messages and seen entries before draining queue
    _purge_stale_messages()
    _purge_stale_seen()

    # Drain queued messages for this user
    if message_queue[user_id]:
        for queued_msg, _ in message_queue[user_id]:
            try:
                await websocket.send_json({"type": "message", "data": queued_msg})
            except Exception:
                pass
        message_queue[user_id].clear()

    # Notify the other user that this user came online
    peer = _get_peer(user_id)
    if peer in connections:
        try:
            await connections[peer].send_json({"type": "presence", "user": user_id, "status": "online"})
        except Exception:
            pass

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            target = msg.get("to")
            msg_id = msg.get("id")

            # Check if this message was already seen (prevent duplicates from retries)
            seen_key = (target, msg_id)
            if seen_key in seen_messages:
                # Still send ack for deduped messages
                await websocket.send_json({"type": "ack", "id": msg_id, "ts": time.time()})
                continue

            # Mark this message as seen
            seen_messages[seen_key] = time.time()

            # Forward to the other user if they're connected, otherwise queue it
            if target in connections:
                try:
                    await connections[target].send_json({"type": "message", "data": msg})
                except Exception:
                    del connections[target]
                    # If send failed, queue it anyway
                    message_queue[target].append((msg, time.time()))
            else:
                # Target is offline, queue the message for later delivery
                message_queue[target].append((msg, time.time()))

            # Send ack AFTER message is queued/forwarded (more accurate semantics)
            await websocket.send_json({"type": "ack", "id": msg_id, "ts": time.time()})

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        connections.pop(user_id, None)
        # Notify the peer that this user went offline
        if peer in connections:
            try:
                await connections[peer].send_json({"type": "presence", "user": user_id, "status": "offline"})
            except Exception:
                pass



def _purge_stale_messages(ttl_seconds: int = 300) -> None:
    """Remove queued messages older than ttl_seconds (default 5 minutes)."""
    now = time.time()
    for user_id in message_queue:
        message_queue[user_id] = [
            (msg, ts) for msg, ts in message_queue[user_id]
            if now - ts < ttl_seconds
        ]


def _purge_stale_seen(ttl_seconds: int = 600) -> None:
    """Remove seen message entries older than ttl_seconds (default 10 minutes)."""
    now = time.time()
    stale_keys = [
        key for key, ts in seen_messages.items()
        if now - ts >= ttl_seconds
    ]
    for key in stale_keys:
        del seen_messages[key]


def _get_peer(user_id: str) -> str:
    return "asma" if user_id == "usman" else "usman"


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "active": list(connections.keys()),
        "queued": {user_id: len(msgs) for user_id, msgs in message_queue.items() if msgs}
    }
