from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json, time

app = FastAPI(title="Chatter Relay", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

VALID_USERS = {"usman", "asma"}
connections: dict[str, WebSocket] = {}


@app.websocket("/ws/{user_id}")
async def relay(websocket: WebSocket, user_id: str):
    if user_id not in VALID_USERS:
        await websocket.close(code=4001, reason="Unknown user")
        return

    await websocket.accept()
    connections[user_id] = websocket

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

            # Acknowledge receipt to sender instantly
            await websocket.send_json({"type": "ack", "id": msg.get("id"), "ts": time.time()})

            # Forward to the other user if they're connected
            if target in connections:
                try:
                    await connections[target].send_json({"type": "message", "data": msg})
                except Exception:
                    del connections[target]

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


def _get_peer(user_id: str) -> str:
    return "asma" if user_id == "usman" else "usman"


@app.get("/health")
async def health():
    return {"status": "ok", "active": list(connections.keys())}
