from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Dict, Any, Callable, Awaitable, Optional
from src.client.config import AppConfig

logger = logging.getLogger("MessageRouter")

CONNECTION_MESSAGE_TYPES = {"HELLO", "HELLO_OK", "PING", "PONG"}


class MessageRouter:
    """
    Validates message envelope (type, msg_id, ttl=1, src, dst) and dispatches
    to registered handlers for session, lobby, and gameplay.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._handlers: Dict[str, Callable[[Any, Dict[str, Any]], Awaitable[None]]] = {}

    def register(self, msg_type: str, handler: Callable[[Any, Dict[str, Any]], Awaitable[None]]) -> None:
        self._handlers[msg_type.upper()] = handler

    async def route(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Validates envelope and dispatches to handler.
        conn is PeerConnection.
        """
        if not isinstance(msg, dict):
            await self._send_error(conn, None, "bad_request", "Message must be a JSON object")
            return

        msg_type = msg.get("type")
        msg_id = msg.get("msg_id")
        ttl = msg.get("ttl")

        if not msg_type or not isinstance(msg_type, str):
            await self._send_error(conn, msg_id, "bad_request", "Missing or invalid 'type'")
            return

        msg_type = msg_type.upper()

        if not msg_id or not isinstance(msg_id, str):
            await self._send_error(conn, None, "bad_request", "Missing or invalid 'msg_id'")
            return

        if ttl != 1:
            logger.debug(f"[MessageRouter] Discarding message from {conn.peer_id} with ttl={ttl} (must be 1)")
            return

        # Check src and dst for non-connection messages
        if msg_type not in CONNECTION_MESSAGE_TYPES:
            src = msg.get("src")
            dst = msg.get("dst")

            if not src or not dst:
                await self._send_error(conn, msg_id, "bad_request", "Missing 'src' or 'dst'")
                return

            if dst != self.config.peer_id:
                logger.warning(f"[MessageRouter] Message addressed to {dst}, but this is {self.config.peer_id}")
                await self._send_error(conn, msg_id, "not_for_me", f"Message addressed to {dst}")
                return

        handler = self._handlers.get(msg_type)
        if not handler:
            logger.warning(f"[MessageRouter] Unknown message type '{msg_type}' from {conn.peer_id}")
            await self._send_error(conn, msg_id, "unknown_type", f"Type '{msg_type}' is unknown")
            return

        try:
            await handler(conn, msg)
        except Exception as e:
            logger.error(f"[MessageRouter] Error handling {msg_type} from {conn.peer_id}: {e}", exc_info=True)
            await self._send_error(conn, msg_id, "invalid_state", str(e))

    async def _send_error(
        self,
        conn: Any,
        ref_msg_id: Optional[str],
        code: str,
        message: str,
    ) -> None:
        err = {
            "type": "ERROR",
            "msg_id": str(uuid.uuid4()),
            "src": self.config.peer_id,
            "dst": conn.peer_id,
            "ref_msg_id": ref_msg_id,
            "code": code,
            "message": message,
            "ttl": 1,
        }
        try:
            await conn.send_json(err)
        except Exception:
            pass
