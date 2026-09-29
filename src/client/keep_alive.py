from __future__ import annotations
import asyncio
import datetime
import logging
import time
import uuid
from typing import Dict, Optional, Callable, Awaitable, Any, Tuple
from src.client.config import AppConfig
from src.client.peer_table import PeerTable
from src.client.peer_connection import ConnectionManager

logger = logging.getLogger("KeepAlive")


def iso_utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class KeepAliveManager:
    """
    Manages periodic PING/PONG heartbeats, computes RTT, and detects STALE peers.
    """

    def __init__(
        self,
        config: AppConfig,
        peer_table: PeerTable,
        conn_mgr: ConnectionManager,
        on_stale: Callable[[str], Awaitable[None]],
    ):
        self.config = config
        self.peer_table = peer_table
        self.conn_mgr = conn_mgr
        self.on_stale = on_stale

        # msg_id -> (peer_id, sent_monotonic_time)
        self._pending_pings: Dict[str, Tuple[str, float]] = {}
        # peer_id -> bool indicating if pong was received for current ping round
        self._awaiting_pong: Dict[str, bool] = {}
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._ping_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _ping_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.config.ping_interval)
                await self._send_pings_to_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[KeepAlive] Loop exception: {e}")

    async def _send_pings_to_all(self) -> None:
        connected = self.peer_table.connected_peers()
        now_mono = time.monotonic()

        for peer in connected:
            pid = peer.peer_id
            conn = self.conn_mgr.get_connection(pid)
            if not conn or conn.is_closed:
                continue

            # Check if previous ping went unanswered
            if self._awaiting_pong.get(pid, False):
                missed = self.peer_table.increment_missed_pings(pid)
                if missed >= self.config.ping_miss_limit:
                    logger.warning(f"[KeepAlive] {pid} missed {missed} PINGs — marking STALE")
                    self.peer_table.mark_stale(pid)
                    self._awaiting_pong.pop(pid, None)
                    await conn.close()
                    await self.on_stale(pid)
                    continue
            else:
                self._awaiting_pong[pid] = True

            # Send new PING
            mid = str(uuid.uuid4())
            self._pending_pings[mid] = (pid, now_mono)

            ping_msg = {
                "type": "PING",
                "msg_id": mid,
                "timestamp": iso_utc_now(),
                "ttl": 1,
            }
            await conn.send_json(ping_msg)

    async def handle_ping(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Receives PING from peer, responds with PONG repeating msg_id and timestamp.
        """
        mid = msg.get("msg_id", str(uuid.uuid4()))
        ts = msg.get("timestamp", iso_utc_now())

        pong_msg = {
            "type": "PONG",
            "msg_id": mid,
            "timestamp": ts,
            "ttl": 1,
        }
        await conn.send_json(pong_msg)

    async def handle_pong(self, peer_id: str, msg: Dict[str, Any]) -> None:
        """
        Receives PONG from peer, calculates RTT, resets missed pings.
        """
        mid = msg.get("msg_id")
        self._awaiting_pong[peer_id] = False
        self.peer_table.record_pong(peer_id)

        now_mono = time.monotonic()
        if mid and mid in self._pending_pings:
            _, sent_mono = self._pending_pings.pop(mid)
            rtt_sec = max(0.0, now_mono - sent_mono)
            self.peer_table.record_rtt(peer_id, rtt_sec)

            peer_entry = self.peer_table.get(peer_id)
            avg_rtt = peer_entry.avg_rtt_ms if peer_entry else None
            avg_str = f" (avg {avg_rtt:.1f} ms)" if avg_rtt is not None else ""
            logger.info(f"[KeepAlive] {peer_id} RTT={rtt_sec * 1000.0:.1f} ms{avg_str}")
        else:
            logger.debug(f"[KeepAlive] Received PONG from {peer_id} without matched pending ping")
