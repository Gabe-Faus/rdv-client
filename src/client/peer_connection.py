from __future__ import annotations
import asyncio
import json
import logging
import uuid
from typing import Dict, Optional, Callable, Awaitable, Any, List
from src.client.config import AppConfig

logger = logging.getLogger("PeerServer")


class PeerConnection:
    """
    Represents an active TCP connection to a peer (either inbound or outbound).
    Provides line-based JSON framing and thread/task-safe sending.
    """

    def __init__(
        self,
        peer_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        direction: str,  # "inbound" | "outbound"
        features: List[str],
        on_message_cb: Callable[[PeerConnection, Dict[str, Any]], Awaitable[None]],
        on_close_cb: Callable[[PeerConnection, Optional[Exception]], Awaitable[None]],
    ):
        self.peer_id = peer_id
        self.reader = reader
        self.writer = writer
        self.direction = direction
        self.features = features
        self.on_message_cb = on_message_cb
        self.on_close_cb = on_close_cb
        self.is_closed = False
        self._send_lock = asyncio.Lock()
        self._read_task: Optional[asyncio.Task] = None

    def start_reading(self) -> None:
        self._read_task = asyncio.create_task(self._read_loop())

    async def send_json(self, payload: Dict[str, Any]) -> bool:
        """
        Sends a single JSON line terminated by \n.
        """
        if self.is_closed:
            return False

        try:
            line = json.dumps(payload, ensure_ascii=False) + "\n"
            raw_data = line.encode("utf-8")
            if len(raw_data) > 32768:
                logger.error(f"[PeerServer] Outgoing message to {self.peer_id} exceeds 32KB")
                return False

            async with self._send_lock:
                if self.is_closed:
                    return False
                self.writer.write(raw_data)
                await self.writer.drain()
            logger.debug(f"P2P OUT -> {self.peer_id}: {payload}")
            return True
        except Exception as e:
            logger.warning(f"[PeerServer] Send failed to {self.peer_id}: {e}")
            await self.close(exception=e)
            return False

    async def _read_loop(self) -> None:
        close_err: Optional[Exception] = None
        try:
            while not self.is_closed:
                line_bytes = await self.reader.readline()
                if not line_bytes:
                    # Connection closed by remote
                    break

                if len(line_bytes) > 32768:
                    logger.warning(f"[PeerServer] Received line from {self.peer_id} exceeds 32KB, closing")
                    break

                raw_line = line_bytes.decode("utf-8").strip()
                if not raw_line:
                    continue

                try:
                    msg = json.loads(raw_line)
                    logger.debug(f"P2P IN <- {self.peer_id}: {msg}")
                except json.JSONDecodeError as err:
                    logger.warning(f"[PeerServer] Invalid JSON from {self.peer_id}: {raw_line}")
                    # Linhas que não sejam JSON válido devem ser respondidas com ERROR e fechada
                    err_msg = {
                        "type": "ERROR",
                        "msg_id": str(uuid.uuid4()),
                        "code": "bad_json",
                        "message": "Malformed JSON line",
                        "ttl": 1,
                    }
                    try:
                        await self.send_json(err_msg)
                    except Exception:
                        pass
                    break

                # Dispatch message
                await self.on_message_cb(self, msg)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            close_err = e
            logger.debug(f"[PeerServer] Read error on {self.peer_id}: {e}")
        finally:
            await self.close(exception=close_err)

    async def close(self, exception: Optional[Exception] = None) -> None:
        if self.is_closed:
            return
        self.is_closed = True

        if self._read_task and not self._read_task.done() and asyncio.current_task() != self._read_task:
            self._read_task.cancel()

        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

        await self.on_close_cb(self, exception)


class ConnectionManager:
    """
    Manages active TCP connections, handles inbound server and outbound dialing,
    performs HELLO handshakes and enforces the duplicate connection tie-breaking rule.
    """

    def __init__(
        self,
        config: AppConfig,
        on_message: Callable[[PeerConnection, Dict[str, Any]], Awaitable[None]],
        on_peer_connected: Callable[[PeerConnection], Awaitable[None]],
        on_peer_disconnected: Callable[[str, Optional[Exception]], Awaitable[None]],
    ):
        self.config = config
        self.on_message = on_message
        self.on_peer_connected = on_peer_connected
        self.on_peer_disconnected = on_peer_disconnected

        self.active_connections: Dict[str, PeerConnection] = {}
        self._server: Optional[asyncio.Server] = None
        self._lock = asyncio.Lock()

    async def start_server(self) -> None:
        """
        Starts listening for incoming peer connections.
        """
        self._server = await asyncio.start_server(
            self._handle_inbound,
            host="0.0.0.0",
            port=self.config.listen_port,
        )
        logger.info(f"[PeerServer] Listening on 0.0.0.0:{self.config.listen_port}")

    async def stop_server(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("[PeerServer] Server stopped")

    def get_connection(self, peer_id: str) -> Optional[PeerConnection]:
        return self.active_connections.get(peer_id)

    async def connect_to_peer(self, ip: str, port: int, peer_id: str) -> Optional[PeerConnection]:
        """
        Initiates an outbound connection to a discovered peer and completes HELLO handshake.
        """
        async with self._lock:
            existing = self.active_connections.get(peer_id)
            if existing and not existing.is_closed:
                logger.debug(f"[PeerServer] Already connected to {peer_id}")
                return existing

        logger.debug(f"[PeerServer] Dialing {peer_id} at {ip}:{port}...")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=5.0,
            )
        except Exception as e:
            logger.warning(f"[PeerServer] Failed to connect to {peer_id} ({ip}:{port}): {e}")
            await self.on_peer_disconnected(peer_id, e)
            return None

        # Send HELLO
        hello_msg_id = str(uuid.uuid4())
        hello_payload = {
            "type": "HELLO",
            "msg_id": hello_msg_id,
            "peer_id": self.config.peer_id,
            "version": "1.0",
            "features": ["hangman"],
            "ttl": 1,
        }

        try:
            line = json.dumps(hello_payload, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))
            await writer.drain()

            # Wait for HELLO_OK
            raw_resp = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw_resp:
                raise ConnectionError("Connection closed before HELLO_OK")

            resp = json.loads(raw_resp.decode("utf-8").strip())
            if resp.get("type") != "HELLO_OK" or resp.get("msg_id") != hello_msg_id:
                raise ConnectionError(f"Unexpected handshake response: {resp}")

            remote_peer_id = resp.get("peer_id", peer_id)
            features = resp.get("features", [])

            conn = PeerConnection(
                peer_id=remote_peer_id,
                reader=reader,
                writer=writer,
                direction="outbound",
                features=features,
                on_message_cb=self.on_message,
                on_close_cb=self._handle_conn_close,
            )

            # Tie-breaker check against any existing connection
            winner_conn = await self._resolve_duplicate(conn)
            if winner_conn is conn:
                conn.start_reading()
                logger.info(f"[PeerServer] Outbound connected: {remote_peer_id} (features={','.join(features)})")
                await self.on_peer_connected(conn)
                return conn
            else:
                return None

        except Exception as e:
            logger.warning(f"[PeerServer] Handshake failed with {peer_id}: {e}")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            await self.on_peer_disconnected(peer_id, e)
            return None

    async def _handle_inbound(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Handles incoming connection, waits for HELLO, replies HELLO_OK, resolves duplicates.
        """
        try:
            raw_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if not raw_line:
                writer.close()
                await writer.wait_closed()
                return

            msg = json.loads(raw_line.decode("utf-8").strip())
            if msg.get("type") != "HELLO":
                logger.warning(f"[PeerServer] Inbound connection sent non-HELLO first: {msg}")
                writer.close()
                await writer.wait_closed()
                return

            remote_peer_id = msg.get("peer_id")
            if not remote_peer_id:
                writer.close()
                await writer.wait_closed()
                return

            req_msg_id = msg.get("msg_id", str(uuid.uuid4()))
            features = msg.get("features", [])

            # Send HELLO_OK
            hello_ok = {
                "type": "HELLO_OK",
                "msg_id": req_msg_id,
                "peer_id": self.config.peer_id,
                "version": "1.0",
                "features": ["hangman"],
                "ttl": 1,
            }
            line = json.dumps(hello_ok, ensure_ascii=False) + "\n"
            writer.write(line.encode("utf-8"))
            await writer.drain()

            conn = PeerConnection(
                peer_id=remote_peer_id,
                reader=reader,
                writer=writer,
                direction="inbound",
                features=features,
                on_message_cb=self.on_message,
                on_close_cb=self._handle_conn_close,
            )

            winner_conn = await self._resolve_duplicate(conn)
            if winner_conn is conn:
                conn.start_reading()
                logger.info(f"[PeerServer] Inbound connected: {remote_peer_id} (features={','.join(features)})")
                await self.on_peer_connected(conn)

        except Exception as e:
            logger.debug(f"[PeerServer] Inbound handshake exception: {e}")
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _resolve_duplicate(self, new_conn: PeerConnection) -> PeerConnection:
        """
        Enforces tie-breaker rule:
        Keep the connection opened by the peer with lower peer_id (lexicographical order).
        Close the other with BYE reason: "duplicate".
        """
        async with self._lock:
            existing = self.active_connections.get(new_conn.peer_id)
            if not existing or existing.is_closed:
                self.active_connections[new_conn.peer_id] = new_conn
                return new_conn

            # Both existing and new_conn exist
            local_id = self.config.peer_id
            remote_id = new_conn.peer_id

            # Lower peer opens winning connection
            # If local_id < remote_id: local opened (outbound) connection is kept!
            # If remote_id < local_id: remote opened (inbound) connection is kept!
            if local_id < remote_id:
                winning_direction = "outbound"
            else:
                winning_direction = "inbound"

            if new_conn.direction == winning_direction:
                # new_conn wins, existing loses
                loser = existing
                winner = new_conn
            else:
                # existing wins, new_conn loses
                loser = new_conn
                winner = existing

            logger.info(
                f"[PeerServer] Duplicate connection with {remote_id} — keeping {winning_direction} (lower peer_id)"
            )

            # Send BYE to loser
            bye_msg = {
                "type": "BYE",
                "msg_id": str(uuid.uuid4()),
                "src": self.config.peer_id,
                "dst": remote_id,
                "reason": "duplicate",
                "ttl": 1,
            }
            asyncio.create_task(self._send_bye_and_close(loser, bye_msg))

            self.active_connections[winner.peer_id] = winner
            return winner

    async def _send_bye_and_close(self, conn: PeerConnection, bye_msg: Dict[str, Any]) -> None:
        try:
            await conn.send_json(bye_msg)
            await asyncio.sleep(0.1)
        except Exception:
            pass
        finally:
            await conn.close()

    async def _handle_conn_close(self, conn: PeerConnection, exception: Optional[Exception]) -> None:
        async with self._lock:
            current = self.active_connections.get(conn.peer_id)
            if current is conn:
                del self.active_connections[conn.peer_id]
        await self.on_peer_disconnected(conn.peer_id, exception)

    async def close_all(self, reason: str = "shutdown") -> None:
        async with self._lock:
            conns = list(self.active_connections.values())
            self.active_connections.clear()

        for conn in conns:
            bye = {
                "type": "BYE",
                "msg_id": str(uuid.uuid4()),
                "src": self.config.peer_id,
                "dst": conn.peer_id,
                "reason": reason,
                "ttl": 1,
            }
            try:
                await conn.send_json(bye)
            except Exception:
                pass
            await conn.close()
