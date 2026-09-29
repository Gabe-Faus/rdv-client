from __future__ import annotations
import asyncio
import logging
import random
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from src.client.config import AppConfig
from src.client.models import PeerEntry, PeerState, GameLobbyInfo, HostGameStatus, PlayerGameStatus
from src.client.rendezvous_client import RendezvousClient, RendezvousError
from src.client.peer_table import PeerTable
from src.client.peer_connection import PeerConnection, ConnectionManager
from src.client.keep_alive import KeepAliveManager
from src.client.message_router import MessageRouter
from src.client.game_host import GameHost
from src.client.game_player import GamePlayer
from src.client.hangman import normalize_word

logger = logging.getLogger("P2PApp")


class P2PForcaApp:
    """
    Main P2P application orchestrator for PyP2P-Forca.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.peer_table = PeerTable(config)
        self.rdv = RendezvousClient(config)
        self.router = MessageRouter(config)

        self.conn_mgr = ConnectionManager(
            config=config,
            on_message=self._on_message_received,
            on_peer_connected=self._on_peer_connected,
            on_peer_disconnected=self._on_peer_disconnected,
        )

        self.keep_alive = KeepAliveManager(
            config=config,
            peer_table=self.peer_table,
            conn_mgr=self.conn_mgr,
            on_stale=self._on_peer_stale,
        )

        self.host = GameHost(config=config, send_to_peer=self.send_to_peer)
        self.player = GamePlayer(config=config, send_to_peer=self.send_to_peer)

        # Pending GAME_LIST queries: msg_id -> dict of responses
        self._pending_game_lists: Dict[str, Dict[str, Any]] = {}

        # Tasks
        self._discover_task: Optional[asyncio.Task] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._running = False

        self._setup_routes()

    def _setup_routes(self) -> None:
        # Session messages
        self.router.register("PING", self.keep_alive.handle_ping)
        self.router.register("PONG", self._handle_pong)
        self.router.register("BYE", self._handle_bye)
        self.router.register("BYE_OK", self._handle_bye_ok)
        self.router.register("ERROR", self._handle_error)

        # Lobby messages
        self.router.register("GAME_LIST", self.host.handle_game_list)
        self.router.register("GAME_LIST_OK", self._handle_game_list_ok)
        self.router.register("JOIN", self.host.handle_join)
        self.router.register("JOIN_OK", self.player.handle_join_ok)
        self.router.register("JOIN_ERR", self.player.handle_join_err)
        self.router.register("LEAVE", self.host.handle_leave)
        self.router.register("LEAVE_OK", self.player.handle_leave_ok)

        # Gameplay messages
        self.router.register("GAME_START", self.player.handle_game_start)
        self.router.register("STATE", self.player.handle_state)
        self.router.register("GUESS", self.host.handle_guess)
        self.router.register("GUESS_RESULT", self.player.handle_guess_result)
        self.router.register("GAME_OVER", self.player.handle_game_over)

    async def start(self) -> None:
        """
        Starts peer server, registers at Rendezvous, and launches background tasks.
        """
        self._running = True
        logger.info(f"Starting PyP2P-Forca client for {self.config.peer_id}...")

        # 1. Start TCP listening server
        await self.conn_mgr.start_server()

        # 2. Register at Rendezvous
        try:
            await self.rdv.register()
        except Exception as e:
            logger.error(f"Initial RDV registration failed: {e}")

        # 3. Start KeepAlive
        self.keep_alive.start()

        # 4. Launch background tasks
        self._discover_task = asyncio.create_task(self._discover_loop())
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

        # Perform initial discovery right away
        await self.reconcile_with_rdv()

    async def stop(self) -> None:
        """
        Graceful shutdown sequence:
        - If host in game: GAME_OVER ABORTED
        - If player in game: LEAVE
        - BYE to all peers
        - UNREGISTER from Rendezvous
        - Stop server and background tasks
        """
        if not self._running:
            return
        self._running = False
        logger.info("[P2PApp] Shutting down...")

        # 1. Stop background loops
        for t in (self._discover_task, self._refresh_task, self._reconnect_task):
            if t and not t.done():
                t.cancel()

        await self.keep_alive.stop()

        # 2. Abort or leave game if active
        if self.host.status == HostGameStatus.RUNNING:
            await self.host.cancel_game(reason="host_shutdown")
        if self.player.status in (PlayerGameStatus.JOINED, PlayerGameStatus.PLAYING):
            await self.player.leave_game()

        # 3. Disconnect all peers with BYE
        await self.conn_mgr.close_all(reason="shutdown")

        # 4. Stop TCP server
        await self.conn_mgr.stop_server()

        # 5. Unregister from RDV
        await self.rdv.unregister()
        logger.info("[P2PApp] Shutdown complete")

    async def send_to_peer(self, peer_id: str, payload: Dict[str, Any]) -> bool:
        conn = self.conn_mgr.get_connection(peer_id)
        if not conn or conn.is_closed:
            logger.debug(f"[P2PApp] Cannot send to {peer_id}: no active connection")
            return False
        return await conn.send_json(payload)

    # -------------------------------------------------------------
    # Event Handlers & Callbacks
    # -------------------------------------------------------------

    async def _on_message_received(self, conn: PeerConnection, msg: Dict[str, Any]) -> None:
        await self.router.route(conn, msg)

    async def _on_peer_connected(self, conn: PeerConnection) -> None:
        ip, port = "", 0
        try:
            peername = conn.writer.get_extra_info("peername")
            if peername:
                ip, port = peername[0], peername[1]
        except Exception:
            pass
        self.peer_table.mark_connected(conn.peer_id, conn.direction, conn.features, ip=ip, port=port)

    async def _on_peer_disconnected(self, peer_id: str, exception: Optional[Exception]) -> None:
        logger.debug(f"[P2PApp] Peer disconnected: {peer_id}")
        self.peer_table.mark_stale(peer_id)
        await self.host.handle_player_disconnect(peer_id)
        self.player.handle_host_disconnect(peer_id)

    async def _on_peer_stale(self, peer_id: str) -> None:
        await self.host.handle_player_disconnect(peer_id)
        self.player.handle_host_disconnect(peer_id)

    async def _handle_pong(self, conn: Any, msg: Dict[str, Any]) -> None:
        await self.keep_alive.handle_pong(conn.peer_id, msg)

    async def _handle_bye(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id", str(uuid.uuid4()))
        reason = msg.get("reason", "")
        logger.info(f"[PeerServer] BYE received from {conn.peer_id} (reason: {reason})")

        # Send BYE_OK
        bye_ok = {
            "type": "BYE_OK",
            "msg_id": mid,
            "src": self.config.peer_id,
            "dst": conn.peer_id,
            "ttl": 1,
        }
        try:
            await conn.send_json(bye_ok)
        except Exception:
            pass
        finally:
            await conn.close()

    async def _handle_bye_ok(self, conn: Any, msg: Dict[str, Any]) -> None:
        logger.debug(f"[PeerServer] BYE_OK received from {conn.peer_id}")

    async def _handle_error(self, conn: Any, msg: Dict[str, Any]) -> None:
        code = msg.get("code")
        error_msg = msg.get("message")
        ref_id = msg.get("ref_msg_id")
        logger.warning(f"[P2PApp] Received ERROR from {conn.peer_id}: code={code} ref={ref_id} ({error_msg})")

    async def _handle_game_list_ok(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        if mid and mid in self._pending_game_lists:
            games = msg.get("games", [])
            for g in games:
                self._pending_game_lists[mid][g.get("game_id")] = GameLobbyInfo.from_dict(g)

    # -------------------------------------------------------------
    # Periodic Loops & Reconciliation
    # -------------------------------------------------------------

    async def reconcile_with_rdv(self) -> None:
        """
        Queries RDV DISCOVER, updates peer table, opens connections to new peers,
        and disconnects gone peers.
        """
        try:
            discovered = await self.rdv.discover()
            to_connect, gone_ids = self.peer_table.reconcile(discovered)

            # Connect to new peers
            for entry in to_connect:
                asyncio.create_task(self._dial_peer(entry))

            # Disconnect gone peers
            for gid in gone_ids:
                conn = self.conn_mgr.get_connection(gid)
                if conn and not conn.is_closed:
                    bye = {
                        "type": "BYE",
                        "msg_id": str(uuid.uuid4()),
                        "src": self.config.peer_id,
                        "dst": gid,
                        "reason": "shutdown",
                        "ttl": 1,
                    }
                    await conn.send_json(bye)
                    await conn.close()
        except Exception as e:
            logger.warning(f"[P2PApp] RDV reconciliation failed: {e}")

    async def _dial_peer(self, entry: PeerEntry) -> None:
        self.peer_table.mark_connecting(entry.peer_id)
        await self.conn_mgr.connect_to_peer(entry.ip, entry.port, entry.peer_id)

    async def _discover_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.config.discover_interval)
                await self.reconcile_with_rdv()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[P2PApp] Error in discover loop: {e}")

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                # Refresh at half of TTL
                ttl = self.rdv.effective_ttl
                refresh_interval = max(60, ttl // 2)
                await asyncio.sleep(refresh_interval)
                await self.rdv.register()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[P2PApp] Error in register refresh loop: {e}")

    async def _reconnect_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(2.0)
                now = time.time()
                for peer in self.peer_table.all():
                    if peer.state == PeerState.STALE and now >= peer.next_reconnect_at:
                        peer.next_reconnect_at = now + 999999.0  # avoid duplicate trigger
                        asyncio.create_task(self._dial_peer(peer))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[P2PApp] Error in reconnect loop: {e}")

    # -------------------------------------------------------------
    # High-level API Methods (Used by CLI and Flask)
    # -------------------------------------------------------------

    async def list_games(self, timeout: float = 2.0) -> List[GameLobbyInfo]:
        """
        Sends GAME_LIST to all currently connected peers and consolidates results.
        """
        connected = self.peer_table.connected_peers()
        if not connected:
            # Also include local game if hosted
            if self.host.status != HostGameStatus.IDLE:
                return [
                    GameLobbyInfo(
                        game_id=self.host.game_id or "",
                        host=self.config.peer_id,
                        status=self.host.status.value,
                        players=list(self.host.players),
                        max_players=self.host.max_players,
                        lives=self.host.game.lives if self.host.game else self.config.lives,
                        word_length=self.host.game.word_length if self.host.game else 0,
                    )
                ]
            return []

        mid = str(uuid.uuid4())
        self._pending_game_lists[mid] = {}

        for p in connected:
            conn = self.conn_mgr.get_connection(p.peer_id)
            if conn and not conn.is_closed:
                req = {
                    "type": "GAME_LIST",
                    "msg_id": mid,
                    "src": self.config.peer_id,
                    "dst": p.peer_id,
                    "ttl": 1,
                }
                await conn.send_json(req)

        await asyncio.sleep(timeout)
        found_games = list(self._pending_game_lists.pop(mid, {}).values())

        # Include local game if present
        if self.host.status != HostGameStatus.IDLE and self.host.game_id:
            found_games.insert(
                0,
                GameLobbyInfo(
                    game_id=self.host.game_id,
                    host=self.config.peer_id,
                    status=self.host.status.value,
                    players=list(self.host.players),
                    max_players=self.host.max_players,
                    lives=self.host.game.lives if self.host.game else self.config.lives,
                    word_length=self.host.game.word_length if self.host.game else 0,
                ),
            )

        return found_games

    def create_game(self, word: str, max_players: Optional[int] = None) -> str:
        if self.player.status != PlayerGameStatus.IDLE:
            raise ValueError("Cannot create game while joined in another game as player")
        return self.host.create_game(word, max_players)

    def create_game_random(self, max_players: Optional[int] = None) -> str:
        if self.player.status != PlayerGameStatus.IDLE:
            raise ValueError("Cannot create game while joined in another game as player")

        words_file = Path(self.config.words_file)
        if not words_file.exists():
            raise FileNotFoundError(f"Words file not found: {self.config.words_file}")

        with open(words_file, "r", encoding="utf-8") as f:
            candidates = [line.strip() for line in f if line.strip()]

        if not candidates:
            raise ValueError("Words file is empty")

        selected = random.choice(candidates)
        return self.host.create_game(selected, max_players)

    async def start_game(self) -> None:
        await self.host.start_game()

    async def cancel_game(self) -> None:
        await self.host.cancel_game()

    async def join_game(self, host_peer_id_or_game_id: str) -> bool:
        if self.host.status in (HostGameStatus.OPEN, HostGameStatus.RUNNING):
            raise ValueError("Cannot join another game while hosting an active game")

        target = host_peer_id_or_game_id.strip()
        if "/" in target:
            # Full game_id provided, e.g. alice@CIC/3f9a2c1b
            host_peer_id = target.split("/")[0]
            game_id = target
        else:
            # Only host_peer_id provided, query games first to find game_id
            host_peer_id = target
            games = await self.list_games(timeout=1.5)
            matching = [g for g in games if g.host == host_peer_id and g.status in ("OPEN", "RUNNING")]
            if not matching:
                raise ValueError(f"No open game found hosted by {host_peer_id}")
            game_id = matching[0].game_id

        # Ensure we have a connection to the host
        if not self.conn_mgr.get_connection(host_peer_id):
            peer_entry = self.peer_table.get(host_peer_id)
            if not peer_entry:
                raise ValueError(f"Host {host_peer_id} is unknown (not discovered yet)")
            conn = await self.conn_mgr.connect_to_peer(peer_entry.ip, peer_entry.port, host_peer_id)
            if not conn:
                raise ConnectionError(f"Could not connect to host {host_peer_id}")

        return await self.player.join_game(host_peer_id, game_id)

    async def leave_game(self) -> bool:
        return await self.player.leave_game()

    async def guess_letter(self, letter: str) -> Dict[str, Any]:
        return await self.player.guess_letter(letter)

    async def guess_word(self, word: str) -> Dict[str, Any]:
        return await self.player.guess_word(word)

    def get_status_summary(self) -> Dict[str, Any]:
        """
        Returns full status dictionary suitable for CLI or Flask API.
        """
        return {
            "peer_id": self.config.peer_id,
            "public_ip": self.rdv.public_ip,
            "listen_port": self.config.listen_port,
            "namespace": self.config.namespace,
            "host_status": {
                "status": self.host.status.value,
                "game_id": self.host.game_id,
                "players": list(self.host.players),
                "turn": self.host.current_turn_player,
                "masked": self.host.game.masked if self.host.game else None,
                "lives": self.host.game.lives if self.host.game else None,
                "wrong_letters": self.host.game.wrong_letters if self.host.game else [],
                "scores": self.host.game.scores if self.host.game else {},
            },
            "player_status": {
                "status": self.player.status.value,
                "game_id": self.player.game_id,
                "host_id": self.player.host_id,
                "masked": self.player.masked,
                "lives": self.player.lives,
                "wrong_letters": self.player.wrong_letters,
                "turn": self.player.turn,
                "is_my_turn": self.player.is_my_turn,
                "players": self.player.players,
                "last_guess": self.player.last_guess,
            },
            "connections": [
                {
                    "peer_id": p.peer_id,
                    "ip": p.ip,
                    "port": p.port,
                    "state": p.state.value,
                    "direction": p.direction,
                    "avg_rtt_ms": p.avg_rtt_ms,
                }
                for p in self.peer_table.all()
            ],
        }
