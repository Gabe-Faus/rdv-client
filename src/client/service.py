from __future__ import annotations
import asyncio
import threading
from typing import Dict, List, Optional, Any, Callable
from src.client.config import AppConfig
from src.client.p2p_client import P2PForcaApp
from src.client.models import GameLobbyInfo


class P2PClientService:
    """
    Thread-safe service layer designed for seamless integration with Flask or CLI.
    Runs the asyncio event loop in a dedicated background thread, exposing simple
    synchronous methods using asyncio.run_coroutine_threadsafe.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.app: Optional[P2PForcaApp] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._started_event = threading.Event()
        self._listeners: List[Callable[[Dict[str, Any]], None]] = []

    def start(self) -> None:
        """
        Starts the background asyncio event loop and initializes P2PForcaApp.
        """
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="P2P-EventLoop")
        self._thread.start()
        self._started_event.wait(timeout=10.0)

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self.app = P2PForcaApp(self.config)

        async def _init():
            try:
                await self.app.start()
            finally:
                self._started_event.set()

        self._loop.create_task(_init())
        try:
            self._loop.run_forever()
        finally:
            try:
                # Cancel all remaining tasks
                pending = asyncio.all_tasks(self._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            self._loop.close()

    def stop(self, timeout: float = 5.0) -> None:
        """
        Stops the P2P application cleanly.
        """
        if not self._loop or not self._loop.is_running():
            return

        if self.app:
            future = asyncio.run_coroutine_threadsafe(self.app.stop(), self._loop)
            try:
                future.result(timeout=timeout)
            except Exception:
                pass

        self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=timeout)

    def _run_sync(self, coro, timeout: float = 10.0):
        if not self._loop or not self._loop.is_running():
            raise RuntimeError("P2P service is not running")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    # -------------------------------------------------------------
    # Synchronous thread-safe API for Flask & CLI
    # -------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        if not self.app:
            return {}
        return self.app.get_status_summary()

    def get_peers(self) -> List[Dict[str, Any]]:
        if not self.app:
            return []
        return [
            {
                "peer_id": p.peer_id,
                "name": p.name,
                "namespace": p.namespace,
                "ip": p.ip,
                "port": p.port,
                "state": p.state.value,
                "avg_rtt_ms": p.avg_rtt_ms,
            }
            for p in self.app.peer_table.all()
        ]

    def get_connections(self) -> List[Dict[str, Any]]:
        if not self.app:
            return []
        return [
            {
                "peer_id": p.peer_id,
                "ip": p.ip,
                "port": p.port,
                "state": p.state.value,
                "direction": p.direction,
                "avg_rtt_ms": p.avg_rtt_ms,
                "features": p.features,
            }
            for p in self.app.peer_table.connected_peers()
        ]

    def list_games(self, timeout: float = 2.0) -> List[Dict[str, Any]]:
        if not self.app:
            return []
        games: List[GameLobbyInfo] = self._run_sync(self.app.list_games(timeout=timeout), timeout=timeout + 2.0)
        return [g.to_dict() for g in games]

    def create_game(self, word: str, max_players: Optional[int] = None) -> str:
        if not self.app:
            raise RuntimeError("App not initialized")
        # create_game is synchronous within app
        return self.app.create_game(word, max_players)

    def create_game_random(self, max_players: Optional[int] = None) -> str:
        if not self.app:
            raise RuntimeError("App not initialized")
        return self.app.create_game_random(max_players)

    def start_game(self) -> None:
        if not self.app:
            raise RuntimeError("App not initialized")
        self._run_sync(self.app.start_game())

    def cancel_game(self) -> None:
        if not self.app:
            raise RuntimeError("App not initialized")
        self._run_sync(self.app.cancel_game())

    def join_game(self, host_peer_id_or_game_id: str) -> bool:
        if not self.app:
            raise RuntimeError("App not initialized")
        return self._run_sync(self.app.join_game(host_peer_id_or_game_id))

    def leave_game(self) -> bool:
        if not self.app:
            raise RuntimeError("App not initialized")
        return self._run_sync(self.app.leave_game())

    def guess_letter(self, letter: str) -> Dict[str, Any]:
        if not self.app:
            raise RuntimeError("App not initialized")
        return self._run_sync(self.app.guess_letter(letter))

    def guess_word(self, word: str) -> Dict[str, Any]:
        if not self.app:
            raise RuntimeError("App not initialized")
        return self._run_sync(self.app.guess_word(word))

    def reconnect(self) -> None:
        if not self.app:
            raise RuntimeError("App not initialized")
        self._run_sync(self.app.reconcile_with_rdv())
