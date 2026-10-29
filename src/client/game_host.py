from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Dict, List, Optional, Any, Callable, Awaitable, Set
from src.client.config import AppConfig
from src.client.models import HostGameStatus
from src.client.hangman import HangmanGame, normalize_word

logger = logging.getLogger("Host")


class GameHost:
    """
    State machine and coordinator for hosting a Hangman game.
    Authoritative state holder.
    """

    def __init__(
        self,
        config: AppConfig,
        send_to_peer: Callable[[str, Dict[str, Any]], Awaitable[bool]],
    ):
        self.config = config
        self.send_to_peer = send_to_peer

        self.status = HostGameStatus.IDLE
        self.game_id: Optional[str] = None
        self.game: Optional[HangmanGame] = None
        self.max_players: int = config.max_players
        self.players: List[str] = []
        self.all_registered_players: Set[str] = set()
        self.current_turn_index: int = 0
        self.seq: int = 0
        self.last_guess: Optional[Dict[str, Any]] = None

        self._turn_timer_task: Optional[asyncio.Task] = None
        self._finished_cleanup_task: Optional[asyncio.Task] = None

        # Callbacks for autonomous / bot mode
        self.on_player_joined: Optional[Callable[[str], Awaitable[None]]] = None
        self.on_player_rejoined: Optional[Callable[[str], Awaitable[None]]] = None
        self.on_game_finished: Optional[Callable[[str, Optional[str], Optional[str]], Awaitable[None]]] = None
        self.on_guess_activity: Optional[Callable[[], None]] = None
        self.on_no_players_remaining: Optional[Callable[[str], Awaitable[None]]] = None

    @property
    def current_turn_player(self) -> Optional[str]:
        if not self.players or self.status != HostGameStatus.RUNNING:
            return None
        return self.players[self.current_turn_index % len(self.players)]

    def create_game(self, secret_word: str, max_players: Optional[int] = None) -> str:
        """
        Creates a new game and transitions from IDLE to OPEN.
        """
        if self.status in (HostGameStatus.OPEN, HostGameStatus.RUNNING):
            raise ValueError(f"Cannot create game while game is {self.status.value}")

        self._cancel_timers()

        self.game = HangmanGame(secret_word, initial_lives=self.config.lives)
        suffix = uuid.uuid4().hex[:8]
        self.game_id = f"{self.config.peer_id}/{suffix}"
        self.max_players = max_players if max_players is not None else self.config.max_players
        self.players = []
        self.all_registered_players = set()
        self.status = HostGameStatus.OPEN
        self.seq = 0
        self.last_guess = None

        logger.info(
            f"[Host] Game {self.game_id} created (word_length={self.game.word_length}, lives={self.game.lives})"
        )
        return self.game_id

    async def start_game(self) -> None:
        """
        Starts the hosted game: transitions from OPEN to RUNNING, sends GAME_START and first STATE.
        """
        if self.status != HostGameStatus.OPEN:
            raise ValueError(f"Cannot start game: current status is {self.status.value}")

        if len(self.players) < 1:
            raise ValueError("Cannot start game: at least 1 player is required")

        self.status = HostGameStatus.RUNNING
        self.seq = 1
        self.current_turn_index = 0
        self.last_guess = None
        self.all_registered_players = set(self.players)

        # Broadcast GAME_START
        for player_id in list(self.players):
            start_msg = {
                "type": "GAME_START",
                "msg_id": str(uuid.uuid4()),
                "src": self.config.peer_id,
                "dst": player_id,
                "ttl": 1,
                "game_id": self.game_id,
                "players": list(self.players),
                "lives": self.game.lives,
                "word_length": self.game.word_length,
                "word_hash": self.game.word_hash,
                "turn_timeout": self.config.turn_timeout,
            }
            await self.send_to_peer(player_id, start_msg)

        # Broadcast initial STATE (seq=1)
        await self._broadcast_state()

        # Start turn timer
        self._reset_turn_timer()
        logger.info(f"[Host] Turn: {self.current_turn_player} (timeout {self.config.turn_timeout} s)")

    async def cancel_game(self, reason: str = "host_shutdown") -> None:
        """
        Cancels / aborts the hosted game.
        """
        if self.status == HostGameStatus.RUNNING:
            if self.game:
                self.game.abort(reason=reason)
            await self._broadcast_game_over(result="ABORTED", reason=reason)
        
        self._cancel_timers()
        self.status = HostGameStatus.IDLE
        logger.info(f"[Host] Game {self.game_id} cancelled (reason: {reason})")

    async def handle_game_list(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Responds to GAME_LIST queries.
        """
        req_msg_id = msg.get("msg_id", str(uuid.uuid4()))
        resp_games = []

        if self.status in (HostGameStatus.OPEN, HostGameStatus.RUNNING, HostGameStatus.FINISHED):
            resp_games.append({
                "game_id": self.game_id,
                "host": self.config.peer_id,
                "status": self.status.value,
                "players": list(self.players),
                "max_players": self.max_players,
                "lives": self.game.lives if self.game else self.config.lives,
                "word_length": self.game.word_length if self.game else 0,
            })

        resp = {
            "type": "GAME_LIST_OK",
            "msg_id": req_msg_id,
            "src": self.config.peer_id,
            "dst": conn.peer_id,
            "ttl": 1,
            "games": resp_games,
        }
        await conn.send_json(resp)

    async def handle_join(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Handles JOIN request from a player.
        """
        req_msg_id = msg.get("msg_id", str(uuid.uuid4()))
        src = msg.get("src", conn.peer_id)
        req_game_id = msg.get("game_id")

        if (req_game_id != self.game_id and req_game_id != self.config.peer_id) or self.status == HostGameStatus.IDLE:
            await self._send_join_err(conn, req_msg_id, req_game_id or "", "unknown_game")
            return

        if self.status == HostGameStatus.FINISHED:
            await self._send_join_err(conn, req_msg_id, self.game_id, "game_finished")
            return

        if self.status == HostGameStatus.OPEN:
            if src in self.players:
                await self._send_join_err(conn, req_msg_id, self.game_id, "already_joined")
                return
            if len(self.players) >= self.max_players:
                await self._send_join_err(conn, req_msg_id, self.game_id, "game_full")
                return

            self.players.append(src)
            self.all_registered_players.add(src)
            logger.info(f"[Host] {src} joined ({len(self.players)}/{self.max_players})")

            resp = {
                "type": "JOIN_OK",
                "msg_id": req_msg_id,
                "src": self.config.peer_id,
                "dst": src,
                "game_id": self.game_id,
                "players": list(self.players),
                "rejoin": False,
                "ttl": 1,
            }
            await conn.send_json(resp)
            if self.on_player_joined:
                await self.on_player_joined(src)
            return

        if self.status == HostGameStatus.RUNNING:
            if src in self.all_registered_players:
                # Rejoin allowed
                if src not in self.players:
                    self.players.append(src)
                logger.info(f"[Host] {src} rejoined running game")

                resp = {
                    "type": "JOIN_OK",
                    "msg_id": req_msg_id,
                    "src": self.config.peer_id,
                    "dst": src,
                    "game_id": self.game_id,
                    "players": list(self.players),
                    "rejoin": True,
                    "ttl": 1,
                }
                await conn.send_json(resp)
                # Follow up with current STATE
                await self._send_state_to_player(src)
                if self.on_player_rejoined:
                    await self.on_player_rejoined(src)
            else:
                await self._send_join_err(conn, req_msg_id, self.game_id, "game_running")

    async def _send_join_err(self, conn: Any, req_msg_id: str, game_id: str, reason: str) -> None:
        err = {
            "type": "JOIN_ERR",
            "msg_id": req_msg_id,
            "src": self.config.peer_id,
            "dst": conn.peer_id,
            "game_id": game_id,
            "reason": reason,
            "ttl": 1,
        }
        await conn.send_json(err)

    async def handle_leave(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Handles voluntary LEAVE from a player.
        """
        req_msg_id = msg.get("msg_id", str(uuid.uuid4()))
        src = msg.get("src", conn.peer_id)

        if src in self.players:
            is_turn = (self.current_turn_player == src)
            self.players.remove(src)
            logger.info(f"[Host] {src} left the game")

            resp = {
                "type": "LEAVE_OK",
                "msg_id": req_msg_id,
                "src": self.config.peer_id,
                "dst": src,
                "game_id": self.game_id or "",
                "ttl": 1,
            }
            await conn.send_json(resp)

            if self.status == HostGameStatus.RUNNING:
                if len(self.players) == 0:
                    if self.on_no_players_remaining:
                        await self.on_no_players_remaining(src)
                    else:
                        logger.info("[Host] No players remaining — aborting game")
                        if self.game:
                            self.game.abort(reason="no_players")
                        self._cancel_timers()
                        self.status = HostGameStatus.IDLE
                else:
                    if is_turn:
                        self.current_turn_index = self.current_turn_index % len(self.players)
                    self.seq += 1
                    await self._broadcast_state()
                    self._reset_turn_timer()

    async def handle_guess(self, conn: Any, msg: Dict[str, Any]) -> None:
        """
        Handles GUESS from player.
        """
        if self.on_guess_activity:
            self.on_guess_activity()

        req_msg_id = msg.get("msg_id", str(uuid.uuid4()))
        src = msg.get("src", conn.peer_id)
        letter = msg.get("letter")
        word = msg.get("word")

        if self.status != HostGameStatus.RUNNING or not self.game:
            await self._send_guess_result(conn, req_msg_id, False, None, "game_not_running")
            return

        if src != self.current_turn_player:
            await self._send_guess_result(conn, req_msg_id, False, None, "not_your_turn")
            return

        if (letter is None and word is None) or (letter is not None and word is not None):
            await self._send_guess_result(conn, req_msg_id, False, None, "invalid_letter")
            return

        if letter is not None:
            accepted, correct, reason = self.game.guess_letter(src, letter)
            await self._send_guess_result(conn, req_msg_id, accepted, correct, reason)
            if not accepted:
                return

            corr_str = "correct" if correct else "wrong"
            logger.info(f"[Host] GUESS {src} letter={letter} → {corr_str}, masked={self.game.masked}")
            self.last_guess = {"player": src, "letter": letter.lower(), "correct": correct}

        else:
            accepted, correct, reason = self.game.guess_word(src, word)
            await self._send_guess_result(conn, req_msg_id, accepted, correct, reason)
            if not accepted:
                return

            corr_str = "correct" if correct else "wrong"
            logger.info(f"[Host] GUESS {src} word={word} → {corr_str}, masked={self.game.masked}")
            self.last_guess = {"player": src, "word": word.lower(), "correct": correct}

        # Handle post-guess state
        if self.game.is_finished():
            self._cancel_timers()
            result = self.game.result or "LOST"
            winner = self.game.winner
            logger.info(f"[Host] Game {self.game_id} finished: {result} by {winner}")
            await self._broadcast_game_over(result=result, word=self.game.secret_word, winner=winner)
            if self.on_game_finished:
                await self.on_game_finished(result, self.game.secret_word, winner)
            else:
                self._transition_to_finished()
        else:
            # Advance turn
            self.current_turn_index = (self.current_turn_index + 1) % len(self.players)
            self.seq += 1
            await self._broadcast_state()
            self._reset_turn_timer()
            logger.info(f"[Host] Turn: {self.current_turn_player} (timeout {self.config.turn_timeout} s)")

    async def _send_guess_result(
        self,
        conn: Any,
        req_msg_id: str,
        accepted: bool,
        correct: Optional[bool],
        reason: Optional[str],
    ) -> None:
        payload: Dict[str, Any] = {
            "type": "GUESS_RESULT",
            "msg_id": req_msg_id,
            "src": self.config.peer_id,
            "dst": conn.peer_id,
            "game_id": self.game_id,
            "accepted": accepted,
            "ttl": 1,
        }
        if accepted:
            payload["correct"] = bool(correct)
        else:
            payload["reason"] = reason or "invalid_guess"

        await conn.send_json(payload)

    async def handle_player_disconnect(self, player_id: str) -> None:
        """
        Called when a player drops (connection closed or STALE).
        """
        if self.status == HostGameStatus.RUNNING and player_id in self.players:
            logger.warning(f"[Host] Player {player_id} STALE/disconnected — removed from game")
            is_turn = (self.current_turn_player == player_id)
            self.players.remove(player_id)

            if len(self.players) == 0:
                if self.on_no_players_remaining:
                    await self.on_no_players_remaining(player_id)
                else:
                    logger.info("[Host] No players remaining — aborting game")
                    if self.game:
                        self.game.abort(reason="no_players")
                    self._cancel_timers()
                    self.status = HostGameStatus.IDLE
            else:
                if is_turn:
                    self.current_turn_index = self.current_turn_index % len(self.players)
                self.seq += 1
                await self._broadcast_state()
                self._reset_turn_timer()
                logger.info(f"[Host] Turn: {self.current_turn_player} (timeout {self.config.turn_timeout} s)")

    async def _broadcast_state(self) -> None:
        for player_id in list(self.players):
            await self._send_state_to_player(player_id)

    async def _send_state_to_player(self, player_id: str) -> None:
        if not self.game:
            return
        state_msg = {
            "type": "STATE",
            "msg_id": str(uuid.uuid4()),
            "src": self.config.peer_id,
            "dst": player_id,
            "ttl": 1,
            "game_id": self.game_id,
            "seq": self.seq,
            "masked": self.game.masked,
            "wrong_letters": list(self.game.wrong_letters),
            "lives": self.game.lives,
            "turn": self.current_turn_player,
            "players": list(self.players),
            "last_guess": self.last_guess,
        }
        await self.send_to_peer(player_id, state_msg)

    async def _broadcast_game_over(
        self,
        result: str,
        word: Optional[str] = None,
        winner: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        scores = self.game.scores if self.game else {}
        secret = word if word else (self.game.secret_word if self.game else "")

        for player_id in list(self.players):
            msg: Dict[str, Any] = {
                "type": "GAME_OVER",
                "msg_id": str(uuid.uuid4()),
                "src": self.config.peer_id,
                "dst": player_id,
                "ttl": 1,
                "game_id": self.game_id,
                "result": result,
                "word": secret,
                "winner": winner,
                "scores": scores,
            }
            if reason:
                msg["reason"] = reason

            await self.send_to_peer(player_id, msg)

    def _reset_turn_timer(self) -> None:
        if self._turn_timer_task and not self._turn_timer_task.done():
            self._turn_timer_task.cancel()
        self._turn_timer_task = asyncio.create_task(self._turn_timeout_loop())

    async def _turn_timeout_loop(self) -> None:
        try:
            await asyncio.sleep(self.config.turn_timeout)
            if self.status == HostGameStatus.RUNNING and self.players:
                timed_out_player = self.current_turn_player
                logger.warning(f"[Host] Turn timeout for {timed_out_player} — skipping")
                self.last_guess = {"player": timed_out_player, "timeout": True}
                self.current_turn_index = (self.current_turn_index + 1) % len(self.players)
                self.seq += 1
                await self._broadcast_state()
                self._reset_turn_timer()
                logger.info(f"[Host] Turn: {self.current_turn_player} (timeout {self.config.turn_timeout} s)")
        except asyncio.CancelledError:
            pass

    def _transition_to_finished(self) -> None:
        self.status = HostGameStatus.FINISHED
        self._cancel_timers()
        self._finished_cleanup_task = asyncio.create_task(self._cleanup_after_finished())

    async def _cleanup_after_finished(self) -> None:
        try:
            # Stays FINISHED for 60s in GAME_LIST_OK, then IDLE
            await asyncio.sleep(60.0)
            if self.status == HostGameStatus.FINISHED:
                self.status = HostGameStatus.IDLE
                logger.debug("[Host] Finished game expired from lobby, status set to IDLE")
        except asyncio.CancelledError:
            pass

    def _cancel_timers(self) -> None:
        if self._turn_timer_task and not self._turn_timer_task.done():
            self._turn_timer_task.cancel()
            self._turn_timer_task = None
        if self._finished_cleanup_task and not self._finished_cleanup_task.done():
            self._finished_cleanup_task.cancel()
            self._finished_cleanup_task = None
