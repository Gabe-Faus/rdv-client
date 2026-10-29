from __future__ import annotations
import asyncio
import hashlib
import logging
import uuid
from typing import Dict, List, Optional, Any, Callable, Awaitable
from src.client.config import AppConfig
from src.client.models import PlayerGameStatus
from src.client.hangman import normalize_word

logger = logging.getLogger("Player")


class GamePlayer:
    """
    State machine for playing a game hosted by another peer.
    """

    def __init__(
        self,
        config: AppConfig,
        send_to_peer: Callable[[str, Dict[str, Any]], Awaitable[bool]],
    ):
        self.config = config
        self.send_to_peer = send_to_peer

        self.status = PlayerGameStatus.IDLE
        self.game_id: Optional[str] = None
        self.host_id: Optional[str] = None
        self.word_hash: Optional[str] = None
        self.word_length: int = 0
        self.turn_timeout: int = config.turn_timeout
        self.last_seq: int = 0

        # Current state
        self.masked: str = ""
        self.wrong_letters: List[str] = []
        self.lives: int = 0
        self.turn: Optional[str] = None
        self.players: List[str] = []
        self.last_guess: Optional[Dict[str, Any]] = None

        # Pending response futures: msg_id -> asyncio.Future
        self._pending_responses: Dict[str, asyncio.Future] = {}

    @property
    def is_my_turn(self) -> bool:
        return self.status == PlayerGameStatus.PLAYING and self.turn == self.config.peer_id

    async def join_game(self, host_peer_id: str, game_id: str) -> bool:
        """
        Sends JOIN to host and awaits JOIN_OK or JOIN_ERR.
        """
        if self.status != PlayerGameStatus.IDLE:
            raise ValueError(f"Cannot join game: currently in state {self.status.value}")

        mid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending_responses[mid] = fut

        join_msg = {
            "type": "JOIN",
            "msg_id": mid,
            "src": self.config.peer_id,
            "dst": host_peer_id,
            "game_id": game_id,
            "ttl": 1,
        }

        sent = await self.send_to_peer(host_peer_id, join_msg)
        if not sent:
            self._pending_responses.pop(mid, None)
            raise ConnectionError(f"Could not send JOIN to host {host_peer_id}")

        try:
            resp = await asyncio.wait_for(fut, timeout=5.0)
            if resp.get("type") == "JOIN_OK":
                self.game_id = game_id
                self.host_id = host_peer_id
                rejoin = resp.get("rejoin", False)
                if self.status != PlayerGameStatus.PLAYING:
                    self.status = PlayerGameStatus.PLAYING if rejoin else PlayerGameStatus.JOINED
                self.players = resp.get("players", [])
                rejoin_str = "rejoined" if rejoin else "joined"
                logger.info(f"[Player] Successfully {rejoin_str} game {game_id}")
                return True
            else:
                reason = resp.get("reason", "unknown_error")
                logger.warning(f"[Player] JOIN rejected by host: {reason}")
                raise ValueError(f"JOIN rejected: {reason}")
        finally:
            self._pending_responses.pop(mid, None)

    async def leave_game(self) -> bool:
        """
        Leaves current game voluntarily.
        """
        if self.status == PlayerGameStatus.IDLE or not self.host_id or not self.game_id:
            logger.info("[Player] Not in any game")
            return False

        mid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending_responses[mid] = fut

        leave_msg = {
            "type": "LEAVE",
            "msg_id": mid,
            "src": self.config.peer_id,
            "dst": self.host_id,
            "game_id": self.game_id,
            "ttl": 1,
        }

        await self.send_to_peer(self.host_id, leave_msg)
        try:
            await asyncio.wait_for(fut, timeout=3.0)
        except Exception:
            pass
        finally:
            self._pending_responses.pop(mid, None)
            self._reset()

        logger.info("[Player] Left game")
        return True

    async def guess_letter(self, letter: str) -> Dict[str, Any]:
        """
        Submits a letter guess to the host.
        """
        if self.status != PlayerGameStatus.PLAYING:
            raise ValueError("Not currently playing any game")

        if not self.is_my_turn:
            raise ValueError(f"It is not your turn (current turn: {self.turn})")

        letter_clean = letter.strip().lower()
        if len(letter_clean) != 1 or not ('a' <= letter_clean <= 'z'):
            raise ValueError("Letter guess must be a single letter (a-z)")

        mid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending_responses[mid] = fut

        guess_msg = {
            "type": "GUESS",
            "msg_id": mid,
            "src": self.config.peer_id,
            "dst": self.host_id,
            "game_id": self.game_id,
            "letter": letter_clean,
            "ttl": 1,
        }

        sent = await self.send_to_peer(self.host_id, guess_msg)
        if not sent:
            self._pending_responses.pop(mid, None)
            raise ConnectionError(f"Could not send GUESS to host {self.host_id}")

        try:
            resp = await asyncio.wait_for(fut, timeout=5.0)
            return resp
        finally:
            self._pending_responses.pop(mid, None)

    async def guess_word(self, word: str) -> Dict[str, Any]:
        """
        Submits a full word guess to the host.
        """
        if self.status != PlayerGameStatus.PLAYING:
            raise ValueError("Not currently playing any game")

        if not self.is_my_turn:
            raise ValueError(f"It is not your turn (current turn: {self.turn})")

        try:
            word_norm = normalize_word(word)
        except ValueError as e:
            raise ValueError(f"Invalid word guess: {e}")

        mid = str(uuid.uuid4())
        fut = asyncio.get_running_loop().create_future()
        self._pending_responses[mid] = fut

        guess_msg = {
            "type": "GUESS",
            "msg_id": mid,
            "src": self.config.peer_id,
            "dst": self.host_id,
            "game_id": self.game_id,
            "word": word_norm,
            "ttl": 1,
        }

        sent = await self.send_to_peer(self.host_id, guess_msg)
        if not sent:
            self._pending_responses.pop(mid, None)
            raise ConnectionError(f"Could not send GUESS to host {self.host_id}")

        try:
            resp = await asyncio.wait_for(fut, timeout=5.0)
            return resp
        finally:
            self._pending_responses.pop(mid, None)

    async def handle_game_start(self, conn: Any, msg: Dict[str, Any]) -> None:
        self.game_id = msg.get("game_id", self.game_id)
        self.players = msg.get("players", [])
        self.lives = msg.get("lives", 6)
        self.word_length = msg.get("word_length", 0)
        self.word_hash = msg.get("word_hash")
        self.turn_timeout = msg.get("turn_timeout", self.config.turn_timeout)
        self.status = PlayerGameStatus.PLAYING
        self.last_seq = 0
        logger.info(f"[Player] GAME_START received: word length={self.word_length}, lives={self.lives}")

    async def handle_state(self, conn: Any, msg: Dict[str, Any]) -> None:
        seq = msg.get("seq", 0)
        if seq <= self.last_seq:
            logger.debug(f"[Player] STATE seq={seq} ignored (already at seq={self.last_seq})")
            return

        self.last_seq = seq
        self.masked = msg.get("masked", "")
        self.wrong_letters = msg.get("wrong_letters", [])
        self.lives = msg.get("lives", 0)
        self.turn = msg.get("turn")
        self.players = msg.get("players", [])
        self.last_guess = msg.get("last_guess")

        turn_label = f"{self.turn} (você)" if self.turn == self.config.peer_id else self.turn
        logger.info(f"[Player] STATE seq={seq} applied: {self.masked} lives={self.lives} turn={turn_label}")

    async def handle_guess_result(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        if mid and mid in self._pending_responses:
            fut = self._pending_responses[mid]
            if not fut.done():
                fut.set_result(msg)

        accepted = msg.get("accepted", False)
        if accepted:
            correct = msg.get("correct", False)
            corr_str = "correct!" if correct else "wrong!"
            logger.info(f"[Player] Guess result: {corr_str}")
        else:
            reason = msg.get("reason", "unknown")
            logger.info(f"[Player] Guess rejected: {reason}")

    async def handle_join_ok(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        if mid and mid in self._pending_responses:
            fut = self._pending_responses[mid]
            if not fut.done():
                fut.set_result(msg)

    async def handle_join_err(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        if mid and mid in self._pending_responses:
            fut = self._pending_responses[mid]
            if not fut.done():
                fut.set_result(msg)

    async def handle_leave_ok(self, conn: Any, msg: Dict[str, Any]) -> None:
        mid = msg.get("msg_id")
        if mid and mid in self._pending_responses:
            fut = self._pending_responses[mid]
            if not fut.done():
                fut.set_result(msg)

    async def handle_game_over(self, conn: Any, msg: Dict[str, Any]) -> None:
        result = msg.get("result", "FINISHED")
        word = msg.get("word", "")
        winner = msg.get("winner")
        scores = msg.get("scores", {})

        # Verify word hash
        if word and self.word_hash:
            try:
                calc_hash = hashlib.sha256(normalize_word(word).encode("utf-8")).hexdigest()
                if calc_hash != self.word_hash:
                    logger.warning("[Player] host cheated: hash mismatch")
                else:
                    logger.info(f"[Player] GAME_OVER {result} word={word} winner={winner} — hash OK")
            except Exception:
                logger.warning("[Player] host cheated: hash verification failed")
        else:
            logger.info(f"[Player] GAME_OVER {result} word={word} winner={winner}")

        self._reset()

    def handle_host_disconnect(self, host_peer_id: str) -> None:
        if self.status in (PlayerGameStatus.JOINED, PlayerGameStatus.PLAYING) and self.host_id == host_peer_id:
            logger.warning(f"[Game] Host {host_peer_id} unreachable — game aborted")
            self._reset()

    def _reset(self) -> None:
        self.status = PlayerGameStatus.IDLE
        self.game_id = None
        self.host_id = None
        self.word_hash = None
        self.word_length = 0
        self.last_seq = 0
        self.masked = ""
        self.wrong_letters = []
        self.lives = 0
        self.turn = None
        self.players = []
        self.last_guess = None
        # Cancel any pending futures
        for fut in list(self._pending_responses.values()):
            if not fut.done():
                fut.cancel()
        self._pending_responses.clear()
