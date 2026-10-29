from __future__ import annotations
import asyncio
import logging
import time
from pathlib import Path
from typing import List, Optional, Any, TYPE_CHECKING

from src.client.models import HostGameStatus
from src.client.hangman import normalize_word

if TYPE_CHECKING:
    from src.client.p2p_client import P2PForcaApp

logger = logging.getLogger("Autonomous")


class AutonomousController:
    """
    Orchestrates unattended, autonomous bot gameplay for PyP2P-Forca.

    Lifecycle:
        1. Loads valid words from words_file.
        2. Prepares a match with the current round-robin word and waits for a player.
        3. Starts the match automatically when a player joins.
        4. Monitors match progress:
           - On game finish (win/loss): resets match and advances to next word.
           - On idle timeout (inactivity or player disconnect): cancels match and advances to next word.
        5. Runs continuously without manual interaction.
    """

    def __init__(self, app: P2PForcaApp):
        self.app = app
        self.config = app.config
        self.words: List[str] = []
        self.word_index: int = 0

        self._running: bool = False
        self._supervisor_task: Optional[asyncio.Task] = None
        self._delayed_next_task: Optional[asyncio.Task] = None
        self._stop_event: asyncio.Event = asyncio.Event()

        self._last_activity_time: float = 0.0
        self._disconnect_time: Optional[float] = None
        self._current_word: Optional[str] = None
        self._resetting: bool = False

    def load_words(self) -> List[str]:
        """
        Reads words from the configured words file, normalizing and validating each word.
        Invalid words (e.g., non-alphabetical or out-of-length) are filtered out.
        """
        words_path = Path(self.config.words_file)
        if not words_path.exists():
            raise FileNotFoundError(f"Autonomous mode words file not found: {self.config.words_file}")

        valid_words: List[str] = []
        with open(words_path, "r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                raw = raw_line.strip()
                if not raw:
                    continue
                try:
                    norm = normalize_word(raw)
                    valid_words.append(norm)
                except ValueError as e:
                    logger.debug(f"[Autonomous] Line {line_no} '{raw}' skipped: {e}")

        if not valid_words:
            raise ValueError(f"No valid words found in {self.config.words_file}")

        self.words = valid_words
        self.word_index = 0
        logger.info(f"[Autonomous] Loaded {len(self.words)} valid words from {self.config.words_file}")
        return self.words

    def get_next_word(self) -> str:
        """
        Pulls the next word in round-robin sequence.
        """
        if not self.words:
            self.load_words()

        word = self.words[self.word_index]
        self.word_index = (self.word_index + 1) % len(self.words)
        return word

    async def start(self) -> None:
        """
        Initializes autonomous controller, attaches host callbacks, and prepares first match.
        """
        if self._running:
            return

        self._running = True
        self._stop_event.clear()

        # Load words list
        if not self.words:
            self.load_words()

        # Wire callbacks into GameHost
        self.app.host.on_player_joined = self.on_player_joined
        self.app.host.on_player_rejoined = self.on_player_rejoined
        self.app.host.on_game_finished = self.on_game_finished
        self.app.host.on_guess_activity = self.on_guess_activity
        self.app.host.on_no_players_remaining = self.on_no_players_remaining

        logger.info(
            f"[Autonomous] Starting autonomous bot mode for peer {self.config.peer_id} "
            f"(idle_timeout={self.config.idle_timeout}s, max_players={self.config.auto_max_players})"
        )

        # Prepare first game
        self.prepare_next_match()

        # Launch periodic supervisor loop
        self._supervisor_task = asyncio.create_task(self._supervisor_loop())

    async def stop(self) -> None:
        """
        Shuts down autonomous controller and cancels background tasks.
        """
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._delayed_next_task and not self._delayed_next_task.done():
            self._delayed_next_task.cancel()
            self._delayed_next_task = None

        if self._supervisor_task and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            self._supervisor_task = None

        logger.info("[Autonomous] Autonomous mode stopped")

    async def wait_until_stopped(self) -> None:
        """
        Awaits until stop() is called or process interrupted.
        """
        await self._stop_event.wait()

    def prepare_next_match(self) -> Optional[str]:
        """
        Transitions to a fresh match with the next word drawn in round-robin order.
        Host is set to OPEN and waits for a player to connect.
        """
        if not self._running:
            return None

        # Cancel any delayed next task
        if self._delayed_next_task and not self._delayed_next_task.done():
            self._delayed_next_task.cancel()
            self._delayed_next_task = None

        # Reset host timers and status if not IDLE
        self.app.host._cancel_timers()
        self.app.host.status = HostGameStatus.IDLE

        # Get next word in round-robin order
        word = self.get_next_word()
        self._current_word = word
        self._disconnect_time = None
        self._last_activity_time = time.time()

        game_id = self.app.host.create_game(word, max_players=self.config.auto_max_players)
        current_idx = (self.word_index - 1) % len(self.words)
        logger.info(
            f"[Autonomous] Match ready [Word #{current_idx + 1}/{len(self.words)}, "
            f"length={len(word)}, lives={self.app.host.game.lives}]. "
            f"Game ID: {game_id}. Waiting for player to connect..."
        )
        return game_id

    async def on_player_joined(self, player_id: str) -> None:
        """
        Callback when a player joins the OPEN match.
        Starts the game immediately so the player can test their implementation without delay.
        """
        if self.app.host.status == HostGameStatus.OPEN:
            logger.info(f"[Autonomous] Player {player_id} joined. Auto-starting match...")
            self._last_activity_time = time.time()
            self._disconnect_time = None
            # Allow a brief moment for the joining client to finish processing JOIN_OK
            await asyncio.sleep(0.05)
            try:
                if self.app.host.status == HostGameStatus.OPEN:
                    await self.app.host.start_game()
                    logger.info(f"[Autonomous] Match started against {player_id}! Waiting for guesses...")
            except Exception as e:
                logger.error(f"[Autonomous] Failed to start game on player join: {e}")

    async def on_player_rejoined(self, player_id: str) -> None:
        """
        Callback when a player successfully reconnects and rejoins the running match.
        """
        logger.info(f"[Autonomous] Player {player_id} rejoined running match. Resuming activity.")
        self._last_activity_time = time.time()
        self._disconnect_time = None

    def on_guess_activity(self) -> None:
        """
        Callback when a guess is made (keeps match active).
        """
        self._last_activity_time = time.time()

    async def on_game_finished(self, result: str, secret_word: Optional[str], winner: Optional[str]) -> None:
        """
        Callback when a match concludes (WON or LOST).
        Logs results and schedules the next round-robin match.
        """
        logger.info(
            f"[Autonomous] Match ended: {result}! Secret word was '{secret_word}'. Winner: {winner or 'None'}. "
            f"Resetting and preparing next match in {self.config.reset_delay:.1f}s..."
        )
        self.app.host.status = HostGameStatus.FINISHED
        self.app.host._cancel_timers()

        # Schedule reset and preparation of next match
        if self._delayed_next_task and not self._delayed_next_task.done():
            self._delayed_next_task.cancel()
        self._delayed_next_task = asyncio.create_task(self._delayed_next_match())

    async def on_no_players_remaining(self, player_id: str) -> None:
        """
        Callback when all players leave or disconnect from a running match.
        Starts the idle countdown to give the player time to reconnect before resetting.
        """
        logger.warning(
            f"[Autonomous] Player {player_id} left/disconnected. No active players in match. "
            f"Waiting up to {self.config.idle_timeout:.1f}s for reconnect before resetting..."
        )
        self._disconnect_time = time.time()

    async def _delayed_next_match(self) -> None:
        """
        Waits reset_delay then creates the next match.
        """
        try:
            await asyncio.sleep(self.config.reset_delay)
            if self._running:
                logger.info("[Autonomous] Returning to initial state with next word...")
                self.prepare_next_match()
        except asyncio.CancelledError:
            pass

    async def reset_idle_match(self, reason: str) -> None:
        """
        Cancels the current match due to inactivity or disconnection,
        and prepares a new match with the next word in the list.
        """
        if self._resetting:
            return
        self._resetting = True
        try:
            logger.warning(f"[Autonomous] Resetting idle match (reason: {reason})...")
            if self.app.host.status == HostGameStatus.RUNNING:
                await self.app.host.cancel_game(reason=reason)
            else:
                self.app.host._cancel_timers()
                self.app.host.status = HostGameStatus.IDLE

            if self._running:
                self.prepare_next_match()
        except Exception as e:
            logger.error(f"[Autonomous] Error cancelling idle game: {e}")
        finally:
            self._resetting = False

    async def _supervisor_loop(self) -> None:
        """
        Periodically checks for idle match conditions:
        - Players stopped playing (no guesses for idle_timeout)
        - All players disconnected (no player for idle_timeout)
        - Unexpected IDLE state while running
        """
        while self._running:
            try:
                await asyncio.sleep(0.1)

                if self.app.host.status == HostGameStatus.RUNNING:
                    # 1. Check if all players disconnected
                    if len(self.app.host.players) == 0:
                        if self._disconnect_time is None:
                            self._disconnect_time = time.time()
                        elapsed_disc = time.time() - self._disconnect_time
                        if elapsed_disc >= self.config.idle_timeout:
                            logger.warning(
                                f"[Autonomous] Disconnect timeout reached ({elapsed_disc:.1f}s >= {self.config.idle_timeout}s with no player). "
                                f"Resetting match..."
                            )
                            await self.reset_idle_match(reason="disconnect_timeout")
                            continue

                    # 2. Check for player inactivity (stopped playing)
                    else:
                        elapsed_act = time.time() - self._last_activity_time
                        if elapsed_act >= self.config.idle_timeout:
                            logger.warning(
                                f"[Autonomous] Inactivity idle timeout reached ({elapsed_act:.1f}s >= {self.config.idle_timeout}s without guesses). "
                                f"Resetting match..."
                            )
                            await self.reset_idle_match(reason="idle_inactivity")
                            continue

                elif self.app.host.status == HostGameStatus.IDLE:
                    # In autonomous mode, we should always have an OPEN match waiting for players
                    # If it became IDLE without a pending delayed reset, prepare next match right away
                    if not self._resetting and not (self._delayed_next_task and not self._delayed_next_task.done()):
                        logger.info("[Autonomous] Host is IDLE. Preparing next match...")
                        self.prepare_next_match()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Autonomous] Error in supervisor loop: {e}", exc_info=True)
