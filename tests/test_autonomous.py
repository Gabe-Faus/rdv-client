from __future__ import annotations
import asyncio
import tempfile
import unittest
from pathlib import Path

from src.client.config import AppConfig
from src.client.p2p_client import P2PForcaApp
from src.client.models import HostGameStatus, PlayerGameStatus
from src.client.autonomous import AutonomousController


class TestAutonomousUnit(unittest.TestCase):
    """
    Unit tests for AutonomousController word management and round-robin logic.
    """

    def test_word_list_loading_and_filtering(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as f:
            f.write("banana\n")
            f.write("Maçã\n")  # Should normalize to maca
            f.write("sub-rede\n")  # Invalid (hyphen) -> should filter
            f.write("ip\n")  # Invalid (too short) -> should filter
            f.write("computador\n")
            f.write("   \n")
            temp_path = f.name

        try:
            cfg = AppConfig(words_file=temp_path, autonomous=True)
            app = P2PForcaApp(cfg)
            bot = AutonomousController(app)
            words = bot.load_words()

            self.assertEqual(words, ["banana", "maca", "computador"])
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_round_robin_cycling(self):
        with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False) as f:
            f.write("primeira\nsegunda\nterceira\n")
            temp_path = f.name

        try:
            cfg = AppConfig(words_file=temp_path, autonomous=True)
            app = P2PForcaApp(cfg)
            bot = AutonomousController(app)
            bot.load_words()

            self.assertEqual(bot.get_next_word(), "primeira")
            self.assertEqual(bot.get_next_word(), "segunda")
            self.assertEqual(bot.get_next_word(), "terceira")
            # Round-robin wrap-around:
            self.assertEqual(bot.get_next_word(), "primeira")
            self.assertEqual(bot.get_next_word(), "segunda")
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestAutonomousIntegration(unittest.IsolatedAsyncioTestCase):
    """
    Integration tests simulating autonomous bot and student client interaction.
    """

    async def asyncSetUp(self):
        # Create temp words file with 3 words
        self.temp_words = tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=False)
        self.temp_words.write("banana\nabacaxi\ncomputador\n")
        self.temp_words.flush()
        self.words_file = self.temp_words.name
        self.temp_words.close()

        # Bot peer (Alice)
        self.config_bot = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,
            namespace="AUTO_TEST",
            name="bot_alice",
            listen_port=19081,
            discover_interval=9999,
            ping_interval=9999,
            words_file=self.words_file,
            autonomous=True,
            idle_timeout=1.0,
            reset_delay=0.2,
            auto_max_players=1,
            lives=6,
        )
        self.app_bot = P2PForcaApp(self.config_bot)

        # Student peer (Bob)
        self.config_student = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,
            namespace="AUTO_TEST",
            name="student_bob",
            listen_port=19082,
            discover_interval=9999,
            ping_interval=9999,
            autonomous=False,
            lives=6,
        )
        self.app_student = P2PForcaApp(self.config_student)

        # Start servers and bot
        await self.app_bot.conn_mgr.start_server()
        await self.app_student.conn_mgr.start_server()
        await self.app_bot.autonomous.start()

    async def asyncTearDown(self):
        await self.app_bot.stop()
        await self.app_student.stop()
        Path(self.words_file).unlink(missing_ok=True)

    async def test_match_lifecycle_win_and_round_robin_advancement(self):
        # 1. On start, bot should have prepared a match with the first word ("banana")
        self.assertEqual(self.app_bot.host.status, HostGameStatus.OPEN)
        self.assertEqual(self.app_bot.host.game.secret_word, "banana")
        self.assertEqual(self.app_bot.host.game.word_length, 6)
        first_game_id = self.app_bot.host.game_id

        # 2. Student connects to Bot
        await self.app_student.conn_mgr.connect_to_peer(
            "127.0.0.1", self.config_bot.listen_port, self.config_bot.peer_id
        )
        await asyncio.sleep(0.1)

        # 3. Student joins game -> Bot should automatically start the game!
        joined = await self.app_student.join_game("bot_alice@AUTO_TEST")
        self.assertTrue(joined)

        await asyncio.sleep(0.1)
        self.assertEqual(self.app_bot.host.status, HostGameStatus.RUNNING)
        self.assertEqual(self.app_student.player.status, PlayerGameStatus.PLAYING)
        self.assertEqual(self.app_student.player.masked, "______")
        self.assertTrue(self.app_student.player.is_my_turn)

        # 4. Student plays and wins the match
        res_word = await self.app_student.guess_word("banana")
        self.assertTrue(res_word.get("accepted"))
        self.assertTrue(res_word.get("correct"))

        await asyncio.sleep(0.1)
        self.assertEqual(self.app_bot.host.game.result, "WON")
        self.assertEqual(self.app_student.player.status, PlayerGameStatus.IDLE)

        # 5. After reset_delay (0.2s), bot returns to initial state with the NEXT word ("abacaxi")
        await asyncio.sleep(0.3)
        self.assertEqual(self.app_bot.host.status, HostGameStatus.OPEN)
        self.assertNotEqual(self.app_bot.host.game_id, first_game_id)
        self.assertEqual(self.app_bot.host.game.secret_word, "abacaxi")
        self.assertEqual(self.app_bot.host.game.word_length, 7)

        # 6. Student can immediately join and play the new match!
        second_joined = await self.app_student.join_game("bot_alice@AUTO_TEST")
        self.assertTrue(second_joined)

        await asyncio.sleep(0.1)
        self.assertEqual(self.app_bot.host.status, HostGameStatus.RUNNING)
        self.assertEqual(self.app_student.player.status, PlayerGameStatus.PLAYING)
        self.assertEqual(self.app_student.player.masked, "_______")

    async def test_match_idle_inactivity_timeout_resets_and_advances_word(self):
        # 1. Connect and join first match ("banana")
        await self.app_student.conn_mgr.connect_to_peer(
            "127.0.0.1", self.config_bot.listen_port, self.config_bot.peer_id
        )
        await asyncio.sleep(0.1)
        await self.app_student.join_game("bot_alice@AUTO_TEST")
        await asyncio.sleep(0.1)
        self.assertEqual(self.app_bot.host.status, HostGameStatus.RUNNING)
        self.assertEqual(self.app_bot.host.game.secret_word, "banana")

        # 2. Student stops playing (idle_timeout is 1.0s)
        # Wait 1.3s for inactivity timeout to trigger
        await asyncio.sleep(1.3)

        # 3. Match should have been reset and advanced to word 2 ("abacaxi")
        self.assertEqual(self.app_bot.host.status, HostGameStatus.OPEN)
        self.assertEqual(self.app_bot.host.game.secret_word, "abacaxi")

    async def test_match_disconnect_timeout_resets_and_advances_word(self):
        # 1. Connect and join first match ("banana")
        await self.app_student.conn_mgr.connect_to_peer(
            "127.0.0.1", self.config_bot.listen_port, self.config_bot.peer_id
        )
        await asyncio.sleep(0.1)
        await self.app_student.join_game("bot_alice@AUTO_TEST")
        await asyncio.sleep(0.1)
        self.assertEqual(self.app_bot.host.status, HostGameStatus.RUNNING)

        # 2. Student disconnects abruptly (closes connection)
        student_conn = self.app_student.conn_mgr.get_connection("bot_alice@AUTO_TEST")
        self.assertIsNotNone(student_conn)
        await student_conn.close()

        # Wait a moment for bot to register disconnect
        await asyncio.sleep(0.2)

        # Disconnect timeout is 1.0s. Wait for it to expire
        await asyncio.sleep(1.2)

        # 3. Match should have reset and prepared next match with word 2 ("abacaxi")
        self.assertEqual(self.app_bot.host.status, HostGameStatus.OPEN)
        self.assertEqual(self.app_bot.host.game.secret_word, "abacaxi")


if __name__ == "__main__":
    unittest.main()
