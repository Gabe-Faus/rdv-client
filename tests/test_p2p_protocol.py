import asyncio
import unittest
from src.client.config import AppConfig
from src.client.p2p_client import P2PForcaApp
from src.client.models import HostGameStatus, PlayerGameStatus


class TestP2PProtocolLocal(unittest.IsolatedAsyncioTestCase):
    """
    Tests peer-to-peer gameplay and protocol between two local peers without requiring the external RDV server.
    """

    async def asyncSetUp(self):
        # Configure peer A (Host: Alice)
        self.config_a = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,  # dummy RDV
            namespace="TEST",
            name="alice",
            listen_port=18081,
            discover_interval=9999,
            ping_interval=9999,
            lives=6,
            turn_timeout=30,
        )
        self.app_a = P2PForcaApp(self.config_a)

        # Configure peer B (Player: Bob)
        self.config_b = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,
            namespace="TEST",
            name="bob",
            listen_port=18082,
            discover_interval=9999,
            ping_interval=9999,
            lives=6,
            turn_timeout=30,
        )
        self.app_b = P2PForcaApp(self.config_b)

        # Start servers directly
        await self.app_a.conn_mgr.start_server()
        await self.app_b.conn_mgr.start_server()

    async def asyncTearDown(self):
        await self.app_a.stop()
        await self.app_b.stop()

    async def test_handshake_and_gameplay_flow(self):
        # 1. Connect Bob -> Alice
        conn = await self.app_b.conn_mgr.connect_to_peer("127.0.0.1", self.config_a.listen_port, self.config_a.peer_id)
        self.assertIsNotNone(conn)
        self.assertEqual(conn.peer_id, "alice@TEST")

        # Give a moment for Alice to register inbound connection
        await asyncio.sleep(0.1)
        self.assertIn("bob@TEST", self.app_a.conn_mgr.active_connections)
        self.assertIn("alice@TEST", self.app_b.conn_mgr.active_connections)

        # 2. Alice creates a game
        game_id = self.app_a.create_game("banana", max_players=2)
        self.assertTrue(game_id.startswith("alice@TEST/"))
        self.assertEqual(self.app_a.host.status, HostGameStatus.OPEN)

        # 3. Bob queries GAME_LIST
        games = await self.app_b.list_games(timeout=0.2)
        self.assertEqual(len(games), 1)
        self.assertEqual(games[0].game_id, game_id)
        self.assertEqual(games[0].host, "alice@TEST")
        self.assertEqual(games[0].status, "OPEN")

        # 4. Bob joins the game
        joined = await self.app_b.join_game("alice@TEST")
        self.assertTrue(joined)
        self.assertEqual(self.app_b.player.status, PlayerGameStatus.JOINED)
        self.assertEqual(self.app_a.host.players, ["bob@TEST"])

        # 5. Alice starts the game
        await self.app_a.start_game()
        self.assertEqual(self.app_a.host.status, HostGameStatus.RUNNING)
        await asyncio.sleep(0.1)

        # Bob should now be PLAYING and have received STATE seq=1
        self.assertEqual(self.app_b.player.status, PlayerGameStatus.PLAYING)
        self.assertEqual(self.app_b.player.masked, "______")
        self.assertEqual(self.app_b.player.lives, 6)
        self.assertEqual(self.app_b.player.turn, "bob@TEST")
        self.assertTrue(self.app_b.player.is_my_turn)

        # 6. Bob makes a correct guess 'a'
        res = await self.app_b.guess_letter("a")
        self.assertTrue(res.get("accepted"))
        self.assertTrue(res.get("correct"))

        await asyncio.sleep(0.1)
        self.assertEqual(self.app_b.player.masked, "_a_a_a")
        self.assertEqual(self.app_a.host.game.masked, "_a_a_a")

        # 7. Bob guesses the full word 'banana'
        res_word = await self.app_b.guess_word("banana")
        self.assertTrue(res_word.get("accepted"))
        self.assertTrue(res_word.get("correct"))

        await asyncio.sleep(0.1)
        # Alice game should be finished WON
        self.assertEqual(self.app_a.host.game.result, "WON")
        self.assertEqual(self.app_a.host.game.winner, "bob@TEST")

        # Bob should have received GAME_OVER and returned to IDLE
        self.assertEqual(self.app_b.player.status, PlayerGameStatus.IDLE)

    async def test_duplicate_connection_tiebreaker(self):
        # Alice dials Bob AND Bob dials Alice simultaneously
        conn_a_to_b, conn_b_to_a = await asyncio.gather(
            self.app_a.conn_mgr.connect_to_peer("127.0.0.1", self.config_b.listen_port, self.config_b.peer_id),
            self.app_b.conn_mgr.connect_to_peer("127.0.0.1", self.config_a.listen_port, self.config_a.peer_id),
        )

        await asyncio.sleep(0.3)

        # Both should have exactly 1 active connection with each other
        conns_in_a = self.app_a.conn_mgr.active_connections
        conns_in_b = self.app_b.conn_mgr.active_connections

        self.assertEqual(len(conns_in_a), 1)
        self.assertEqual(len(conns_in_b), 1)

        # Lower peer_id is alice@TEST ('a' < 'b'), so connection opened by Alice (outbound on Alice, inbound on Bob) must win
        active_on_a = conns_in_a["bob@TEST"]
        active_on_b = conns_in_b["alice@TEST"]

        self.assertEqual(active_on_a.direction, "outbound")
        self.assertEqual(active_on_b.direction, "inbound")

    async def test_turn_order_and_out_of_turn_guess(self):
        # Configure peer C (Carol)
        config_c = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,
            namespace="TEST",
            name="carol",
            listen_port=18083,
            discover_interval=9999,
            ping_interval=9999,
        )
        app_c = P2PForcaApp(config_c)
        await app_c.conn_mgr.start_server()

        try:
            # Connect Bob -> Alice and Carol -> Alice
            await self.app_b.conn_mgr.connect_to_peer("127.0.0.1", self.config_a.listen_port, self.config_a.peer_id)
            await app_c.conn_mgr.connect_to_peer("127.0.0.1", self.config_a.listen_port, self.config_a.peer_id)
            await asyncio.sleep(0.1)

            # Alice creates game
            game_id = self.app_a.create_game("computador", max_players=3)

            # Bob joins first, Carol joins second
            await self.app_b.join_game("alice@TEST")
            await app_c.join_game("alice@TEST")

            self.assertEqual(self.app_a.host.players, ["bob@TEST", "carol@TEST"])

            # Start game
            await self.app_a.start_game()
            await asyncio.sleep(0.1)

            # Turn is Bob's turn
            self.assertTrue(self.app_b.player.is_my_turn)
            self.assertFalse(app_c.player.is_my_turn)

            # Carol attempts to guess on Bob's turn -> should fail with error
            with self.assertRaises(ValueError) as ctx:
                await app_c.guess_letter("c")
            self.assertIn("not your turn", str(ctx.exception).lower())

            # Bob guesses 'c' -> accepted and passes turn to Carol
            res_b = await self.app_b.guess_letter("c")
            self.assertTrue(res_b.get("accepted"))
            self.assertTrue(res_b.get("correct"))

            await asyncio.sleep(0.1)
            # Now turn should be Carol's
            self.assertFalse(self.app_b.player.is_my_turn)
            self.assertTrue(app_c.player.is_my_turn)
            self.assertEqual(app_c.player.masked[0], "c")

        finally:
            await app_c.stop()


class TestP2PService(unittest.TestCase):
    """
    Tests the synchronous P2PClientService wrapper designed for Flask routes.
    """

    def test_service_lifecycle_and_api(self):
        from src.client.service import P2PClientService

        cfg = AppConfig(
            rdv_host="127.0.0.1",
            rdv_port=9999,
            namespace="SVC_TEST",
            name="service_peer",
            listen_port=18089,
            discover_interval=9999,
            ping_interval=9999,
        )
        service = P2PClientService(cfg)
        service.start()

        try:
            status = service.get_status()
            self.assertEqual(status.get("peer_id"), "service_peer@SVC_TEST")
            self.assertEqual(status.get("host_status", {}).get("status"), "IDLE")

            # Test creating a game via service
            gid = service.create_game("abacaxi", max_players=2)
            self.assertTrue(gid.startswith("service_peer@SVC_TEST/"))

            status_after = service.get_status()
            self.assertEqual(status_after.get("host_status", {}).get("status"), "OPEN")

            # Cancel game
            service.cancel_game()
            status_cancelled = service.get_status()
            self.assertEqual(status_cancelled.get("host_status", {}).get("status"), "IDLE")

        finally:
            service.stop()


if __name__ == "__main__":
    unittest.main()
