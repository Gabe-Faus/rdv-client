from __future__ import annotations
import asyncio
import logging
import shlex
import sys
from typing import Optional, List
from src.client.p2p_client import P2PForcaApp
from src.client.models import HostGameStatus, PlayerGameStatus

logger = logging.getLogger("CLI")


class CLI:
    """
    Interactive command-line interface for PyP2P-Forca.
    """

    def __init__(self, app: P2PForcaApp):
        self.app = app
        self._running = False

    async def run(self) -> None:
        self._running = True
        print(f"\nPyP2P-Forca CLI initialized for {self.app.config.peer_id}")
        print("Type /help for available commands or /quit to exit.\n")

        while self._running:
            try:
                # Read line asynchronously without blocking event loop
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:
                    break

                cmd_line = line.strip()
                if not cmd_line:
                    continue

                await self.execute_command(cmd_line)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[CLI] Error: {e}")

    async def execute_command(self, cmd_line: str) -> None:
        try:
            parts = shlex.split(cmd_line)
        except ValueError as e:
            print(f"[CLI] Syntax error: {e}")
            return

        if not parts:
            return

        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "/help":
            self._print_help()

        elif cmd == "/peers":
            await self._cmd_peers(args)

        elif cmd == "/conn":
            self._cmd_conn()

        elif cmd == "/rtt":
            self._cmd_rtt()

        elif cmd == "/reconnect":
            await self._cmd_reconnect()

        elif cmd == "/games":
            await self._cmd_games()

        elif cmd == "/create":
            await self._cmd_create(args)

        elif cmd == "/start":
            await self._cmd_start()

        elif cmd == "/cancel":
            await self._cmd_cancel()

        elif cmd == "/join":
            await self._cmd_join(args)

        elif cmd == "/leave":
            await self._cmd_leave()

        elif cmd == "/guess":
            await self._cmd_guess(args)

        elif cmd == "/word":
            await self._cmd_word(args)

        elif cmd == "/status":
            self._cmd_status()

        elif cmd == "/log":
            self._cmd_log(args)

        elif cmd in ("/quit", "/exit"):
            self._running = False
            await self.app.stop()

        else:
            print(f"[CLI] Unknown command '{cmd}'. Type /help for available commands.")

    def _print_help(self) -> None:
        help_text = """
Available commands:
  /peers [* | #namespace]          Discover and list peers (default: local namespace)
  /conn                            List active connections (inbound/outbound, state, RTT)
  /rtt                             Show average RTT per connected peer
  /reconnect                       Force immediate Rendezvous reconciliation
  /games                           List open games discovered on mesh
  /create <word> [max_players]     Host a new game with specified word
  /create --random [max_players]   Host a new game with random word from words.txt
  /start                           Start the hosted game (requires >= 1 player)
  /cancel                          Cancel the hosted game
  /join <host_peer_id>             Join a game hosted by the specified peer
  /leave                           Leave the current game
  /guess <letter>                  Guess a single letter (a-z)
  /word <word>                     Guess the whole word
  /status                          Display current game status
  /log <LEVEL>                     Set log level (DEBUG, INFO, WARNING, ERROR)
  /help                            Show this help message
  /quit                            Gracefully leave games, disconnect peers, and exit
"""
        print(help_text.strip())

    async def _cmd_peers(self, args: List[str]) -> None:
        target_ns: Optional[str] = None
        if args:
            arg = args[0]
            if arg == "*":
                target_ns = ""  # all namespaces
            elif arg.startswith("#"):
                target_ns = arg[1:]
            else:
                target_ns = arg

        try:
            discovered = await self.app.rdv.discover(namespace=target_ns)
            print(f"[CLI] Discovered {len(discovered)} peer(s):")
            for p in discovered:
                print(
                    f"  - {p.get('name')}@{p.get('namespace')} ({p.get('ip')}:{p.get('port')}) "
                    f"ttl={p.get('ttl')} expires_in={p.get('expires_in')}s"
                )
        except Exception as e:
            print(f"[CLI] Failed to discover peers: {e}")

    def _cmd_conn(self) -> None:
        peers = self.app.peer_table.all()
        connected = [p for p in peers if p.state.value == "CONNECTED"]
        print(f"[CLI] Active connections ({len(connected)}):")
        for p in peers:
            rtt_str = f"{p.avg_rtt_ms:.1f}ms" if p.avg_rtt_ms is not None else "N/A"
            dir_str = p.direction or "none"
            print(f"  {p.peer_id:<20} state={p.state.value:<11} dir={dir_str:<8} rtt={rtt_str}")

    def _cmd_rtt(self) -> None:
        connected = self.app.peer_table.connected_peers()
        print(f"[CLI] Average RTT for connected peers ({len(connected)}):")
        for p in connected:
            rtt_val = f"{p.avg_rtt_ms:.1f} ms" if p.avg_rtt_ms is not None else "N/A"
            samples = len(p.rtt_history)
            print(f"  {p.peer_id:<20} RTT avg: {rtt_val} ({samples} samples)")

    async def _cmd_reconnect(self) -> None:
        print("[CLI] Forcing RDV reconciliation...")
        await self.app.reconcile_with_rdv()
        print("[CLI] Reconciliation triggered.")

    async def _cmd_games(self) -> None:
        print("[CLI] Querying games from mesh...")
        games = await self.app.list_games()
        if not games:
            print("[CLI] No active games found.")
            return

        print("[CLI] Partidas abertas:")
        for g in games:
            players_str = f"{len(g.players)}/{g.max_players} jogadores"
            print(
                f"  {g.game_id:<25} host={g.host:<15} status={g.status:<8} "
                f"{players_str:<15} {g.word_length} letras  {g.lives} vidas"
            )

    async def _cmd_create(self, args: List[str]) -> None:
        if not args:
            print("[CLI] Usage: /create <palavra> [max_players] OR /create --random [max_players]")
            return

        max_players = int(args[1]) if len(args) > 1 and args[1].isdigit() else None
        try:
            if args[0] == "--random":
                gid = self.app.create_game_random(max_players)
            else:
                gid = self.app.create_game(args[0], max_players)
            print(f"[CLI] Created game: {gid}")
        except Exception as e:
            print(f"[CLI] Error creating game: {e}")

    async def _cmd_start(self) -> None:
        try:
            await self.app.start_game()
            print("[CLI] Game started!")
        except Exception as e:
            print(f"[CLI] Cannot start game: {e}")

    async def _cmd_cancel(self) -> None:
        try:
            await self.app.cancel_game()
            print("[CLI] Game cancelled.")
        except Exception as e:
            print(f"[CLI] Error cancelling game: {e}")

    async def _cmd_join(self, args: List[str]) -> None:
        if not args:
            print("[CLI] Usage: /join <host_peer_id>")
            return
        target = args[0]
        try:
            print(f"[CLI] Joining game hosted by {target}...")
            await self.app.join_game(target)
            print("[CLI] Joined successfully!")
        except Exception as e:
            print(f"[CLI] Error joining game: {e}")

    async def _cmd_leave(self) -> None:
        try:
            await self.app.leave_game()
            print("[CLI] Left game.")
        except Exception as e:
            print(f"[CLI] Error leaving game: {e}")

    async def _cmd_guess(self, args: List[str]) -> None:
        if not args:
            print("[CLI] Usage: /guess <letra>")
            return
        letter = args[0]
        try:
            res = await self.app.guess_letter(letter)
            if res.get("accepted"):
                corr = "ACERTOU!" if res.get("correct") else "ERROU!"
                print(f"[CLI] Palpite '{letter}': {corr}")
            else:
                print(f"[CLI] Palpite rejeitado: {res.get('reason')}")
        except Exception as e:
            print(f"[CLI] Error: {e}")

    async def _cmd_word(self, args: List[str]) -> None:
        if not args:
            print("[CLI] Usage: /word <palavra>")
            return
        word = args[0]
        try:
            res = await self.app.guess_word(word)
            if res.get("accepted"):
                corr = "ACERTOU A PALAVRA!" if res.get("correct") else "PALAVRA ERRADA!"
                print(f"[CLI] Palpite de palavra: {corr}")
            else:
                print(f"[CLI] Palpite rejeitado: {res.get('reason')}")
        except Exception as e:
            print(f"[CLI] Error: {e}")

    def _cmd_status(self) -> None:
        host = self.app.host
        player = self.app.player

        if host.status != HostGameStatus.IDLE:
            print(f"[Game] {host.game_id}  (host: {self.app.config.peer_id} [você])  status: {host.status.value}")
            if host.game:
                masked_spaced = " ".join(list(host.game.masked))
                wrong_str = ", ".join(host.game.wrong_letters) or "nenhuma"
                print(f"[Game] Palavra: {masked_spaced}     Vidas: {host.game.lives} / {host.game.initial_lives}     Erradas: {wrong_str}")
            players_str = ", ".join(host.players) or "nenhum"
            turn_str = host.current_turn_player or "N/A"
            print(f"[Game] Jogadores: {players_str}   Vez de: {turn_str}")
            return

        if player.status != PlayerGameStatus.IDLE:
            print(f"[Game] {player.game_id}  (host: {player.host_id})  status: {player.status.value}")
            masked_spaced = " ".join(list(player.masked)) if player.masked else "N/A"
            wrong_str = ", ".join(player.wrong_letters) or "nenhuma"
            print(f"[Game] Palavra: {masked_spaced}     Vidas: {player.lives}     Erradas: {wrong_str}")
            players_str = ", ".join(player.players) or "nenhum"
            turn_label = f"{player.turn} (você)" if player.is_my_turn else (player.turn or "N/A")
            print(f"[Game] Jogadores: {players_str}   Vez de: {turn_label}")
            return

        print("[CLI] No active game (status: IDLE). Type /create or /games to get started.")

    def _cmd_log(self, args: List[str]) -> None:
        if not args:
            print("[CLI] Usage: /log <DEBUG|INFO|WARNING|ERROR>")
            return
        lvl_str = args[0].upper()
        level = getattr(logging, lvl_str, None)
        if level is None:
            print(f"[CLI] Invalid log level '{lvl_str}'")
            return
        logging.getLogger().setLevel(level)
        print(f"[CLI] Log level set to {lvl_str}")
