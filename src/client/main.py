from __future__ import annotations
import argparse
import asyncio
import logging
from pathlib import Path
import sys

from src.client.config import AppConfig
from src.client.p2p_client import P2PForcaApp
from src.client.cli import CLI


def setup_logging(level_name: str, log_file: str | None = None) -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    level = getattr(logging, level_name.upper(), logging.INFO)
    root.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    if log_file:
        Path(log_file).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PyP2P-Forca: P2P Hangman Client with Rendezvous.")
    parser.add_argument("--config", default="config.json", help="Path to config.json file (default: config.json)")
    parser.add_argument("--name", help="Peer name identifier")
    parser.add_argument("--namespace", help="Namespace room identifier")
    parser.add_argument("--port", type=int, help="Local TCP listening port for P2P connections")
    parser.add_argument("--rdv-host", help="Rendezvous server host/IP")
    parser.add_argument("--rdv-port", type=int, help="Rendezvous server port")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Console log level")
    parser.add_argument("--log-file", help="Path to file for complete logging (includes DEBUG)")
    parser.add_argument("--autonomous", action="store_true", help="Run in autonomous bot mode (unattended)")
    parser.add_argument("--mode", choices=["normal", "autonomous"], help="Execution mode ('normal' or 'autonomous')")
    parser.add_argument("--idle-timeout", type=float, help="Autonomous idle timeout in seconds (default: 60)")
    parser.add_argument("--words-file", help="Path to words file (default: words.txt)")
    parser.add_argument("--auto-max-players", type=int, help="Max players for autonomous matches (default: 1)")
    return parser.parse_args()


async def main_async() -> None:
    args = parse_args()

    overrides = {}
    if args.name:
        overrides["name"] = args.name
    if args.namespace:
        overrides["namespace"] = args.namespace
    if args.port:
        overrides["listen_port"] = args.port
    if args.rdv_host:
        overrides["rdv_host"] = args.rdv_host
    if args.rdv_port:
        overrides["rdv_port"] = args.rdv_port
    if args.log_file:
        overrides["log_file"] = args.log_file
    if args.autonomous:
        overrides["autonomous"] = True
        overrides["mode"] = "autonomous"
    if args.mode:
        overrides["mode"] = args.mode
        overrides["autonomous"] = (args.mode.lower() == "autonomous")
    if args.idle_timeout is not None:
        overrides["idle_timeout"] = args.idle_timeout
    if args.words_file:
        overrides["words_file"] = args.words_file
    if args.auto_max_players is not None:
        overrides["auto_max_players"] = args.auto_max_players

    config = AppConfig.load(config_path=args.config, overrides=overrides)
    setup_logging(level_name=args.log_level, log_file=config.log_file)

    app = P2PForcaApp(config)

    if config.is_autonomous:
        logging.info("[Main] Starting client in AUTONOMOUS mode (unattended bot)...")
        try:
            await app.start()
            await app.run_autonomous()
        finally:
            await app.stop()
    else:
        cli = CLI(app)
        try:
            await app.start()
            await cli.run()
        finally:
            await app.stop()


def main() -> None:
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
