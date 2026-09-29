from __future__ import annotations
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Any


@dataclass
class AppConfig:
    rdv_host: str = "pyp2p.mfcaetano.cc"
    rdv_port: int = 8080
    namespace: str = "CIC"
    name: str = "alice"
    listen_port: int = 8081
    register_ttl: int = 7200
    discover_interval: int = 20
    ping_interval: int = 30
    ping_miss_limit: int = 3
    max_reconnect_attempts: int = 5
    base_backoff: float = 2.0
    max_backoff: float = 60.0
    lives: int = 6
    max_players: int = 4
    turn_timeout: int = 30
    words_file: str = "words.txt"
    log_file: Optional[str] = None

    @property
    def peer_id(self) -> str:
        return f"{self.name}@{self.namespace}"

    @classmethod
    def load(cls, config_path: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None) -> AppConfig:
        data: Dict[str, Any] = {}
        target_path = Path(config_path) if config_path else Path("config.json")
        
        if target_path.exists() and target_path.is_file():
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"[Config] Warning: Failed to load config from {target_path}: {e}")

        if overrides:
            for k, v in overrides.items():
                if v is not None:
                    data[k] = v

        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered)
