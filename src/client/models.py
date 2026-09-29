from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any
import time


class PeerState(str, Enum):
    DISCOVERED = "DISCOVERED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    STALE = "STALE"
    GONE = "GONE"


class HostGameStatus(str, Enum):
    IDLE = "IDLE"
    OPEN = "OPEN"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"


class PlayerGameStatus(str, Enum):
    IDLE = "IDLE"
    JOINED = "JOINED"
    PLAYING = "PLAYING"


class GameResult(str, Enum):
    WON = "WON"
    LOST = "LOST"
    ABORTED = "ABORTED"


@dataclass
class PeerEntry:
    name: str
    namespace: str
    ip: str
    port: int
    ttl: int = 7200
    expires_in: int = 7200
    state: PeerState = PeerState.DISCOVERED
    rtt_history: List[float] = field(default_factory=list)
    reconnect_attempts: int = 0
    next_reconnect_at: float = 0.0
    features: List[str] = field(default_factory=list)
    missed_pings: int = 0
    direction: Optional[str] = None  # "inbound" | "outbound"

    @property
    def peer_id(self) -> str:
        return f"{self.name}@{self.namespace}"

    @property
    def avg_rtt_ms(self) -> Optional[float]:
        if not self.rtt_history:
            return None
        return (sum(self.rtt_history) / len(self.rtt_history)) * 1000.0

    def add_rtt_sample(self, rtt_seconds: float) -> None:
        self.rtt_history.append(rtt_seconds)
        if len(self.rtt_history) > 10:
            self.rtt_history.pop(0)


@dataclass
class GameLobbyInfo:
    game_id: str
    host: str
    status: str
    players: List[str]
    max_players: int
    lives: int
    word_length: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> GameLobbyInfo:
        return cls(
            game_id=data.get("game_id", ""),
            host=data.get("host", ""),
            status=data.get("status", "OPEN"),
            players=list(data.get("players", [])),
            max_players=data.get("max_players", 4),
            lives=data.get("lives", 6),
            word_length=data.get("word_length", 0),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "game_id": self.game_id,
            "host": self.host,
            "status": self.status,
            "players": self.players,
            "max_players": self.max_players,
            "lives": self.lives,
            "word_length": self.word_length,
        }
