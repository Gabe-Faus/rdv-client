from __future__ import annotations
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from src.client.models import PeerEntry, PeerState
from src.client.config import AppConfig

logger = logging.getLogger("PeerTable")


class PeerTable:
    """
    Maintains peer states, RTT metrics, reconnect backoffs, and reconciliation with Rendezvous.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.peers: Dict[str, PeerEntry] = {}

    def get(self, peer_id: str) -> Optional[PeerEntry]:
        return self.peers.get(peer_id)

    def all(self) -> List[PeerEntry]:
        return list(self.peers.values())

    def connected_peers(self) -> List[PeerEntry]:
        return [p for p in self.peers.values() if p.state == PeerState.CONNECTED]

    def reconcile(self, discovered_list: List[Dict[str, Any]]) -> Tuple[List[PeerEntry], List[str]]:
        """
        Reconciles PeerTable with newly discovered peers from RDV.
        Returns (new_or_reopened_peers, gone_peer_ids).
        """
        discovered_ids = set()
        to_connect: List[PeerEntry] = []

        for p_data in discovered_list:
            name = p_data.get("name", "")
            ns = p_data.get("namespace", "")
            pid = f"{name}@{ns}"
            discovered_ids.add(pid)

            ip = p_data.get("ip", "")
            port = int(p_data.get("port", 0))
            ttl = int(p_data.get("ttl", 7200))
            expires_in = int(p_data.get("expires_in", 7200))

            if pid not in self.peers:
                entry = PeerEntry(
                    name=name,
                    namespace=ns,
                    ip=ip,
                    port=port,
                    ttl=ttl,
                    expires_in=expires_in,
                    state=PeerState.DISCOVERED,
                )
                self.peers[pid] = entry
                to_connect.append(entry)
                logger.info(f"[PeerTable] New peer discovered: {pid} at {ip}:{port}")
            else:
                entry = self.peers[pid]
                entry.ip = ip
                entry.port = port
                entry.ttl = ttl
                entry.expires_in = expires_in

                if entry.state == PeerState.GONE:
                    entry.state = PeerState.DISCOVERED
                    entry.reconnect_attempts = 0
                    entry.missed_pings = 0
                    to_connect.append(entry)
                    logger.info(f"[PeerTable] Peer {pid} reappeared in RDV, marked DISCOVERED")

        # Check for peers no longer present in RDV
        gone_ids: List[str] = []
        for pid, entry in list(self.peers.items()):
            if pid not in discovered_ids:
                if entry.state != PeerState.GONE:
                    logger.info(f"[PeerTable] Peer {pid} disappeared from RDV, marked GONE")
                    entry.state = PeerState.GONE
                    gone_ids.append(pid)

        return to_connect, gone_ids

    def mark_connecting(self, peer_id: str) -> None:
        if peer_id in self.peers:
            self.peers[peer_id].state = PeerState.CONNECTING

    def mark_connected(self, peer_id: str, direction: str, features: List[str], ip: str = "", port: int = 0) -> None:
        if peer_id not in self.peers:
            name, ns = peer_id.split("@", 1) if "@" in peer_id else (peer_id, "")
            self.peers[peer_id] = PeerEntry(
                name=name,
                namespace=ns,
                ip=ip,
                port=port,
                state=PeerState.CONNECTED,
                direction=direction,
                features=features,
            )
            logger.debug(f"[PeerTable] {peer_id} created and marked CONNECTED ({direction})")
            return

        entry = self.peers[peer_id]
        entry.state = PeerState.CONNECTED
        entry.direction = direction
        entry.features = features
        entry.reconnect_attempts = 0
        entry.missed_pings = 0
        logger.debug(f"[PeerTable] {peer_id} is now CONNECTED ({direction})")

    def mark_stale(self, peer_id: str) -> bool:
        """
        Marks a peer as STALE and computes next backoff reconnect time.
        Returns True if eligible for reconnect, False if max attempts reached (and marked GONE).
        """
        if peer_id not in self.peers:
            return False

        entry = self.peers[peer_id]
        entry.state = PeerState.STALE
        entry.direction = None
        entry.reconnect_attempts += 1

        if entry.reconnect_attempts > self.config.max_reconnect_attempts:
            entry.state = PeerState.GONE
            logger.info(
                f"[PeerTable] Peer {peer_id} exceeded max reconnect attempts ({self.config.max_reconnect_attempts}), marked GONE"
            )
            return False

        backoff = min(
            self.config.base_backoff * (2 ** (entry.reconnect_attempts - 1)),
            self.config.max_backoff,
        )
        entry.next_reconnect_at = time.time() + backoff
        logger.info(
            f"[PeerTable] Peer {peer_id} marked STALE (attempt {entry.reconnect_attempts}/{self.config.max_reconnect_attempts}, retry in {backoff:.1f}s)"
        )
        return True

    def mark_gone(self, peer_id: str) -> None:
        if peer_id in self.peers:
            self.peers[peer_id].state = PeerState.GONE
            self.peers[peer_id].direction = None

    def record_rtt(self, peer_id: str, rtt_seconds: float) -> None:
        if peer_id in self.peers:
            self.peers[peer_id].add_rtt_sample(rtt_seconds)

    def record_pong(self, peer_id: str) -> None:
        if peer_id in self.peers:
            self.peers[peer_id].missed_pings = 0

    def increment_missed_pings(self, peer_id: str) -> int:
        if peer_id in self.peers:
            self.peers[peer_id].missed_pings += 1
            return self.peers[peer_id].missed_pings
        return 0
