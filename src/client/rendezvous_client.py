from __future__ import annotations
import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from src.client.config import AppConfig

logger = logging.getLogger("RDV")


class RendezvousError(Exception):
    def __init__(self, message: str, raw_response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.raw_response = raw_response or {}


class RendezvousClient:
    """
    Communicates with the Rendezvous server using short-lived TCP connections.
    One connection per command, terminated by server after response.
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.public_ip: Optional[str] = None
        self.effective_ttl: int = config.register_ttl

    async def _send_command(self, payload: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        """
        Opens a single TCP connection, sends payload as a single JSON line, reads response, closes.
        """
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        if len(line.encode("utf-8")) > 32768:
            raise RendezvousError("Command exceeds 32KB limit")

        logger.debug(f"RDV -> {payload}")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.config.rdv_host, self.config.rdv_port),
                timeout=timeout,
            )
        except Exception as e:
            logger.error(f"Failed to connect to RDV at {self.config.rdv_host}:{self.config.rdv_port}: {e}")
            raise RendezvousError(f"Connection failed: {e}")

        try:
            writer.write(line.encode("utf-8"))
            await writer.drain()

            raw_resp = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not raw_resp:
                raise RendezvousError("Empty response from RDV server")

            resp_str = raw_resp.decode("utf-8").strip()
            resp = json.loads(resp_str)
            logger.debug(f"RDV <- {resp}")

            if resp.get("status") != "OK":
                err_msg = resp.get("message", "Unknown error")
                raise RendezvousError(f"RDV error: {err_msg}", resp)

            return resp
        except json.JSONDecodeError as e:
            raise RendezvousError(f"Invalid JSON from RDV: {e}")
        except asyncio.TimeoutError:
            raise RendezvousError("RDV request timed out")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def register(self) -> Dict[str, Any]:
        """
        Registers or refreshes the peer at Rendezvous server.
        """
        cmd = {
            "type": "REGISTER",
            "namespace": self.config.namespace,
            "name": self.config.name,
            "port": self.config.listen_port,
            "ttl": self.config.register_ttl,
        }
        resp = await self._send_command(cmd)
        self.public_ip = resp.get("ip")
        self.effective_ttl = resp.get("ttl", self.config.register_ttl)
        logger.info(
            f"[RDV] registered {self.config.peer_id} as {self.public_ip}:{self.config.listen_port} (ttl={self.effective_ttl})"
        )
        return resp

    async def discover(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Queries registered peers in the given namespace (or default namespace).
        Filters out self.
        """
        target_ns = namespace if namespace is not None else self.config.namespace
        cmd: Dict[str, Any] = {"type": "DISCOVER"}
        if target_ns:
            cmd["namespace"] = target_ns

        resp = await self._send_command(cmd)
        raw_peers = resp.get("peers", [])
        
        # Filter out self
        filtered: List[Dict[str, Any]] = []
        for p in raw_peers:
            if p.get("name") == self.config.name and p.get("namespace") == self.config.namespace:
                continue
            filtered.append(p)

        logger.debug(f"[RDV] discovered {len(filtered)} peer(s) in namespace '{target_ns}'")
        return filtered

    async def unregister(self) -> bool:
        """
        Unregisters the peer from the Rendezvous server upon graceful exit.
        """
        cmd = {
            "type": "UNREGISTER",
            "namespace": self.config.namespace,
            "name": self.config.name,
            "port": self.config.listen_port,
        }
        try:
            logger.info(f"[RDV] unregistering {self.config.peer_id}")
            resp = await self._send_command(cmd)
            return resp.get("status") == "OK"
        except Exception as e:
            logger.warning(f"[RDV] failed to unregister: {e}")
            return False
