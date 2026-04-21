"""
MonaLisa DRM Wrapper (Keys Only)
Handles ticket parsing, key extraction via CDM, and exposes keys to tracks.
"""
import base64
import logging
from pathlib import Path
from typing import Optional, Union, Any
from uuid import UUID
from .monalisa_cdm import MonaLisaCDM

log = logging.getLogger(__name__)

class MonaLisa:
    class Exceptions:
        class TicketNotFound(Exception): pass
        class KeyExtractionFailed(Exception): pass

    def __init__(self, ticket: Union[str, bytes], aes_key: Union[str, bytes] = None,
                 device_path: Optional[Path] = None, **kwargs: Any):
        if not ticket:
            raise self.Exceptions.TicketNotFound("No PSSH/ticket data provided.")

        self._ticket = ticket
        self._aes_key = bytes.fromhex(aes_key) if aes_key and isinstance(aes_key, str) else aes_key
        self._device_path = device_path
        self._kid: Optional[UUID] = None
        self._key_hex: Optional[str] = None
        self._key_bytes: Optional[bytes] = None
        self._cdm: Optional[MonaLisaCDM] = None
        self.data: dict = kwargs or {}

        self._extract_keys()

    def _ensure_cdm(self) -> MonaLisaCDM:
        if self._cdm is None:
            self._cdm = MonaLisaCDM(device_path=self._device_path)
            self._cdm.open()
        return self._cdm

    def _extract_keys(self) -> None:
        try:
            cdm = self._ensure_cdm()
            keys = cdm.extract_keys(self._ticket)
            if keys:
                self._key_hex = keys.get("key")
                if self._key_hex:
                    self._key_bytes = bytes.fromhex(self._key_hex)
                kid_hex = keys.get("kid")
                if kid_hex:
                    self._kid = UUID(hex=kid_hex)
                log.debug(f"MonaLisa keys extracted: KID={self._kid}, Key={self._key_hex}")
            else:
                raise self.Exceptions.KeyExtractionFailed("Key extraction returned empty values")
        except Exception as e:
            raise self.Exceptions.KeyExtractionFailed(f"Failed to extract keys: {e}")

    @classmethod
    def from_ticket(cls, ticket: Union[str, bytes], aes_key: Union[str, bytes] = None, device_path: Optional[Path] = None) -> "MonaLisa":
        return cls(ticket=ticket, aes_key=aes_key, device_path=device_path)

    @property
    def kid(self) -> Optional[UUID]: return self._kid
    @property
    def key(self) -> Optional[str]: return self._key_hex
    @property
    def key_bytes(self) -> Optional[bytes]: return self._key_bytes
    @property
    def pssh(self) -> str:
        if isinstance(self._ticket, bytes):
            try: return self._ticket.decode("utf-8")
            except: return base64.b64encode(self._ticket).decode("ascii")
        return self._ticket

    def close(self) -> None:
        if self._cdm:
            self._cdm.close()
            self._cdm = None