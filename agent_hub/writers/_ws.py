"""Minimal stdlib RFC6455 WebSocket client for local loopback bridges (CDP / Cline)."""

from __future__ import annotations

import base64
import os
import socket
import struct
from urllib.parse import urlparse


class SimpleWebSocket:
    """Zero-dependency client WebSocket supporting ws:// loopback endpoints."""

    def __init__(self, ws_url: str, timeout: float = 10.0, subprotocols: list[str] | None = None) -> None:
        u = urlparse(ws_url)
        host = u.hostname or "127.0.0.1"
        port = u.port or (443 if u.scheme == "wss" else 80)
        path = u.path or "/"
        if u.query:
            path += "?" + u.query

        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        proto_header = f"Sec-WebSocket-Protocol: {', '.join(subprotocols)}\r\n" if subprotocols else ""
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"{proto_header}"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        self.sock.sendall(req.encode("ascii"))
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            resp += chunk
        head = resp.split(b"\r\n\r\n", 1)[0].decode("utf-8", "replace")
        if "101" not in head.splitlines()[0]:
            self.sock.close()
            raise ConnectionError(f"WebSocket handshake failed: {head.splitlines()[0] if head else 'empty'}")
        self._buf = resp.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in resp else b""

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(max(4096, n - len(self._buf)))
            if not chunk:
                raise ConnectionError("WebSocket closed unexpectedly")
            self._buf += chunk
        out = self._buf[:n]
        self._buf = self._buf[n:]
        return out

    def send_text(self, text: str) -> None:
        """Send a masked RFC6455 text frame."""
        payload = text.encode("utf-8")
        mask = os.urandom(4)
        header = bytearray([0x81])
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def recv_text(self) -> str:
        """Receive next text frame, automatically answering ping frames."""
        while True:
            b1, b2 = self._read_exact(2)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exact(8))[0]
            mask_key = self._read_exact(4) if masked else b""
            payload = self._read_exact(length)
            if masked:
                payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

            if opcode == 0x8:  # close
                raise ConnectionError("WebSocket closed by peer")
            if opcode == 0x9:  # ping -> pong
                self._send_pong(payload)
                continue
            if opcode == 0xA:  # pong
                continue
            return payload.decode("utf-8", "replace")

    def _send_pong(self, payload: bytes) -> None:
        mask = os.urandom(4)
        length = min(len(payload), 125)
        frame = bytearray([0x8A, 0x80 | length]) + bytearray(mask)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload[:length]))
        self.sock.sendall(bytes(frame) + masked)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self) -> "SimpleWebSocket":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
