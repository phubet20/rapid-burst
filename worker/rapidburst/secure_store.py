"""Local secret storage for the worker.

* MT5 credentials arrive end-to-end encrypted with the worker's RSA public key
  (the backend only relays ciphertext).
* At rest they are protected with Windows DPAPI (CryptProtectData, bound to the
  Windows user running the worker).  On non-Windows hosts secrets are kept in
  memory only and never written to disk.
"""
from __future__ import annotations

import base64
import json
import os
import sys
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

IS_WINDOWS = sys.platform == "win32"


# ----------------------------------------------------------------- DPAPI
if IS_WINDOWS:  # pragma: no cover
    import ctypes
    from ctypes import wintypes

    class _BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32

    def _blob(data: bytes) -> _BLOB:
        buf = ctypes.create_string_buffer(data, len(data))
        return _BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))

    def dpapi_protect(data: bytes) -> bytes:
        out = _BLOB()
        if not _crypt32.CryptProtectData(ctypes.byref(_blob(data)), "RapidBurst", None, None, None, 0x01,
                                         ctypes.byref(out)):
            raise OSError("CryptProtectData failed")
        try:
            return ctypes.string_at(out.pbData, out.cbData)
        finally:
            _kernel32.LocalFree(out.pbData)

    def dpapi_unprotect(data: bytes) -> bytes:
        out = _BLOB()
        if not _crypt32.CryptUnprotectData(ctypes.byref(_blob(data)), None, None, None, None, 0x01,
                                           ctypes.byref(out)):
            raise OSError("CryptUnprotectData failed")
        try:
            return ctypes.string_at(out.pbData, out.cbData)
        finally:
            _kernel32.LocalFree(out.pbData)
else:
    def dpapi_protect(data: bytes) -> bytes:  # pragma: no cover - not used off Windows
        raise OSError("DPAPI unavailable")

    def dpapi_unprotect(data: bytes) -> bytes:  # pragma: no cover
        raise OSError("DPAPI unavailable")


class SecureStore:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self._mem_creds: Optional[dict] = None
        self._key = self._load_or_create_key()

    # RSA key used by the UI to encrypt credentials for this worker only
    def _load_or_create_key(self):
        path = os.path.join(self.data_dir, "worker_key.bin")
        if IS_WINDOWS and os.path.exists(path):
            try:
                pem = dpapi_unprotect(open(path, "rb").read())
                return serialization.load_pem_private_key(pem, password=None)
            except Exception:  # noqa: BLE001
                pass
        key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        if IS_WINDOWS:
            pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                    serialization.NoEncryption())
            with open(path, "wb") as fh:
                fh.write(dpapi_protect(pem))
        return key

    def public_key_spki_b64(self) -> str:
        der = self._key.public_key().public_bytes(serialization.Encoding.DER,
                                                  serialization.PublicFormat.SubjectPublicKeyInfo)
        return base64.b64encode(der).decode()

    def decrypt_credentials(self, ciphertext_b64: str) -> dict:
        pt = self._key.decrypt(base64.b64decode(ciphertext_b64),
                               padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
        data = json.loads(pt.decode("utf-8"))
        login = int(str(data["login"]).strip())
        server = str(data["server"]).strip()
        password = str(data["password"])
        if not server or not password:
            raise ValueError("login, password and server are required")
        return {"login": login, "password": password, "server": server}

    # credentials at rest
    def save_credentials(self, creds: dict) -> str:
        self._mem_creds = dict(creds)
        if IS_WINDOWS:
            with open(os.path.join(self.data_dir, "mt5_creds.bin"), "wb") as fh:
                fh.write(dpapi_protect(json.dumps(creds).encode()))
            return "stored encrypted (Windows DPAPI)"
        return "kept in memory only (non-Windows host)"

    def load_credentials(self) -> Optional[dict]:
        if self._mem_creds:
            return dict(self._mem_creds)
        env_login = os.environ.get("MT5_LOGIN")
        if env_login and os.environ.get("MT5_PASSWORD") and os.environ.get("MT5_SERVER"):
            return {"login": int(env_login), "password": os.environ["MT5_PASSWORD"],
                    "server": os.environ["MT5_SERVER"]}
        path = os.path.join(self.data_dir, "mt5_creds.bin")
        if IS_WINDOWS and os.path.exists(path):
            try:
                self._mem_creds = json.loads(dpapi_unprotect(open(path, "rb").read()))
                return dict(self._mem_creds)
            except Exception:  # noqa: BLE001
                return None
        return None

    def forget_credentials(self) -> None:
        self._mem_creds = None
        path = os.path.join(self.data_dir, "mt5_creds.bin")
        if os.path.exists(path):
            os.remove(path)
