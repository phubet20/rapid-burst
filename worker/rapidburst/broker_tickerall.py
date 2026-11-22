"""TickerAll adapter: the MT5Broker interface over TickerAll's hosted MT5 API.

Lets the unchanged trading engine run on a Linux host (e.g. Render) with no
Windows PC and no MetaTrader terminal: TickerAll keeps the broker connection
and exposes REST + a WebSocket tick stream (https://tickerall.com/docs).

Honesty rules (same as the MT5 adapter):
* every number comes from TickerAll / the broker; nothing is invented;
* fields TickerAll does not publish are reported as unknown, never as made-up
  values (see ``symbol_info`` trade_mode and ``calc_margin``);
* TickerAll cannot store an MT5 magic number (positions read magic 0), so the
  bot's own trades are recognised by their comment prefix (cfg.comment_prefix)
  and by the ticket registry this adapter keeps; they are then reported with the
  bot's magic so the engine's ownership rules work unchanged.

Free plan facts this adapter relies on (tickerall.com/pricing, /faq, /docs):
demo accounts only via the API (live -> 403 FREE_TIER_LIVE_REJECTED), an idle
account cools after ~30 min but never while a WebSocket subscription is open,
and the MT5 password is held in TickerAll memory only - after a TickerAll
restart the session must be re-created with POST /v1/sessions.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("rb.tickerall")

DEFAULT_BASE = "https://api.tickerall.com"
ENTRY_MAP = {"IN": 0, "OUT": 1, "INOUT": 2, "OUT_BY": 3}
ORDER_TYPE_MAP = {"BUY_LIMIT": 2, "SELL_LIMIT": 3, "BUY_STOP": 4, "SELL_STOP": 5,
                  "BUY_STOP_LIMIT": 6, "SELL_STOP_LIMIT": 7}
MARGIN_MODE_MAP = {"netting": 0, "exchange": 1, "hedging": 2}
ACCOUNT_TYPE_MAP = {"demo": 0, "contest": 1, "real": 2, "live": 2}


class TickerAllError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Optional[dict] = None):
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message, self.details = status, code, message, details or {}


def _iso_to_epoch(s: Any) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _epoch_to_iso(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


class TickerAllBroker:
    name = "tickerall"
    avail_label = "TickerAll API key"

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None,
                 ws_url: Optional[str] = None, magic: int = 26112201, comment_prefix: str = "RBurst",
                 data_dir: Optional[str] = None, cache_ttl: float = 1.0, http_timeout: float = 15.0):
        self.api_key = (api_key if api_key is not None else os.environ.get("TICKERALL_API_KEY", "")).strip()
        self.base = (base_url or os.environ.get("TICKERALL_BASE_URL") or DEFAULT_BASE).rstrip("/")
        self.ws_url = ws_url or os.environ.get("TICKERALL_WS_URL") or (
            self.base.replace("https://", "wss://").replace("http://", "ws://") + "/v1/stream")
        self.magic = int(magic)
        self.prefix = comment_prefix
        self.data_dir = data_dir
        self.cache_ttl = cache_ttl
        self.http_timeout = http_timeout
        self.account_id: Optional[str] = None
        self._creds: Optional[dict] = None          # memory only; needed to re-arm a cooled session
        self._lock = threading.RLock()
        self._rearm_lock = threading.Lock()
        self._last_rearm = 0.0
        self.rearms = 0
        self._acct_cache: Optional[dict] = None
        self._acct_ts = 0.0
        self._list_row: dict = {}
        self._srv_off = 0
        self._hot = False
        self._specs: dict[str, dict] = {}
        self._spec_rows: Optional[dict] = None
        self._live_at: dict[str, float] = {}
        self._symbols: Optional[list] = None
        self._watched: set[str] = set()
        self._ticks: dict[str, deque] = {}
        self._last_tick: dict[str, dict] = {}
        self._own: set[int] = set()
        self._load_registry()
        # stream thread
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_stop = threading.Event()
        self._ws_ok = False
        self._ws_send_q: Optional[asyncio.Queue] = None
        self.stream_events = deque(maxlen=50)

    # ------------------------------------------------------------- registry
    def _reg_path(self) -> Optional[str]:
        return os.path.join(self.data_dir, "tickerall_own.json") if self.data_dir else None

    def _load_registry(self) -> None:
        p = self._reg_path()
        if p and os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    self._own = {int(x) for x in json.load(fh).get("tickets", [])}
            except Exception:  # noqa: BLE001
                self._own = set()

    def _remember(self, ticket: int) -> None:
        if not ticket or int(ticket) in self._own:
            return
        self._own.add(int(ticket))
        p = self._reg_path()
        if p:
            try:
                os.makedirs(self.data_dir, exist_ok=True)
                tmp = p + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump({"tickets": sorted(self._own)[-5000:]}, fh)
                os.replace(tmp, p)
            except OSError:
                pass

    def _is_own(self, ticket: Any, comment: Any) -> bool:
        try:
            t = int(ticket)
        except (TypeError, ValueError):
            t = 0
        own = t in self._own or str(comment or "").startswith(self.prefix)
        if own and t:
            self._remember(t)
        return own

    # ----------------------------------------------------------------- http
    def _req(self, method: str, path: str, body: Optional[dict] = None, query: Optional[dict] = None,
             idem: Optional[str] = None, timeout: Optional[float] = None) -> Any:
        url = self.base + path
        if query:
            url += "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.api_key}")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", "rapid-burst/tickerall-adapter")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if method in ("POST", "PATCH", "DELETE", "PUT"):
            req.add_header("Idempotency-Key", idem or uuid.uuid4().hex)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.http_timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            try:
                err = json.loads(exc.read() or b"{}")
            except ValueError:
                err = {}
            code = str(err.get("error") or err.get("code") or exc.reason)
            if exc.code == 409 and code == "BROKER_ACCOUNT_NOT_HOT":
                self._hot = False
            raise TickerAllError(exc.code, code, str(err.get("message") or ""), err.get("details")) from None

    # ----------------------------------------------------------- connection
    def available(self) -> tuple[bool, str]:
        if not self.api_key:
            return False, "TICKERALL_API_KEY is not set on the server (create a free key at tickerall.com)"
        return True, "TickerAll API key configured (hosted MT5 connection)"

    def last_error(self) -> str:
        return ""

    def _open_session(self, login: int, password: str, server: str, timeout_s: float) -> dict:
        deadline = time.time() + timeout_s
        while True:
            try:
                return self._req("POST", "/v1/sessions", {"broker": "mt5", "server": server,
                                                         "account": int(login), "password": password},
                                 timeout=max(20.0, self.http_timeout))
            except TickerAllError as e:
                wait = _f((e.details or {}).get("retryAfterSeconds"), 0.0)
                if e.code == "BROKER_AUTH_FAILED" and (e.details or {}).get("reason") == "ACCOUNT_NOT_READY" \
                        and wait and time.time() + wait < deadline:
                    time.sleep(min(wait, 30.0))
                    continue
                if e.status in (429, 503) and time.time() + 3 < deadline:
                    time.sleep(3.0)
                    continue
                raise

    def connect(self, login: int, password: str, server: str, timeout_ms: int = 60000) -> tuple[bool, str]:
        ok, why = self.available()
        if not ok:
            return False, why
        try:
            res = self._open_session(int(login), password, server, timeout_ms / 1000.0)
        except TickerAllError as e:
            hint = {
                "BROKER_AUTH_FAILED": "MT5 login rejected by the broker (check login / password / server)",
                "FREE_TIER_LIVE_REJECTED": "TickerAll free plan only accepts DEMO accounts",
                "BROKER_UNREACHABLE": "broker server unreachable from TickerAll (check the server name)",
                "BROKER_ACCOUNT_ALREADY_LINKED": "this MT5 login is linked to another TickerAll customer",
                "BROKER_ACCOUNT_HELD_BY_ANOTHER_KEY": "this MT5 login belongs to another of your TickerAll keys",
                "UNAUTHORIZED": "TickerAll API key rejected (check TICKERALL_API_KEY)",
            }.get(e.code, "")
            return False, f"TickerAll connect failed - {e.code}" + (f": {hint}" if hint else (f": {e.message}" if e.message else ""))
        except Exception as exc:  # noqa: BLE001
            return False, f"TickerAll connect failed - {type(exc).__name__}: {exc}"
        with self._lock:
            self.account_id = str(res.get("accountId") or "")
            self._creds = {"login": int(login), "password": password, "server": server}
            self._hot = True
            self._acct_ts = 0.0
        if not self.account_id:
            return False, "TickerAll did not return an account id"
        acct = self.account(force=True)
        if not acct:
            return False, "TickerAll session opened but the account could not be read"
        self._start_stream()
        return True, "connected"

    def _rearm(self) -> bool:
        """Re-create a cooled session with the in-memory credentials (TickerAll keeps none)."""
        c = self._creds
        if not c:
            return False
        with self._rearm_lock:            # one re-arm at a time (REST and stream may both notice)
            if time.time() - self._last_rearm < 5.0:
                return self._hot
            self._last_rearm = time.time()
            try:
                res = self._open_session(c["login"], c["password"], c["server"], 30.0)
                self.account_id = str(res.get("accountId") or self.account_id)
                self._hot = True
                self._acct_ts = 0.0
                self.rearms += 1
                return True
            except Exception as exc:  # noqa: BLE001
                log.warning("re-arm failed: %s", type(exc).__name__)
                return False

    def shutdown(self) -> None:
        self._stop_stream()
        aid = self.account_id
        if aid:
            try:
                self._req("DELETE", f"/v1/sessions/{aid}", timeout=10)
            except Exception as exc:  # noqa: BLE001
                log.info("session release: %s", exc)
        with self._lock:
            self._hot = False
            self._acct_cache = None
            self._creds = None
            self.account_id = None

    # --------------------------------------------------------------- account
    def _account_raw(self, force: bool = False) -> Optional[dict]:
        if not self.account_id:
            return None
        with self._lock:
            if not force and self._acct_cache is not None and time.time() - self._acct_ts < self.cache_ttl:
                return self._acct_cache
        try:
            data = self._req("GET", f"/v1/accounts/{self.account_id}")
        except TickerAllError as e:
            if e.code == "BROKER_ACCOUNT_NOT_HOT" and self._rearm():
                try:
                    data = self._req("GET", f"/v1/accounts/{self.account_id}")
                except Exception:  # noqa: BLE001
                    return None
            else:
                return None
        except Exception:  # noqa: BLE001
            return None
        online = data.get("status") == "online" and bool(data.get("account"))
        with self._lock:
            if online:
                self._acct_cache, self._acct_ts, self._hot = data, time.time(), True
                self._srv_off = int(_f((data.get("account") or {}).get("serverTimeOffsetSeconds"), self._srv_off))
            else:
                self._hot = False
                self._acct_cache, self._acct_ts = None, 0.0
        if not self._list_row or force:
            try:
                rows = self._req("GET", "/v1/accounts")
                rows = rows if isinstance(rows, list) else rows.get("accounts", [])
                self._list_row = next((r for r in rows if r.get("id") == self.account_id), {}) or {}
            except Exception:  # noqa: BLE001
                pass
        return data if online else None

    def is_connected(self) -> bool:
        return bool(self.account_id) and self._hot and self._account_raw() is not None

    def terminal(self) -> dict:
        d = self._account_raw()
        if not d:
            return {}
        read_only = bool(self._list_row.get("readOnly"))
        return {"connected": True, "trade_allowed": not read_only, "tradeapi_disabled": read_only,
                "ping_last": None, "provider": "TickerAll", "stream_connected": self._ws_ok}

    def account(self, force: bool = False) -> dict:
        d = self._account_raw(force=force)
        if not d:
            return {}
        a = d.get("account") or {}
        typ = str(a.get("accountType") or "").lower()
        trade_mode = ACCOUNT_TYPE_MAP.get(typ, 0 if d.get("isDemo") else 2)
        if not d.get("isDemo") and trade_mode == 0:
            trade_mode = 2          # never report a non-demo account as DEMO
        bal, eq = a.get("balance"), a.get("equity")
        mm = self._list_row.get("marginMode")
        read_only = bool(self._list_row.get("readOnly"))
        return {
            "login": (self._creds or {}).get("login"), "server": d.get("server") or (self._creds or {}).get("server"),
            "name": a.get("name"), "company": f"via TickerAll ({a.get('group') or d.get('server') or ''})",
            "currency": a.get("currency"), "leverage": a.get("leverage"), "trade_mode": trade_mode,
            "balance": bal, "equity": eq, "margin": a.get("margin"), "margin_free": a.get("freeMargin"),
            "margin_level": a.get("marginLevel") or 0.0,
            "profit": (round(_f(eq) - _f(bal), 2) if bal is not None and eq is not None else None),
            "trade_allowed": not read_only, "trade_expert": not read_only,
            "margin_mode": MARGIN_MODE_MAP.get(str(mm).lower(), 2) if mm else 2,
            "margin_mode_known": bool(mm), "is_demo": bool(d.get("isDemo")),
        }

    # --------------------------------------------------------------- symbols
    def _symbol_list(self) -> list:
        if self._symbols is None and self.account_id:
            try:
                self._symbols = list(self._req("GET", f"/v1/accounts/{self.account_id}/symbols").get("symbols") or [])
            except Exception:  # noqa: BLE001
                return []
        return self._symbols or []

    CRYPTO = {"BTC", "ETH", "LTC", "XRP", "BCH", "ADA", "SOL", "DOGE", "DOT", "LNK", "LINK", "BNB", "XLM", "TRX",
              "AVAX", "MATIC", "UNI", "ATOM", "SHIB", "TON", "NEAR", "APT", "ETC", "EOS", "DASH", "XMR", "ZEC"}
    METALS = {"XAU", "XAG", "XPT", "XPD"}
    FIAT = {"USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "SGD", "HKD", "CNH", "CNY", "MXN", "ZAR", "TRY",
            "NOK", "SEK", "DKK", "PLN", "CZK", "HUF", "THB", "ILS", "RUB"}
    ALIASES = {"gold": "xau", "silver": "xag", "platinum": "xpt", "palladium": "xpd", "bitcoin": "btc",
               "ethereum": "eth", "litecoin": "ltc", "ripple": "xrp", "dogecoin": "doge", "solana": "sol",
               "forex": "fx", "currency": "fx", "stock": "stocks", "index": "indices"}

    def _all_specs(self) -> dict[str, dict]:
        if self._spec_rows is None and self.account_id:
            rows: dict[str, dict] = {}
            try:
                res = self._req("GET", f"/v1/accounts/{self.account_id}/symbol-specs", timeout=30)
                for r in (res.get("specs") if isinstance(res, dict) else res) or []:
                    if r.get("name"):
                        rows[r["name"]] = r
            except Exception as exc:  # noqa: BLE001
                log.info("symbol-specs unavailable: %s", type(exc).__name__)
            self._spec_rows = rows
        return self._spec_rows or {}

    def _classify(self, name: str, spec: dict) -> str:
        base = str(spec.get("baseCurrency") or "").upper()
        prof = str(spec.get("profitCurrency") or "").upper()
        n = "".join(ch for ch in name.upper() if ch.isalpha())
        head = base or n[:3]
        if head in self.CRYPTO or any(n.startswith(c) for c in self.CRYPTO if len(c) >= 3):
            return "crypto"
        if head in self.METALS or n[:3] in self.METALS:
            return "metal"
        if any(k in n for k in ("OIL", "BRENT", "WTI", "NGAS", "NATGAS")):
            return "energy"
        if (base in self.FIAT and prof in self.FIAT) or (not base and n[:3] in self.FIAT and n[3:6] in self.FIAT):
            return "fx"
        return "indices/stocks/other"

    def describe(self, name: str) -> tuple[str, str]:
        spec = self._all_specs().get(name, {})
        kind = self._classify(name, spec)
        base, prof = spec.get("baseCurrency"), spec.get("profitCurrency")
        pair = f"{base}/{prof}" if base and prof else (f"quoted in {prof}" if prof else "")
        return (f"{kind} (derived){' ' + pair if pair else ''}".strip(), kind)

    def search_symbols(self, query: str, limit: int = 60) -> list[dict]:
        """Full broker symbol catalogue; matches name, derived class and currency pair."""
        q = " ".join(self.ALIASES.get(t, t) for t in (query or "").lower().split())
        toks = q.split()
        out = []
        for s in self._symbol_list():
            desc, kind = self.describe(s)
            hay = f"{s} {desc} {kind}".lower()
            if toks and not all(t in hay for t in toks):
                continue
            out.append({"name": s, "desc": desc, "path": kind, "visible": s in self._watched, "trade_mode": None})
        key = toks[0] if toks else ""
        out.sort(key=lambda x: (not x["name"].lower().startswith(key), x["path"], x["name"]))
        return out[:limit]

    def select_symbol(self, symbol: str) -> bool:
        if symbol not in self._symbol_list():
            return False
        if symbol not in self._watched:
            self._watched.add(symbol)
            self._ticks.setdefault(symbol, deque(maxlen=5000))
            self._ws_subscribe([symbol])
        return True

    def symbol_info(self, symbol: str) -> dict:
        if symbol in self._specs:
            return self._specs[symbol]
        if not self.account_id:
            return {}
        try:
            spec = self._req("GET", f"/v1/accounts/{self.account_id}/symbols/{urllib.parse.quote(symbol)}/spec")
            spec = spec.get("spec", spec)
        except Exception:  # noqa: BLE001
            return {}
        if spec.get("point") is None or spec.get("digits") is None:
            return {}                        # unknown geometry -> not tradable by the bot
        tva = spec.get("tickValueAccount")
        tv_src = "broker (tickValueAccount)"
        cs, ts = _f(spec.get("contractSize")), _f(spec.get("tickSize"), _f(spec["point"]))
        if not tva:
            # TickerAll did not give the account-currency tick value: derive it only when it is exact
            cur = (self.account() or {}).get("currency")
            prof, base = spec.get("profitCurrency"), spec.get("baseCurrency")
            if cs and ts and cur and prof == cur:
                tva, tv_src = cs * ts, "derived: contract x tick (quote currency = account currency)"
            elif cs and ts and cur and base == cur:
                px = _f(self.last_tick(symbol).get("bid"))
                if px > 0:
                    tva, tv_src = cs * ts / px, "derived: contract x tick / price (base currency = account currency)"
            if not tva:
                tva, tv_src = 0.0, "unknown (cannot be derived safely)"
        info = {
            "name": spec.get("name", symbol), "point": _f(spec["point"]), "digits": int(spec["digits"]),
            "volume_min": _f(spec.get("volumeMin")), "volume_max": _f(spec.get("volumeMax")),
            "volume_step": _f(spec.get("volumeStep")),
            "trade_tick_size": ts,
            # risk is sized off the ACCOUNT-currency tick value; unknown -> 0 so the trader refuses the symbol
            "trade_tick_value": _f(tva), "trade_tick_value_loss": _f(tva), "tick_value_source": tv_src,
            "trade_contract_size": cs,
            "trade_stops_level": int(_f(spec.get("stopLevel"))), "trade_freeze_level": int(_f(spec.get("freezeLevel"))),
            "currency_margin": spec.get("marginCurrency"), "currency_profit": spec.get("profitCurrency"),
            "currency_base": spec.get("baseCurrency"),
            # TickerAll does not publish the symbol trade mode; the broker validates every order
            # (MARKET_CLOSED / TRADE_DISABLED come back as rejections). 4 = "full" is therefore an
            # assumption that is labelled as such, not broker data.
            "trade_mode": 4, "trade_mode_source": "assumed (TickerAll does not publish it)",
            "filling_mode": 0, "visible": True, "spec_source": spec.get("specSource"),
        }
        self._specs[symbol] = info
        return info

    # ----------------------------------------------------------------- ticks
    def _on_tick(self, msg: dict) -> None:
        s = msg.get("symbol")
        bid, ask = _f(msg.get("bid")), _f(msg.get("ask"))
        if not s or bid <= 0 or ask <= 0 or msg.get("stale"):
            return
        self._live_at[s] = time.time()
        t = _iso_to_epoch(msg.get("timestamp")) or time.time()
        msc = int(round((t + self._srv_off) * 1000))   # broker server time, like MT5 time_msc
        buf = self._ticks.setdefault(s, deque(maxlen=5000))
        if buf and msc <= buf[-1][3]:
            msc = buf[-1][3] + 1                         # keep strictly increasing for ticks_since
        buf.append((msc / 1000.0, bid, ask, msc))
        self._last_tick[s] = {"time": int(msc // 1000), "time_msc": msc, "bid": bid, "ask": ask}

    def quote_state(self, symbol: str, fresh_s: float = 60.0) -> str:
        """live = a non-stale tick arrived in the last fresh_s; closed = stream is up but no live ticks."""
        if time.time() - self._live_at.get(symbol, 0.0) <= fresh_s:
            return "live"
        return "closed" if self._ws_ok else "no_stream"

    def wait_quotes(self, symbols: list, timeout: float) -> None:
        """Give freshly subscribed symbols a moment to deliver their first live tick."""
        for s in symbols:
            self.select_symbol(s)          # ensure subscribed (idempotent)
        end = time.time() + timeout
        while time.time() < end:
            if all(self.quote_state(s) == "live" for s in symbols):
                return
            time.sleep(0.2)

    def last_tick(self, symbol: str) -> dict:
        lt = self._last_tick.get(symbol)
        if lt:
            return dict(lt)
        if not self.account_id:
            return {}
        try:   # REST quote has no tick time: use receipt time (labelled)
            q = self._req("GET", f"/v1/accounts/{self.account_id}/symbols/{urllib.parse.quote(symbol)}/quote")
        except Exception:  # noqa: BLE001
            return {}
        if not q or q.get("bid") in (None, 0) or q.get("ask") in (None, 0):
            return {}
        msc = int((time.time() + self._srv_off) * 1000)
        return {"time": msc // 1000, "time_msc": msc, "bid": _f(q["bid"]), "ask": _f(q["ask"]),
                "time_source": "receipt"}

    def ticks_since(self, symbol: str, last_msc: int, max_count: int = 2000) -> list[tuple[float, float, float, int]]:
        buf = self._ticks.get(symbol)
        if not buf:
            return []
        out = [t for t in list(buf) if t[3] > last_msc]
        return out[-max_count:]

    # ----------------------------------------------------- positions/orders
    def _map_pos(self, p: dict) -> dict:
        t = int(p.get("ticket") or 0)
        own = self._is_own(t, p.get("comment"))
        return {
            "ticket": t, "identifier": t, "symbol": p.get("symbol"), "type": 0 if p.get("side") == "BUY" else 1,
            "volume": _f(p.get("volume")), "price_open": _f(p.get("entryPrice")),
            "price_current": _f(p.get("currentPrice")), "sl": _f(p.get("stopLoss")), "tp": _f(p.get("takeProfit")),
            "profit": _f(p.get("profit")), "swap": _f(p.get("swap")), "commission": _f(p.get("commission")),
            "magic": self.magic if own else int(_f(p.get("magic"))), "comment": p.get("comment") or "",
            "time": _iso_to_epoch(p.get("openTime")) + self._srv_off,
        }

    def positions(self, symbol: Optional[str] = None, force: bool = False) -> list[dict]:
        d = self._account_raw(force=force)
        if not d:
            return []
        return [self._map_pos(p) for p in (d.get("positions") or []) if symbol is None or p.get("symbol") == symbol]

    def position(self, ticket: int) -> Optional[dict]:
        for p in self.positions(force=True):
            if p["ticket"] == int(ticket):
                return p
        return None

    def orders(self) -> list[dict]:
        if not self.account_id:
            return []
        try:
            res = self._req("GET", f"/v1/accounts/{self.account_id}/orders")
        except Exception:  # noqa: BLE001
            return []
        rows = res if isinstance(res, list) else res.get("orders", [])
        out = []
        for o in rows:
            t = int(_f(o.get("ticket")))
            own = self._is_own(t, o.get("comment"))
            out.append({"ticket": t, "symbol": o.get("symbol"), "type": ORDER_TYPE_MAP.get(str(o.get("type")), -1),
                        "volume_current": _f(o.get("volume")), "price_open": _f(o.get("price")),
                        "sl": _f(o.get("stopLoss")), "tp": _f(o.get("takeProfit")),
                        "magic": self.magic if own else int(_f(o.get("magic"))), "comment": o.get("comment") or ""})
        return out

    def deals(self, t_from: float, t_to: float) -> list[dict]:
        """Deals between two broker-server times (same convention as MT5 history_deals_get)."""
        if not self.account_id:
            return []
        out, cursor = [], None
        q = {"from": _epoch_to_iso(t_from - self._srv_off), "to": _epoch_to_iso(t_to - self._srv_off), "limit": 500}
        for _ in range(10):
            try:
                res = self._req("GET", f"/v1/accounts/{self.account_id}/deals-history", query=dict(q, cursor=cursor))
            except Exception:  # noqa: BLE001
                break
            for d in res.get("deals") or []:
                pid = int(_f(d.get("positionId")))
                out.append({"ticket": int(_f(d.get("ticket"))), "position_id": pid, "symbol": d.get("symbol"),
                            "entry": ENTRY_MAP.get(str(d.get("entry")), -1), "volume": _f(d.get("volume")),
                            "price": _f(d.get("price")), "profit": _f(d.get("profit")), "swap": _f(d.get("swap")),
                            "commission": _f(d.get("commission")), "fee": 0.0,
                            "magic": self.magic if pid in self._own else 0,
                            "time": _iso_to_epoch(d.get("time")) + self._srv_off})
            cursor = res.get("nextCursor")
            if not res.get("hasMore") or not cursor:
                break
        return out

    def calc_margin(self, side: str, symbol: str, volume: float, price: float) -> Optional[float]:
        """Margin ESTIMATE (TickerAll has no margin calculator): contract x lots / leverage,
        only when the margin or profit currency is the account currency. Otherwise None ->
        the engine skips the entry rather than guessing. The broker still rejects NO_MONEY."""
        info = self.symbol_info(symbol)
        acct = self.account()
        cs, lev, cur = _f(info.get("trade_contract_size")), _f(acct.get("leverage")), acct.get("currency")
        if not info or not cs or not lev or not cur:
            return None
        if info.get("currency_margin") == cur:
            return cs * volume / lev
        if info.get("currency_profit") == cur and price > 0:
            return cs * volume * price / lev
        return None

    # ------------------------------------------------------------ execution
    def market_order(self, symbol: str, side: str, volume: float, sl: float, tp: float,
                     deviation: int, magic: int, comment: str) -> dict:
        if not self.account_id:
            return {"ok": False, "msg": "TickerAll not connected"}
        info = self.symbol_info(symbol)
        if not info:
            return {"ok": False, "msg": f"symbol {symbol} unavailable"}
        digits = int(info["digits"])
        lt = self.last_tick(symbol)
        req_price = _f(lt.get("ask") if side == "BUY" else lt.get("bid"))
        comment = comment[:31]
        body = {"type": "market", "symbol": symbol, "side": side, "volume": float(volume),
                "stopLoss": round(sl, digits), "comment": comment}
        if tp:
            body["takeProfit"] = round(tp, digits)
        idem = uuid.uuid4().hex
        t_send = time.time()
        res, last_exc = None, None
        for _ in range(3):   # same Idempotency-Key: a retry can never place a second order
            try:
                res = self._req("POST", f"/v1/accounts/{self.account_id}/orders", body, idem=idem)
                break
            except TickerAllError as e:
                if e.status in (408, 429, 500, 502, 503, 504):
                    last_exc = e
                    time.sleep(0.5)
                    continue
                return {"ok": False, "retcode": e.status, "msg": f"order rejected: {e.code} {e.message}".strip()}
            except Exception as exc:  # noqa: BLE001  network: outcome unknown
                last_exc = exc
                time.sleep(0.5)
        latency = time.time() - t_send
        ticket = int(_f((res or {}).get("ticket")))
        pos = None
        for _ in range(6):
            for p in self.positions(symbol, force=True):
                if (ticket and p["ticket"] == ticket) or p["comment"].startswith(comment):
                    pos = p
                    break
            if pos:
                break
            time.sleep(0.4)
        if pos is None:
            if res is None:
                return {"ok": False, "msg": f"order outcome unknown ({last_exc}) and no position found"}
            return {"ok": False, "retcode": 201,
                    "msg": "order reported done but position not found - entries paused for verification"}
        self._remember(pos["ticket"])
        return {"ok": True, "retcode": 10009, "msg": "done", "ticket": pos["ticket"], "price": pos["price_open"],
                "req_price": req_price or pos["price_open"], "volume": pos["volume"], "latency": latency}

    def close_position(self, ticket: int, deviation: int, magic: int) -> dict:
        if not self.account_id:
            return {"ok": False, "msg": "TickerAll not connected", "verified": False}
        if self.position(ticket) is None:
            return {"ok": True, "msg": "already closed", "verified": True}
        idem, msg, price = uuid.uuid4().hex, "", None
        for _ in range(3):
            try:
                r = self._req("DELETE", f"/v1/accounts/{self.account_id}/positions/{int(ticket)}", idem=idem)
                price = r.get("closePrice")
                msg = "closed"
            except TickerAllError as e:
                msg = f"{e.code} {e.message}".strip()
                if e.code not in ("TICKET_NOT_FOUND",) and e.status not in (429, 500, 502, 503, 504):
                    break
            except Exception as exc:  # noqa: BLE001
                msg = f"{type(exc).__name__}: {exc}"
            for _ in range(3):
                if self.position(ticket) is None:
                    return {"ok": True, "msg": msg or "closed", "verified": True, "price": price}
                time.sleep(0.3)
        gone = self.position(ticket) is None
        return {"ok": gone, "msg": msg if gone else f"close not verified: {msg}", "verified": gone, "price": price}

    def cancel_order(self, ticket: int) -> dict:
        if not self.account_id:
            return {"ok": False, "msg": "TickerAll not connected"}
        try:
            self._req("DELETE", f"/v1/accounts/{self.account_id}/orders/{int(ticket)}")
            return {"ok": True, "msg": "cancelled"}
        except TickerAllError as e:
            if e.code == "TICKET_NOT_FOUND":
                return {"ok": True, "msg": "already gone"}
            return {"ok": False, "msg": f"{e.code} {e.message}".strip()}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "msg": str(exc)}

    def modify_sl(self, ticket: int, sl: float, tp: float) -> dict:
        if not self.account_id:
            return {"ok": False, "msg": "TickerAll not connected"}
        pos = self.position(ticket)
        if pos is None:
            return {"ok": False, "msg": "no position"}
        digits = int((self.symbol_info(pos["symbol"]) or {}).get("digits", 8))
        try:
            self._req("PATCH", f"/v1/accounts/{self.account_id}/positions/{int(ticket)}",
                      {"stopLoss": round(float(sl), digits), "takeProfit": round(float(tp or 0.0), digits)})
            self._acct_ts = 0.0
            return {"ok": True, "msg": "modified"}
        except TickerAllError as e:
            return {"ok": False, "msg": f"{e.code} {e.message}".strip()}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "msg": str(exc)}

    # --------------------------------------------------------------- stream
    def _start_stream(self) -> None:
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._ws_stop.clear()
        self._ws_thread = threading.Thread(target=self._ws_main, daemon=True, name="tickerall-stream")
        self._ws_thread.start()

    def _stop_stream(self) -> None:
        self._ws_stop.set()
        loop = self._ws_loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(lambda: None)
        if self._ws_thread:
            self._ws_thread.join(timeout=5)
        self._ws_thread = None
        self._ws_ok = False

    def _ws_subscribe(self, symbols: list) -> None:
        loop, q = self._ws_loop, self._ws_send_q
        if loop and q and self.account_id:
            msg = {"type": "subscribe", "channels": [{"kind": "ticks", "accountId": self.account_id,
                                                      "symbols": list(symbols)}]}
            loop.call_soon_threadsafe(q.put_nowait, msg)

    def _ws_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._ws_loop = loop
        try:
            loop.run_until_complete(self._ws_run())
        finally:
            self._ws_loop = None
            loop.close()

    async def _ws_run(self) -> None:
        import aiohttp
        backoff = 1.0
        while not self._ws_stop.is_set():
            self._ws_send_q = asyncio.Queue()
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.ws_connect(self.ws_url, headers={"Authorization": f"Bearer {self.api_key}"},
                                            heartbeat=20, autoping=True) as ws:
                        aid = self.account_id
                        chans = [{"kind": "positions", "accountId": aid}, {"kind": "account", "accountId": aid}]
                        if self._watched:
                            chans.insert(0, {"kind": "ticks", "accountId": aid, "symbols": sorted(self._watched)})
                        await ws.send_json({"type": "subscribe", "channels": chans, "correlationId": "rb"})
                        backoff = 1.0
                        last_ping = time.time()
                        while not self._ws_stop.is_set():
                            while not self._ws_send_q.empty():
                                await ws.send_json(self._ws_send_q.get_nowait())
                            if time.time() - last_ping > 20:
                                await ws.send_json({"type": "ping"})
                                last_ping = time.time()
                            try:
                                msg = await ws.receive(timeout=0.5)
                            except asyncio.TimeoutError:
                                continue
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                            aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                data = json.loads(msg.data)
                            except ValueError:
                                continue
                            self._on_stream(data)
            except Exception as exc:  # noqa: BLE001
                log.info("stream error: %s", exc)
            self._ws_ok = False
            if self._ws_stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def _on_stream(self, data: dict) -> None:
        t = data.get("type")
        if t == "tick":
            self._on_tick(data)
        elif t == "subscribed":
            self._ws_ok = True
            rej = data.get("rejected") or []
            if any((r or {}).get("code") == "BROKER_ACCOUNT_NOT_HOT" for r in rej):
                self._hot = False
                self._ws_ok = False
                if self._rearm():
                    self._ws_subscribe(sorted(self._watched))
            if rej:
                self.stream_events.append({"ts": time.time(), "rejected": rej})
        elif t in ("position_update", "account_update"):
            self._acct_ts = 0.0          # next read is fresh
        elif t == "event":
            ev = data.get("event") or {}
            self.stream_events.append({"ts": time.time(), "kind": ev.get("kind"), "title": ev.get("title")})
            if ev.get("kind") == "maintenance_clear":
                self._rearm()
