"""
VOD Failover for Dispatcharr.

Stock Dispatcharr only fails over between provider accounts while *choosing* one
(an account with no free slot is skipped). Once an account is chosen, an upstream
error while opening the stream (HTTP 458 "max connections", 404, timeout, ...) is
returned to the client as a 500 and the other account is never tried.

This plugin adds two hooks:

1. MultiWorkerVODConnectionManager.stream_content_with_session
   If stream setup fails with Dispatcharr's "Streaming error" 500, the failing
   relation (or, for connection-limit/timeout errors, the whole account for that
   title) is remembered in Redis and the request is retried on the next account
   that carries the title, within the same HTTP request.

2. views._order_candidates
   Remembered failures are skipped when Dispatcharr orders candidate streams, so
   later requests (new sessions, Streamcache probes, other workers) go straight to
   a working account. If every candidate is marked bad, nothing is filtered.

Originals are stored once on the patched objects and never overwritten, so plugin
reloads in Dispatcharr cannot leave a dangling reference.
"""
import logging
import re
import sys
import threading
import time
from urllib.parse import urlparse
from functools import wraps

logger = logging.getLogger(__name__)

ACTIVE_PLUGIN = None
KEY_PREFIX = "vod_failover:bad:"
DEFAULT_SETTINGS = {"enabled": True, "fail_ttl_minutes": 5, "limit_ttl_seconds": 60, "max_attempts": 4,
                    "preempt_idle_downloads": True, "streamcache_url": "http://127.0.0.1:8080",
                    "proxy_host": "127.0.0.1", "mirror_urls": ""}
PLUGIN_KEY = "vod_failover"
MIRROR_HELP = ("# One [section] per M3U Failover account, then its URLs in order of preference.\n"
               "# Base URLs only (scheme://host[:port]). Lines starting with # are ignored.\n")

_ORIG_ORDER_ATTR = "_vod_failover_orig_order_candidates"
_ORIG_STREAM_ATTR = "_vod_failover_orig_stream_content_with_session"
_ORIG_SELECT_ATTR = "_vod_failover_orig_select_vod_stream"


HOOKED_VIEWS = ("_order_candidates", "_select_vod_stream", "_content_type_for_obj")
HOOK_TRIES = 60
HOOK_RETRY_SECONDS = 1


def _views():
    mod = sys.modules.get("apps.proxy.vod_proxy.views")
    if mod is None:
        try:
            import apps.proxy.vod_proxy.views as mod
        except Exception:
            return None
    return mod


def _manager_cls():
    try:
        from apps.proxy.vod_proxy.multi_worker_connection_manager import MultiWorkerVODConnectionManager
        return MultiWorkerVODConnectionManager
    except Exception:
        return None


def _redis():
    try:
        from core.utils import RedisClient
        return RedisClient.get_client()
    except Exception:
        cls = _manager_cls()
        try:
            return cls.get_instance().redis_client if cls else None
        except Exception:
            return None


def _content_key(relation):
    movie_id = getattr(relation, "movie_id", None)
    if movie_id:
        return f"m{movie_id}"
    episode_id = getattr(relation, "episode_id", None)
    if episode_id:
        return f"e{episode_id}"
    return None


def _relation_keys(relation):
    """Redis keys that would mark this relation as bad."""
    keys = [f"{KEY_PREFIX}rel:{type(relation).__name__}:{relation.id}"]
    ck = _content_key(relation)
    if ck:
        keys.append(f"{KEY_PREFIX}acct:{relation.m3u_account_id}:{ck}")
    return keys


def _purge_failed_session(manager, session_id):
    """Dispatcharr keeps a failed session idle for ~1s so a reconnect can join it. A retry joins it
    within milliseconds and reuses the OLD (failed) provider URL, so delete it now. Uses Dispatcharr's
    own cleanup, which only deletes when no streams are active; the profile slot was already released
    by Dispatcharr's rollback, so no connection_manager is passed (avoids a double decrement)."""
    try:
        from apps.proxy.vod_proxy.multi_worker_connection_manager import RedisBackedVODConnection
        RedisBackedVODConnection(session_id, manager.redis_client).cleanup(
            current_worker_id=getattr(manager, "worker_id", None))
    except Exception as e:
        logger.debug("[VOD-Failover] session purge failed for %s: %s", session_id, e)


_FAILED_URL_RE = re.compile(r"for url: (\S+)")


def _failed_url(message, fallback):
    """The URL the provider actually rejected (may differ from the one we intended to use)."""
    m = _FAILED_URL_RE.search(message or "")
    return m.group(1).rstrip(".,") if m else fallback


def _is_setup_failure(response):
    """Dispatcharr returns HttpResponse('Streaming error: ...', 500) when opening upstream fails."""
    try:
        from django.http import StreamingHttpResponse
        if isinstance(response, StreamingHttpResponse):
            return False
        if getattr(response, "status_code", None) != 500:
            return False
        body = getattr(response, "content", b"") or b""
        return body.startswith(b"Streaming error")
    except Exception:
        return False


def _is_connection_limit_error(message):
    """Provider refused because it thinks the account's connections are in use (often lingering
    connections it hasn't released yet). Transient by nature."""
    m = (message or "").lower()
    # 460: one provider's answer while it still counts a dropped connection as in use
    return any(t in m for t in (" 458 ", " 460 ", " 429 ", "max connections", "too many"))


def _is_account_level_error(message):
    """Errors that mean 'this account can't serve right now' rather than 'this one stream is broken'."""
    m = (message or "").lower()
    if " 404 " in m or "not found" in m:
        return False
    return True  # 458/429/403/5xx/timeouts/connection errors


# ---------------------------------------------------------------- M3U Failover mirrors

MIRROR_FIELDS = [
    {
        "id": "mirrors",
        "label": "Mirror URLs (M3U Failover)",
        "type": "section",
        "collapsed": False,
        "description": "Only for accounts that use the M3U Failover proxy. The VOD failover above works without it."
    },
    {
        "id": "proxy_host",
        "section": "mirrors",
        "label": "M3U Failover address",
        "type": "string",
        "default": "127.0.0.1",
        "help_text": "Host of the M3U Failover proxy as seen from Dispatcharr. Accounts whose Server URL points at it are listed.",
        "description": "Host of the M3U Failover proxy as seen from Dispatcharr. Accounts whose Server URL points at it are listed."
    },
    {
        "id": "mirror_urls",
        "section": "mirrors",
        "label": "Mirror list",
        "type": "text",
        "rows": 12,
        "default": "",
        "placeholder": "Press Load Mirror List, then reopen this page.",
        "help_text": "Edit, press Save, then Apply Mirror List. One [section] per account, then its URLs in order of preference. Sections left out are not changed. The nightly sort may reorder them again.",
        "description": "Edit, press Save, then Apply Mirror List. One [section] per account, then its URLs in order of preference. Sections left out are not changed. The nightly sort may reorder them again."
    }
]
MIRROR_ACTIONS = [
    {
        "id": "mirror_status",
        "label": "Mirror Status",
        "description": "Show which mirror URL each proxied account uses, pauses, resting URLs and the last sort.",
        "button_label": "Show Status",
        "button_variant": "filled",
        "button_color": "blue"
    },
    {
        "id": "mirror_load",
        "label": "Load Mirror List",
        "description": "Copy the proxy's current URL lists into the Mirror list box. Reopen this page to see them.",
        "button_label": "Load",
        "button_variant": "outline",
        "button_color": "blue"
    },
    {
        "id": "mirror_apply",
        "label": "Apply Mirror List",
        "description": "Send the saved Mirror list to the proxy. It writes upstreams.conf and uses the new order at once.",
        "button_label": "Apply",
        "button_variant": "filled",
        "button_color": "orange"
    },
    {
        "id": "mirror_sort",
        "label": "Sort Mirrors Now",
        "description": "Run the nightly sort now: a few spaced logins per URL, then fastest and most reliable first.",
        "button_label": "Sort Now",
        "button_variant": "outline",
        "button_color": "blue"
    }
]


def _proxy_accounts(host):
    """[(dispatcharr account name, port)] for M3U accounts whose server URL points at the proxy."""
    from apps.m3u.models import M3UAccount
    hosts = {host.strip().lower()} | ({"127.0.0.1", "localhost"} if host.strip() in ("127.0.0.1", "localhost") else set())
    found = []
    for acct in M3UAccount.objects.order_by("name"):
        u = urlparse(acct.server_url or "")
        try:
            port = u.port
        except ValueError:
            continue
        if (u.hostname or "").lower() in hosts and port:
            found.append((acct.name, port))
    return found


def _proxy_call(host, port, path, payload=None, timeout=5):
    import json
    import urllib.error
    import urllib.request
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://{host}:{port}{path}", data=data, method="POST" if data else "GET",
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {"error": f"HTTP {e.code}"}


def _proxy_states(host):
    """[(dispatcharr name, port, status dict or None, error)] for every proxied account."""
    out = []
    for name, port in _proxy_accounts(host):
        try:
            code, st = _proxy_call(host, port, "/__status")
            out.append((name, port, st, None) if code == 200 and isinstance(st, dict) and "account" in st
                       else (name, port, None, f"HTTP {code}, not an M3U Failover port"))
        except Exception as e:
            out.append((name, port, None, f"not reachable ({e.__class__.__name__})"))
    return out


def _parse_mirror_text(text):
    """'[section]' then URLs -> {section: [urls]}; '#' comments and text after a URL are ignored."""
    sections, cur = {}, None
    for lineno, raw in enumerate((text or "").splitlines(), 1):
        line = raw.split("#", 1)[0].strip()  # a base URL never contains '#'
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            cur = line[1:-1].strip()
            if cur in sections:
                raise ValueError(f"line {lineno}: [{cur}] is listed twice")
            sections[cur] = []
        elif line.lower().startswith(("http://", "https://")):
            if cur is None:
                raise ValueError(f"line {lineno}: URL before any [section]")
            sections[cur].append(line.split()[0].rstrip("/"))
        else:
            raise ValueError(f"line {lineno}: expected [section] or an http(s) URL, got {line[:60]!r}")
    return sections


def _mirror_text(states):
    lines = [MIRROR_HELP]
    for name, port, st, err in states:
        if st and "urls" in st:
            lines.append(f"[{st['account']}]   # Dispatcharr account: {name}, port {port}")
            lines.extend(st.get("urls") or [])
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _when(seconds):
    seconds = int(seconds)
    return f"{seconds // 60}m {seconds % 60}s" if seconds >= 60 else f"{seconds}s"


def _mirror_report(states):
    if not states:
        return ("No M3U account points at the M3U Failover proxy. Set an account's Server URL to "
                "http://<proxy host>:<port> from upstreams.conf.")
    parts = []
    for name, port, st, err in states:
        if not st:
            parts.append(f"{name} (port {port}): {err}")
            continue
        bits = [f"{name} [{st['account']}]: using {st.get('preferred') or (st.get('upstreams') or ['-'])[0]}",
                f"{len(st.get('urls') or st.get('upstreams') or [])} URL(s)"]
        if st.get("paused_seconds"):
            bits.append(f"PAUSED {_when(st['paused_seconds'])} ({st.get('pause_reason') or 'repeated failures'})")
        if st.get("stuck_slot_seconds"):
            bits.append(f"stuck connection slot for {_when(st['stuck_slot_seconds'])}")
        cooling = [f["target"] for f in st.get("failures") or [] if f.get("cooling_down_seconds")]
        if cooling:
            bits.append("resting: " + ", ".join(cooling))
        if st.get("refused_feeds"):
            bits.append(f"{len(st['refused_feeds'])} live feed(s) refused")
        streams = st.get("active_streams") or []
        bits.append(f"{len(streams)} stream(s) playing")
        ls = st.get("last_sort")
        bits.append(f"last sort {ls['at']}: {ls['result']}" if ls else
                    "not sorted yet" + ("" if st.get("login_known") else " (login not seen yet)"))
        parts.append("; ".join(bits))
    return " | ".join(parts)


def _save_setting(key, value):
    """Write one setting without the save hooks; the row lock keeps a save from another worker."""
    from django.db import transaction
    from apps.plugins.models import PluginConfig
    with transaction.atomic():
        row = PluginConfig.objects.select_for_update().filter(key=key).first()
        if row is None:
            return False
        PluginConfig.objects.filter(pk=row.pk).update(settings={**(row.settings or {}), "mirror_urls": value})
    return True


class Plugin:
    name = "VOD Failover"
    version = "1.5"
    description = "Retries a VOD on the next provider account when the first one fails to start."
    author = "Brxdy22"

    fields = [
        {"id": "enabled", "label": "Enable failover", "type": "boolean", "default": True,
         "help_text": "Master switch.", "description": "Master switch."},
        {"id": "fail_ttl_minutes", "label": "Remember failures for (minutes)", "type": "number", "default": 5,
         "help_text": "How long a failed account/stream is skipped for that title (404s, timeouts, server errors).",
         "description": "How long a failed account/stream is skipped for that title (404s, timeouts, server errors)."},
        {"id": "limit_ttl_seconds", "label": "Remember connection-limit errors for (seconds)", "type": "number", "default": 60,
         "help_text": "458/429 'max connections' errors are usually the provider still counting a just-closed connection.",
         "description": "458/429 'max connections' errors are usually the provider still counting a just-closed connection."},
        {"id": "preempt_idle_downloads", "label": "Free busy accounts by cancelling unwatched Streamcache downloads",
         "type": "boolean", "default": True,
         "help_text": "When every account carrying a title is at capacity, cancel Streamcache background downloads that have no viewers, then retry.",
         "description": "When every account carrying a title is at capacity, cancel Streamcache background downloads that have no viewers, then retry."},
        {"id": "streamcache_url", "label": "Streamcache URL (as seen from Dispatcharr)", "type": "string",
         "default": "http://127.0.0.1:8080",
         "help_text": "Used only to list and cancel unwatched downloads.", "description": "Used only to list and cancel unwatched downloads."},
        {"id": "max_attempts", "label": "Max attempts per request", "type": "number", "default": 4,
         "help_text": "Total upstream attempts (including the first) per request.",
         "description": "Total upstream attempts (including the first) per request."},
    ] + MIRROR_FIELDS

    actions = [
        {"id": "status_check", "label": "Check Hook Status",
         "description": "Show whether the failover hooks are installed and how many failures are remembered.",
         "button_label": "Check Status", "button_variant": "filled", "button_color": "blue"},
        {"id": "clear_failures", "label": "Clear Remembered Failures",
         "description": "Forget all remembered failures so every account is tried again.",
         "button_label": "Clear", "button_variant": "outline", "button_color": "red"},
    ] + MIRROR_ACTIONS

    def __init__(self):
        global ACTIVE_PLUGIN
        ACTIVE_PLUGIN = self
        self._cached_settings = {}
        self._settings_cache = (0.0, {})
        self._install_when_ready()

    def _install_when_ready(self, tries=HOOK_TRIES):
        """Dispatcharr can load plugins while its VOD views are still importing. Hooking then
        would fail and leave this worker without failover until the next restart, so wait."""
        if ACTIVE_PLUGIN is not self:
            return  # replaced by a reload
        views = _views()
        if views is None or all(hasattr(views, name) for name in HOOKED_VIEWS):
            self._install_hooks()
        elif tries > 0:
            timer = threading.Timer(HOOK_RETRY_SECONDS, self._install_when_ready, args=(tries - 1,))
            timer.daemon = True
            timer.start()
        else:
            logger.error("[VOD-Failover] Dispatcharr VOD views never finished loading; hooks not installed.")

    # ------------------------------------------------------------------ settings

    def _config_row(self):
        try:
            from apps.plugins.models import PluginConfig
            if __name__.startswith("_dispatcharr_plugin_"):
                key = __name__[len("_dispatcharr_plugin_"):].split(".", 1)[0]
                row = PluginConfig.objects.filter(key=key).first()
                if row is not None:
                    return row
            if __name__.startswith("plugins."):
                row = PluginConfig.objects.filter(key=__name__.split(".", 1)[1]).first()
                if row is not None:
                    return row
            return (PluginConfig.objects.filter(key__iexact="vod_failover").first()
                    or PluginConfig.objects.filter(name__iexact="VOD Failover").first())
        except Exception:
            return None

    def _settings(self):
        """Settings + enabled flag, cached for 30s (read on every VOD request)."""
        ts, cached = self._settings_cache
        if time.time() - ts < 30 and cached:
            return cached
        merged = dict(DEFAULT_SETTINGS)
        merged.update(self._cached_settings)
        row = self._config_row()
        db_enabled = True
        if row is not None:
            db_enabled = bool(getattr(row, "enabled", True))
            if isinstance(getattr(row, "settings", None), dict):
                merged.update({k: v for k, v in row.settings.items() if v is not None})
        val = merged.get("enabled", True)
        if isinstance(val, str):
            val = val.lower() in ("true", "1", "yes")
        merged["_active"] = db_enabled and bool(val)
        self._settings_cache = (time.time(), merged)
        return merged

    def _active(self):
        return self._settings().get("_active", False)

    def _ttl_seconds(self, message=""):
        s = self._settings()
        try:
            if _is_connection_limit_error(message):
                return max(10, int(float(s.get("limit_ttl_seconds", 60))))
            return max(60, int(float(s.get("fail_ttl_minutes", 5)) * 60))
        except Exception:
            return 60 if _is_connection_limit_error(message) else 300

    def _max_attempts(self):
        try:
            return max(1, min(10, int(float(self._settings().get("max_attempts", 3)))))
        except Exception:
            return 3

    # ------------------------------------------------------------------ hooks

    def _install_hooks(self):
        views = _views()
        mgr = _manager_cls()
        if views is None or mgr is None:
            logger.warning("[VOD-Failover] Dispatcharr VOD modules not importable; hooks not installed.")
            return

        # Store originals exactly once; never replace them with a patched function or None.
        if not callable(getattr(views, _ORIG_ORDER_ATTR, None)):
            setattr(views, _ORIG_ORDER_ATTR, views._order_candidates)
        if not callable(getattr(mgr, _ORIG_STREAM_ATTR, None)):
            setattr(mgr, _ORIG_STREAM_ATTR, mgr.stream_content_with_session)

        if not callable(getattr(views, _ORIG_SELECT_ATTR, None)):
            setattr(views, _ORIG_SELECT_ATTR, views._select_vod_stream)

        orig_order = getattr(views, _ORIG_ORDER_ATTR)
        orig_stream = getattr(mgr, _ORIG_STREAM_ATTR)
        orig_select = getattr(views, _ORIG_SELECT_ATTR)

        @wraps(orig_select)
        def select_vod_stream(*args, **kwargs):
            selected = orig_select(*args, **kwargs)
            plugin = ACTIVE_PLUGIN
            if selected is not None or plugin is None or not plugin._active():
                return selected
            try:
                return plugin._select_after_preempt(orig_select, args, kwargs)
            except Exception as e:
                logger.warning("[VOD-Failover] preempt error (ignored): %s", e)
                return None

        @wraps(orig_order)
        def order_candidates(candidates, preferred_relation=None):
            ordered = orig_order(candidates, preferred_relation)
            plugin = ACTIVE_PLUGIN
            if plugin is None or len(ordered) < 2 or not plugin._active():
                return ordered
            try:
                r = _redis()
                if r is None:
                    return ordered
                keys_per = [_relation_keys(rel) for rel in ordered]
                flat = [k for ks in keys_per for k in ks]
                hits = r.mget(flat) if flat else []
                bad = {k for k, v in zip(flat, hits) if v}
                keep = [rel for rel, ks in zip(ordered, keys_per) if not any(k in bad for k in ks)]
                if keep and len(keep) != len(ordered):
                    logger.info("[VOD-Failover] Skipping %d remembered-bad stream(s); %d candidate(s) left",
                                len(ordered) - len(keep), len(keep))
                return keep or ordered
            except Exception as e:
                logger.warning("[VOD-Failover] candidate filter error (ignored): %s", e)
                return ordered

        @wraps(orig_stream)
        def stream_content_with_session(self, session_id, content_obj, stream_url, m3u_profile, *args, **kwargs):
            response = orig_stream(self, session_id, content_obj, stream_url, m3u_profile, *args, **kwargs)
            plugin = ACTIVE_PLUGIN
            if not _is_setup_failure(response):
                return response
            _purge_failed_session(self, session_id)
            if plugin is None or not plugin._active():
                return response
            try:
                return plugin._failover(orig_stream, self, response, session_id, content_obj,
                                        stream_url, m3u_profile, args, kwargs)
            except Exception as e:
                logger.error("[VOD-Failover] failover error, returning original failure: %s", e, exc_info=True)
                return response

        order_candidates._vod_failover = True
        stream_content_with_session._vod_failover = True
        select_vod_stream._vod_failover = True
        views._select_vod_stream = select_vod_stream
        views._order_candidates = order_candidates
        mgr.stream_content_with_session = stream_content_with_session
        logger.info("[VOD-Failover] Hooks installed (candidate filter + setup-failure retry + idle-download preempt).")

    def _failover(self, orig_stream, manager, response, session_id, content_obj, stream_url, m3u_profile, args, kwargs):
        views = _views()
        content_type = views._content_type_for_obj(content_obj)
        content_uuid = str(content_obj.uuid)
        name = getattr(content_obj, "name", content_uuid)
        tried = {stream_url}
        attempts = 1
        loaded_missing = False

        while True:
            message = (getattr(response, "content", b"") or b"").decode("utf-8", "replace")
            self._remember_failure(views, content_type, content_uuid, stream_url, m3u_profile, message)

            if attempts >= self._max_attempts():
                logger.warning("[VOD-Failover] %s: giving up after %d attempt(s)", name, attempts)
                return response

            selected = views._select_vod_stream(content_type, content_uuid)
            if (not selected or selected["final_stream_url"] in tried) and not loaded_missing:
                # Dispatcharr loads a series' episode list from ONE account on demand, so another
                # account may carry this episode without Dispatcharr knowing yet. Load those lists
                # now (provider API calls only, no stream slots) and look again.
                loaded_missing = True
                if self._load_missing_episode_lists(content_obj, content_type):
                    selected = views._select_vod_stream(content_type, content_uuid)
            if not selected or selected["final_stream_url"] in tried:
                logger.warning("[VOD-Failover] %s: no other account available after %d attempt(s)", name, attempts)
                return response

            stream_url = selected["final_stream_url"]
            m3u_profile = selected["m3u_profile"]
            tried.add(stream_url)
            attempts += 1
            logger.info("[VOD-Failover] %s: retrying on account %s (attempt %d)",
                        name, selected["m3u_account"].name, attempts)

            try:
                from django.db import close_old_connections
                close_old_connections()
            except Exception:
                pass

            response = orig_stream(manager, session_id, content_obj, stream_url, m3u_profile, *args, **kwargs)
            if not _is_setup_failure(response):
                logger.info("[VOD-Failover] %s: playing from account %s", name, selected["m3u_account"].name)
                return response
            _purge_failed_session(manager, session_id)

    def _select_after_preempt(self, orig_select, args, kwargs):
        """Every candidate account is at capacity. If Streamcache holds slots with downloads nobody
        is watching, cancel them, wait for Dispatcharr to release the slot, and select again."""
        st = self._settings()
        val = st.get("preempt_idle_downloads", True)
        if isinstance(val, str):
            val = val.lower() in ("true", "1", "yes")
        if not val:
            return None
        r = _redis()
        # At most one preempt sweep every 5s across all workers (Emby fires bursts of requests).
        if r is not None and not r.set(f"{KEY_PREFIX}preempt-lock", "1", nx=True, ex=5):
            time.sleep(1.5)
            return orig_select(*args, **kwargs)
        cancelled = self._cancel_idle_downloads(str(st.get("streamcache_url") or "http://127.0.0.1:8080"))
        if not cancelled:
            return None
        for _ in range(8):  # up to ~4s for the cancelled upstream connection to be released
            time.sleep(0.5)
            selected = orig_select(*args, **kwargs)
            if selected is not None:
                logger.info("[VOD-Failover] Freed %s by cancelling %d unwatched Streamcache download(s)",
                            selected["m3u_account"].name, cancelled)
                return selected
        logger.warning("[VOD-Failover] Cancelled %d unwatched download(s) but no slot was released in time", cancelled)
        return None

    @staticmethod
    def _cancel_idle_downloads(base_url):
        import json
        import urllib.parse
        import urllib.request
        base = base_url.rstrip("/")
        try:
            with urllib.request.urlopen(f"{base}/debug/state", timeout=3) as resp:
                state = json.load(resp)
        except Exception as e:
            logger.debug("[VOD-Failover] Streamcache not reachable at %s: %s", base, e)
            return 0
        cancelled = 0
        for f in state.get("active_downloads") or []:
            total, done = f.get("total_size") or 0, f.get("downloaded_bytes") or 0
            if not f.get("active_writer") or (f.get("active_clients") or 0) > 0 or (total and done >= total):
                continue
            key = f.get("key")
            try:
                req = urllib.request.Request(f"{base}/api/hydration/cancel?key={urllib.parse.quote(key)}",
                                             data=b"", method="POST")
                urllib.request.urlopen(req, timeout=3).close()
                cancelled += 1
                logger.info("[VOD-Failover] Cancelled unwatched Streamcache download %s (%s)",
                            key, (f.get("url") or "").split("/proxy/vod/")[-1])
            except Exception as e:
                logger.warning("[VOD-Failover] could not cancel %s: %s", key, e)
        return cancelled

    def _load_missing_episode_lists(self, content_obj, content_type):
        """For an episode, fetch the series' episode list from every other active account that has
        the series but no copy of this episode yet. Returns True if any list was (re)loaded."""
        if content_type != "episode":
            return False
        try:
            from django.utils import timezone
            from apps.vod.models import M3USeriesRelation, M3UEpisodeRelation
            from apps.vod.tasks import refresh_series_episodes
        except Exception as e:
            logger.warning("[VOD-Failover] cannot load episode lists: %s", e)
            return False

        series = getattr(content_obj, "series", None)
        if series is None:
            return False
        have = set(M3UEpisodeRelation.objects.filter(episode=content_obj)
                   .values_list("m3u_account_id", flat=True))
        r = _redis()
        loaded = False
        relations = (M3USeriesRelation.objects
                     .filter(series=series, m3u_account__is_active=True)
                     .exclude(m3u_account_id__in=have)
                     .select_related("m3u_account"))
        for rel in relations:
            # Don't hit the same provider for the same series more than once per 30 min.
            guard = f"{KEY_PREFIX}loaded:{rel.m3u_account_id}:{rel.external_series_id}"
            if r is not None and not r.set(guard, "1", nx=True, ex=1800):
                continue
            if rel.last_episode_refresh and timezone.now() - rel.last_episode_refresh < timezone.timedelta(minutes=30):
                continue
            try:
                started = time.time()
                refresh_series_episodes(rel.m3u_account, series, rel.external_series_id)
                loaded = True
                logger.info("[VOD-Failover] %s: loaded %s episode list from %s (%.1fs)",
                            getattr(series, "name", "?"), rel.external_series_id, rel.m3u_account.name,
                            time.time() - started)
            except Exception as e:
                logger.warning("[VOD-Failover] loading episodes from %s failed: %s", rel.m3u_account.name, e)
        return loaded

    def _remember_failure(self, views, content_type, content_uuid, stream_url, m3u_profile, message):
        r = _redis()
        if r is None:
            return
        ttl = self._ttl_seconds(message)
        relation = None
        failed_url = _failed_url(message, stream_url)
        try:
            _obj, _rel, candidates = views._get_content_and_relation(content_type, content_uuid)
            for cand in candidates or []:
                if cand.m3u_account_id == m3u_profile.m3u_account_id and \
                        views._build_vod_stream_url(cand, m3u_profile, content_type) == failed_url:
                    relation = cand
                    break
            if relation is None:
                # The provider rejected a different URL than intended (e.g. a reused session):
                # match by provider host + stream id in the rejected URL.
                fu = urlparse(failed_url)
                leaf = fu.path.rsplit("/", 1)[-1].split(".", 1)[0]
                for cand in candidates or []:
                    su = urlparse(getattr(cand.m3u_account, "server_url", "") or "")
                    # host AND port: several accounts can share 127.0.0.1 behind m3u-failover
                    if str(cand.stream_id) == leaf and su.hostname and \
                            (su.hostname, su.port) == (fu.hostname, fu.port):
                        relation = cand
                        m3u_profile = None
                        break
        except Exception as e:
            logger.debug("[VOD-Failover] relation lookup failed: %s", e)

        account_level = _is_account_level_error(message)
        account_name = (relation.m3u_account.name if relation is not None
                        else getattr(getattr(m3u_profile, "m3u_account", None), "name", "?"))
        if relation is not None:
            keys = _relation_keys(relation)
            key = keys[1] if (account_level and len(keys) > 1) else keys[0]
        else:
            prefix = "m" if content_type == "movie" else "e"
            try:
                from apps.vod.models import Movie, Episode
                model = Movie if content_type == "movie" else Episode
                pk = model.objects.filter(uuid=content_uuid).values_list("id", flat=True).first()
            except Exception:
                pk = None
            if pk is None:
                return
            if m3u_profile is None:
                return
            key = f"{KEY_PREFIX}acct:{m3u_profile.m3u_account_id}:{prefix}{pk}"

        r.setex(key, ttl, message[:200] or "failed")
        logger.info("[VOD-Failover] Marked %s on account %s as bad for %ds (%s): %s",
                    "account" if ":acct:" in key else "stream", account_name, ttl,
                    content_type, message[:120])

    # ------------------------------------------------------------------ lifecycle / actions

    def stop(self, context=None):
        global ACTIVE_PLUGIN
        views = _views()
        mgr = _manager_cls()
        if views is not None and callable(getattr(views, _ORIG_ORDER_ATTR, None)):
            views._order_candidates = getattr(views, _ORIG_ORDER_ATTR)
        if views is not None and callable(getattr(views, _ORIG_SELECT_ATTR, None)):
            views._select_vod_stream = getattr(views, _ORIG_SELECT_ATTR)
        if mgr is not None and callable(getattr(mgr, _ORIG_STREAM_ATTR, None)):
            mgr.stream_content_with_session = getattr(mgr, _ORIG_STREAM_ATTR)
        if ACTIVE_PLUGIN is self:
            ACTIVE_PLUGIN = None
        logger.info("[VOD-Failover] Hooks removed (context=%s).", context)

    def run(self, action, params=None, context=None):
        if context and isinstance(context.get("settings"), dict):
            self._cached_settings.update(context["settings"])
            self._settings_cache = (0.0, {})

        if action == "status_check":
            views = _views()
            mgr = _manager_cls()
            hooked = bool(views and getattr(views._order_candidates, "_vod_failover", False)
                          and mgr and getattr(mgr.stream_content_with_session, "_vod_failover", False))
            count = 0
            r = _redis()
            if r is not None:
                try:
                    count = sum(1 for _ in r.scan_iter(f"{KEY_PREFIX}*", count=500))
                except Exception:
                    pass
            msg = (f"Failover is {'ACTIVE' if (hooked and self._active()) else 'INACTIVE'} in this worker; "
                   f"{count} failure(s) remembered.")
            return {"status": "ok", "hook_installed": hooked, "is_enabled": self._active(),
                    "remembered_failures": count, "message": msg}

        if action == "clear_failures":
            r = _redis()
            removed = 0
            if r is not None:
                for k in list(r.scan_iter(f"{KEY_PREFIX}*", count=500)):
                    removed += r.delete(k)
            return {"status": "ok", "message": f"Cleared {removed} remembered failure(s)."}

        if action in ("mirror_status", "mirror_load", "mirror_apply", "mirror_sort"):
            try:
                return self._mirror_action(action)
            except Exception as e:
                logger.error("[VOD-Failover] %s failed: %s", action, e, exc_info=True)
                return {"status": "error", "message": f"{action} failed: {e}"}

        return {"status": "error", "message": f"Unknown action: {action}"}

    def _fresh_settings(self):
        """Saved settings straight from the database (the 30s cache could be older than a Save)."""
        self._settings_cache = (0.0, {})
        return self._settings()

    def _mirror_action(self, action):
        s = self._fresh_settings()
        host = str(s.get("proxy_host") or "127.0.0.1").strip()
        states = _proxy_states(host)
        live = [x for x in states if x[2]]

        if action == "mirror_status":
            return {"status": "ok", "message": _mirror_report(states)}

        if not live:
            return {"status": "error", "message": _mirror_report(states)}

        if action == "mirror_sort":
            name, port = live[0][0], live[0][1]
            code, body = _proxy_call(host, port, "/__sort")
            if code == 202:
                return {"status": "ok", "message": "Sort started. It takes a few minutes; press Mirror Status "
                                                   "afterwards. Accounts whose login the proxy hasn't seen yet are skipped."}
            if code == 409:
                return {"status": "ok", "message": "A sort is already running."}
            return {"status": "error", "message": f"Sort not started: {body.get('error') or f'HTTP {code}'}"}

        if action == "mirror_load":
            if not any("urls" in st for _, _, st, _ in live):
                return {"status": "error", "message": "The M3U Failover proxy is older than 1.3 and doesn't "
                                                      "share its full URLs; update it first."}
            row = self._config_row()
            if row is None or not _save_setting(row.key, _mirror_text(live)):
                return {"status": "error", "message": "Plugin settings not found."}
            return {"status": "ok", "message": f"Loaded the URL lists of {len(live)} account(s). Reopen or refresh "
                                               "this page to see them in Mirror list."}

        # mirror_apply
        text = s.get("mirror_urls") or ""
        try:
            wanted = _parse_mirror_text(text)
        except ValueError as e:
            return {"status": "error", "message": f"Mirror list not applied: {e}"}
        if not wanted:
            return {"status": "error", "message": "Mirror list is empty. Press Load Mirror List first, "
                                                  "edit it, press Save, then Apply."}
        by_section = {st["account"]: (name, port, st) for name, port, st, err in live}
        unknown = [sec for sec in wanted if sec not in by_section]
        if unknown:
            return {"status": "error", "message": "Mirror list not applied: no proxied account named "
                                                  + ", ".join(f"[{u}]" for u in unknown)
                                                  + f". Known: {', '.join(f'[{k}]' for k in by_section)}."}
        empty = [sec for sec, urls in wanted.items() if not urls]
        if empty:
            return {"status": "error", "message": "Mirror list not applied: every account needs at least one URL ("
                                                  + ", ".join(f"[{e}]" for e in empty) + " has none)."}
        done, same, failed = [], [], []
        for sec, urls in wanted.items():
            name, port, st = by_section[sec]
            if "urls" not in st:  # before 1.3 the proxy would forward /__urls to the provider
                failed.append(f"{name}: this M3U Failover version can't edit URLs; update it to 1.3 or later")
                continue
            if urls == st["urls"]:
                same.append(name)
                continue
            code, body = _proxy_call(host, port, "/__urls", {"urls": urls})
            if code == 200:
                done.append(f"{name} ({len(urls)} URL(s))")
                logger.info("[VOD-Failover] mirror list for %s set to %d URL(s)", name, len(urls))
            else:
                failed.append(f"{name}: {body.get('error') or f'HTTP {code}'}")
        msg = []
        if done:
            msg.append("Applied: " + ", ".join(done) + ".")
        if same:
            msg.append("Unchanged: " + ", ".join(same) + ".")
        if failed:
            msg.append("Not applied: " + "; ".join(failed) + ".")
        return {"status": "error" if failed and not done else "ok", "message": " ".join(msg)}
