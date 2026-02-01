# main.py — OuiCestFait (Option A — rapidité/volume/fiabilité)
# - Tarification IDF vs Hors IDF
# - Hors IDF: distance auto (gratuit) + péage estimé (gratuit) => prix total sans demander le péage au client
# - Anti-doublon (message_id + soft dedup), rate limiting, DB WAL/busy_timeout
# - Parsing payload ultra tolérant (Make/WhatsApp/Meta)
# - Base départ: Mormant 77720
# - Urgent keywords => délai "sous 1h" sinon "dans la journée"
#
# Dépendances (requirements.txt): fastapi, uvicorn, pydantic, python-multipart (si form-data)
# :contentReference[oaicite:1]{index=1}

from __future__ import annotations

import os
import re
import json
import time
import uuid
import sqlite3
import hashlib
import asyncio
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List, Tuple, Union

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

# Compat Pydantic v1/v2
try:
    from pydantic import ConfigDict  # type: ignore
except Exception:
    ConfigDict = None  # type: ignore


# =========================
# Config (ENV)
# =========================

APP_NAME = os.getenv("APP_NAME", "OuiCestFait")
DB_PATH = os.getenv("DB_PATH", "ouicestfait.db")

# Si vide => pas de contrôle. Si renseigné => header X-OCF-TOKEN obligatoire.
OCF_TOKEN = os.getenv("OCF_TOKEN", "").strip()

# Buffer (regroupe messages proches). 0 => immédiat (recommandé volume).
CLIENT_BUFFER_WINDOW_SEC = int(os.getenv("CLIENT_BUFFER_WINDOW_SEC", "0"))

# Anti-boucle si tu connais ton propre numéro WhatsApp (sans + ni espaces)
WHATSAPP_OWN_NUMBER = os.getenv("WHATSAPP_OWN_NUMBER", "").strip()

# Debug (logs DB)
DEBUG = os.getenv("DEBUG", "0") == "1"
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "").strip()  # protège endpoints debug/admin si souhaité

# Déduplication
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "3600"))
SOFT_DEDUP_WINDOW_SEC = int(os.getenv("SOFT_DEDUP_WINDOW_SEC", "12"))

# Rate limit (volume/fiabilité)
RATE_LIMIT_CLIENT_PER_MIN = int(os.getenv("RATE_LIMIT_CLIENT_PER_MIN", "30"))
RATE_LIMIT_GLOBAL_PER_MIN = int(os.getenv("RATE_LIMIT_GLOBAL_PER_MIN", "800"))

# Base départ
BASE_CITY = os.getenv("BASE_CITY", "Mormant")
BASE_POSTAL = os.getenv("BASE_POSTAL", "77720")
BASE_LOCATION = f"{BASE_CITY} {BASE_POSTAL}"

# Urgent (règle validée)
URGENT_KEYWORDS = [
    "urgent", "rapidement", "tout de suite", "dans l’heure", "dans l'heure",
    "aujourd’hui", "aujourd'hui", "ce soir", "immédiat", "immediat"
]
DELAI_URGENT = "sous 1h"
DELAI_STANDARD = "dans la journée"

# IDF (départements + quelques villes)
IDF_DEPTS = {"75", "77", "78", "91", "92", "93", "94", "95"}
IDF_CITIES = {
    "paris", "mormant", "melun", "meaux", "chelles", "torcy", "lognes", "serris", "chessy",
    "versailles", "cergy", "pontoise", "argenteuil", "sarcelles",
    "nanterre", "courbevoie", "montreuil", "saint-denis", "saint denis", "aubervilliers",
    "vitry-sur-seine", "vitry sur seine", "creteil", "créteil", "evry", "évry", "massy",
}

# Tarifs IDF (configurables)
IDF_STANDARD_EUR = Decimal(os.getenv("IDF_STANDARD_EUR", "80"))
IDF_URGENT_EUR = Decimal(os.getenv("IDF_URGENT_EUR", "150"))
MIN_PRICE_EUR = Decimal(os.getenv("MIN_PRICE_EUR", "25"))

# Hors IDF (règle validée)
# base = (1€ * km) + (2 * péage) + 30€ repas ; total = base * (1 + 40%)
PROV_EUR_PER_KM = Decimal(os.getenv("PROV_EUR_PER_KM", "1.0"))
PROV_TOLL_MULT = Decimal(os.getenv("PROV_TOLL_MULT", "2.0"))
PROV_MEAL_EUR = Decimal(os.getenv("PROV_MEAL_EUR", "30"))
PROV_MARGIN_PCT = Decimal(os.getenv("PROV_MARGIN_PCT", "0.40"))  # +40%

# Suppléments Province (optionnels) — validés
PROV_AR_PCT = Decimal(os.getenv("PROV_AR_PCT", "0.50"))        # +50%
PROV_NIGHT_PCT = Decimal(os.getenv("PROV_NIGHT_PCT", "0.30"))  # +30%
PROV_WEEKEND_PCT = Decimal(os.getenv("PROV_WEEKEND_PCT", "0.20"))  # +20%
PROV_BULKY_MIN_EUR = Decimal(os.getenv("PROV_BULKY_MIN_EUR", "20"))
PROV_BULKY_MAX_EUR = Decimal(os.getenv("PROV_BULKY_MAX_EUR", "60"))

# Distance gratuite (avec limites)
GEO_ENABLE = os.getenv("GEO_ENABLE", "1").strip() not in ("0", "false", "False", "no", "NO")
GEO_COUNTRYCODES = os.getenv("GEO_COUNTRYCODES", "fr").strip()
NOMINATIM_BASE_URL = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org").rstrip("/")
OSRM_BASE_URL = os.getenv("OSRM_BASE_URL", "https://router.project-osrm.org").rstrip("/")
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "").strip()  # recommandé
GEO_TIMEOUT_SEC = float(os.getenv("GEO_TIMEOUT_SEC", "8"))
NOMINATIM_MIN_INTERVAL_SEC = float(os.getenv("NOMINATIM_MIN_INTERVAL_SEC", "1.1"))  # ~1 req/s
GEO_CACHE_TTL_DAYS = int(os.getenv("GEO_CACHE_TTL_DAYS", "30"))
ROUTE_CACHE_TTL_DAYS = int(os.getenv("ROUTE_CACHE_TTL_DAYS", "30"))

# Péage estimé (gratuit) — ne jamais demander au client
TOLL_EST_ENABLE = os.getenv("TOLL_EST_ENABLE", "1").strip() not in ("0", "false", "False", "no", "NO")
TOLL_EUR_PER_KM_CAR = float(os.getenv("TOLL_EUR_PER_KM_CAR", "0.13"))
TOLL_EUR_PER_KM_VAN = float(os.getenv("TOLL_EUR_PER_KM_VAN", "0.20"))
TOLL_SHARE_SHORT = float(os.getenv("TOLL_SHARE_SHORT", "0.15"))   # < 60 km
TOLL_SHARE_MED = float(os.getenv("TOLL_SHARE_MED", "0.50"))       # 60-120 km
TOLL_SHARE_LONG = float(os.getenv("TOLL_SHARE_LONG", "0.85"))     # > 120 km
TOLL_DISTANCE_SHORT_KM = float(os.getenv("TOLL_DISTANCE_SHORT_KM", "60"))
TOLL_DISTANCE_LONG_KM = float(os.getenv("TOLL_DISTANCE_LONG_KM", "120"))
TOLL_MAX_EUR = float(os.getenv("TOLL_MAX_EUR", "250"))

CURRENCY_ROUND = Decimal("0.01")


# =========================
# App
# =========================

app = FastAPI(title=f"{APP_NAME} API", version="2.2.0")


# =========================
# Pydantic models (compat Make)
# =========================

class DemandeMission(BaseModel):
    message_id: Optional[str] = Field(None)
    client_id: Optional[str] = Field(None)
    texte: Optional[str] = Field(None)

    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")  # pydantic v2
    else:
        class Config:
            extra = "allow"


class MissionOut(BaseModel):
    mission_id: int
    client_id: str
    resume_client: str
    prix_recommande_eur: Optional[float] = None
    delai_estime: str
    conditions: Optional[str] = None
    zone_tarifaire: Optional[str] = None
    type_detecte: Optional[str] = None
    statut: str
    ref_mission: Optional[str] = None  # ajout utile (ne casse pas Make)


# =========================
# SQLite helpers (WAL + indices)
# =========================

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ts() -> int:
    return int(time.time())


def init_db() -> None:
    conn = _db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS missions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ref TEXT NOT NULL,
        client_id TEXT NOT NULL,
        message_id TEXT,
        raw_request TEXT NOT NULL,
        merged_request TEXT NOT NULL,
        resume_client TEXT,
        type_detecte TEXT,
        zone_tarifaire TEXT,
        delai_estime TEXT,
        prix_recommande_eur REAL,
        conditions TEXT,
        origin TEXT,
        destination TEXT,
        distance_km REAL,
        toll_est_eur REAL,
        statut TEXT NOT NULL DEFAULT 'en_attente_validation',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS message_dedup (
        message_id TEXT PRIMARY KEY,
        client_id TEXT,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedup_created ON message_dedup(created_at)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS soft_dedup (
        k TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_soft_created ON soft_dedup(created_at)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_message_buffer (
        client_id TEXT PRIMARY KEY,
        buffer_text TEXT NOT NULL,
        first_at INTEGER NOT NULL,
        last_at INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rate_limit (
        scope TEXT NOT NULL,
        k TEXT NOT NULL,
        bucket INTEGER NOT NULL,
        cnt INTEGER NOT NULL,
        PRIMARY KEY(scope, k, bucket)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rl_bucket ON rate_limit(bucket)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS kv (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS geo_cache (
        address TEXT PRIMARY KEY,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        display_name TEXT,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_geo_created ON geo_cache(created_at)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS route_cache (
        k TEXT PRIMARY KEY,
        distance_m REAL NOT NULL,
        duration_s REAL,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_route_created ON route_cache(created_at)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_missions_client ON missions(client_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_missions_created ON missions(created_at)")

    conn.commit()
    conn.close()


init_db()


# =========================
# Security / Debug / Logging
# =========================

def require_token(request: Request) -> None:
    if not OCF_TOKEN:
        return
    token = request.headers.get("x-ocf-token", "")
    if token != OCF_TOKEN:
        raise HTTPException(status_code=401, detail="X-OCF-TOKEN invalid")


def _debug_allowed(token: str) -> bool:
    if not DEBUG_TOKEN:
        return True
    return token == DEBUG_TOKEN


def _log(kind: str, payload: Dict[str, Any]) -> None:
    if not DEBUG:
        return
    try:
        conn = _db()
        conn.execute(
            "INSERT INTO audit_log(kind, payload, created_at) VALUES(?,?,?)",
            (kind, json.dumps(payload, ensure_ascii=False), utc_now_iso()),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# =========================
# Rate limiting (SQLite bucket)
# =========================

def _rate_limit(scope: str, key: str, limit_per_min: int) -> bool:
    if limit_per_min <= 0:
        return True
    now = _now_ts()
    bucket = now // 60

    conn = _db()
    cur = conn.cursor()
    cur.execute("DELETE FROM rate_limit WHERE bucket < ?", (bucket - 10,))

    cur.execute(
        """
        INSERT INTO rate_limit(scope, k, bucket, cnt) VALUES(?,?,?,1)
        ON CONFLICT(scope, k, bucket) DO UPDATE SET cnt = cnt + 1
        """,
        (scope, key, bucket)
    )
    cur.execute("SELECT cnt FROM rate_limit WHERE scope=? AND k=? AND bucket=? LIMIT 1", (scope, key, bucket))
    row = cur.fetchone()
    conn.commit()
    conn.close()

    cnt = int(row["cnt"]) if row else 1
    return cnt <= limit_per_min


def rate_limit_or_429(client_id: str) -> None:
    if not _rate_limit("global", "all", RATE_LIMIT_GLOBAL_PER_MIN):
        raise HTTPException(status_code=429, detail="Rate limit global atteint. Réessaie dans 60s.")
    if not _rate_limit("client", client_id, RATE_LIMIT_CLIENT_PER_MIN):
        raise HTTPException(status_code=429, detail="Trop de demandes. Réessaie dans 60s.")


# =========================
# Parsing payload (Make/WhatsApp/Meta)
# =========================

def normalize_client_id(client_id: str) -> str:
    x = (client_id or "").strip()
    return x.replace(" ", "").replace("+", "")


def safe_round_eur(value: Decimal) -> Decimal:
    return value.quantize(CURRENCY_ROUND, rounding=ROUND_HALF_UP)


def _norm_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (k or "").lower())


def _get_any_fuzzy(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    idx = {_norm_key(str(k)): k for k in d.keys()}
    for k in keys:
        nk = _norm_key(k)
        if nk in idx:
            v = d[idx[nk]]
            if v not in (None, "", [], {}):
                return v
    return None


def _deep_get(obj: Any, path: List[Union[str, int]]) -> Optional[Any]:
    cur = obj
    try:
        for p in path:
            if isinstance(p, int):
                if not isinstance(cur, list) or len(cur) <= p:
                    return None
                cur = cur[p]
            else:
                if not isinstance(cur, dict):
                    return None
                if p in cur:
                    cur = cur[p]
                else:
                    nk = _norm_key(p)
                    found = None
                    for kk in cur.keys():
                        if _norm_key(str(kk)) == nk:
                            found = kk
                            break
                    if found is None:
                        return None
                    cur = cur[found]
        if cur in (None, "", [], {}):
            return None
        return cur
    except Exception:
        return None


def _first_in_list(x: Any) -> Optional[Any]:
    if isinstance(x, list) and len(x) > 0:
        return x[0]
    return None


def resolve_payload(payload: DemandeMission) -> Tuple[str, Optional[str], str, Dict[str, Any]]:
    raw = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()

    msg_id: Optional[Any] = payload.message_id or _get_any_fuzzy(raw, "message_id", "messageId", "id", "Message ID")
    cid: Optional[Any] = payload.client_id or _get_any_fuzzy(raw, "client_id", "clientId", "from", "From", "wa_id", "waId", "sender", "Sender")
    txt: Optional[Any] = payload.texte or _get_any_fuzzy(raw, "texte", "text", "Text", "body", "Body", "Body content", "content", "message")

    if isinstance(txt, dict):
        txt = _get_any_fuzzy(txt, "body", "Body", "text", "Text")

    # wrappers fréquents
    for wrapper_key in ("data", "payload", "body", "request", "input"):
        w = _get_any_fuzzy(raw, wrapper_key)
        if isinstance(w, dict):
            if not msg_id:
                msg_id = _get_any_fuzzy(w, "message_id", "messageId", "id", "Message ID")
            if not cid:
                cid = _get_any_fuzzy(w, "client_id", "from", "From", "wa_id", "sender", "Sender")
            if (not txt or str(txt).strip() == ""):
                t2 = _get_any_fuzzy(w, "texte", "text", "body", "Body", "Body content", "message", "content")
                if isinstance(t2, dict):
                    t2 = _get_any_fuzzy(t2, "body", "Body")
                txt = t2 or txt

    # Make Messages[]
    messages = _get_any_fuzzy(raw, "Messages", "messages")
    m0 = _first_in_list(messages)
    if isinstance(m0, dict):
        if not msg_id:
            msg_id = _get_any_fuzzy(m0, "id", "message_id", "messageId", "Message ID")
        if not cid:
            cid = _get_any_fuzzy(m0, "from", "From", "wa_id", "sender", "Sender", "client_id")
        if (not txt or str(txt).strip() == ""):
            txt = (
                _deep_get(m0, ["Text", "Body"])
                or _deep_get(m0, ["text", "body"])
                or _get_any_fuzzy(m0, "Body", "body", "text", "Text", "Body content")
                or ""
            )

    # Webhook Meta (entry/changes/value/messages)
    if not cid:
        cid = (
            _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "from"])
            or _deep_get(raw, ["entry", 0, "changes", 0, "value", "contacts", 0, "wa_id"])
        )
    if not msg_id:
        msg_id = _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "id"])
    if (not txt or str(txt).strip() == ""):
        txt = _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "text", "body"]) or ""

    msg_id = (str(msg_id).strip() if msg_id else None) or None
    cid = normalize_client_id(str(cid)) if cid else ""
    txt = (str(txt).strip() if txt else "")

    diag = {
        "has_message_id": bool(msg_id),
        "has_client_id": bool(cid),
        "has_text": bool(txt),
        "buffer_window": CLIENT_BUFFER_WINDOW_SEC,
    }
    return cid, msg_id, txt, diag


# =========================
# Détection (urgent / IDF / type)
# =========================

def is_urgent(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in URGENT_KEYWORDS)


def _contains_idf_postal_or_dept(text: str) -> bool:
    t = (text or "").lower()

    # "75" etc
    if re.search(r"\b(75|77|78|91|92|93|94|95)\b", t):
        return True

    # CP 75xxx etc
    if re.search(r"\b(75|77|78|91|92|93|94|95)\d{3}\b", t):
        return True

    return False


def is_idf_text(text: str) -> bool:
    t = (text or "").lower()
    if _contains_idf_postal_or_dept(t):
        return True
    for c in IDF_CITIES:
        if c in t:
            return True
    # mots-clés larges
    if any(k in t for k in ["idf", "ile de france", "île de france", "ile-de-france", "île-de-france"]):
        return True
    return False


def detect_type(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["livraison", "colis", "document", "dossier", "remettre", "déposer", "deposer", "récupérer", "recuperer"]):
        return "livraison"
    if any(k in t for k in ["courses", "supermarché", "supermarche", "acheter", "liste de courses"]):
        return "courses"
    if any(k in t for k in ["transport", "déménagement", "demenagement", "véhicule", "vehicule", "conduire", "van"]):
        return "transport"
    return "conciergerie"


# =========================
# Extraction trajet (origin/destination)
# =========================

def extract_route(text: str) -> Tuple[str, str]:
    """
    Retourne (origin, destination).
    Si ambigu: origin = BASE_LOCATION, destination = lieu trouvé ou texte.
    """
    s = (text or "").strip()

    # X -> Y
    m = re.search(r"([A-Za-zÀ-ÿ0-9'\- ,]+)\s*(?:->|→)\s*([A-Za-zÀ-ÿ0-9'\- ,]+)", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # de X à Y
    m = re.search(r"(?:de|depuis)\s+(.+?)\s+(?:à|a|vers)\s+(.+)", s, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # récupérer à X ... livrer à Y
    m = re.search(
        r"(?:r[ée]cup[ée]rer|recuperer|prendre|collecter).{0,90}?\b(?:à|au|aux)\s+(.+?)\b.{0,140}?\b(?:livrer|d[ée]poser|deposer|remettre).{0,90}?\b(?:à|au|aux)\s+(.+)",
        s,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # si "à Lyon" seulement -> dest = Lyon, origin = base
    m = re.search(r"(?:\bà\b|\ba\b)\s+([A-Za-zÀ-ÿ0-9'\- ,]{3,})", s, re.IGNORECASE)
    if m:
        dest = m.group(1).strip()
        return BASE_LOCATION, dest

    # fallback: tout le texte comme "destination"
    return BASE_LOCATION, s or BASE_LOCATION


# =========================
# Déduplication + buffer
# =========================

def anti_loop_ignore(client_id: str) -> bool:
    if not WHATSAPP_OWN_NUMBER:
        return False
    return normalize_client_id(client_id) == normalize_client_id(WHATSAPP_OWN_NUMBER)


def dedup_check_and_store(message_id: Optional[str], client_id: str) -> bool:
    if not message_id:
        return False

    now = _now_ts()
    conn = _db()
    cur = conn.cursor()

    cur.execute("DELETE FROM message_dedup WHERE created_at < ?", (now - DEDUP_TTL_SEC,))
    cur.execute("SELECT message_id FROM message_dedup WHERE message_id = ? LIMIT 1", (message_id,))
    if cur.fetchone():
        conn.close()
        return True

    cur.execute(
        "INSERT OR REPLACE INTO message_dedup(message_id, client_id, created_at) VALUES(?,?,?)",
        (message_id, client_id, now),
    )
    conn.commit()
    conn.close()
    return False


def soft_dedup_check_and_store(client_id: str, texte: str) -> bool:
    if SOFT_DEDUP_WINDOW_SEC <= 0:
        return False

    now = _now_ts()
    bucket = now // SOFT_DEDUP_WINDOW_SEC
    key_raw = f"{client_id}|{texte}|{bucket}"
    k = hashlib.sha1(key_raw.encode("utf-8", errors="ignore")).hexdigest()

    conn = _db()
    cur = conn.cursor()

    purge_before = now - (SOFT_DEDUP_WINDOW_SEC * 3)
    cur.execute("DELETE FROM soft_dedup WHERE created_at < ?", (purge_before,))

    cur.execute("SELECT k FROM soft_dedup WHERE k = ? LIMIT 1", (k,))
    if cur.fetchone():
        conn.commit()
        conn.close()
        return True

    cur.execute("INSERT OR REPLACE INTO soft_dedup(k, created_at) VALUES(?,?)", (k, now))
    conn.commit()
    conn.close()
    return False


def buffer_append_and_maybe_flush(client_id: str, new_text: str) -> Tuple[bool, str]:
    now = _now_ts()
    new_text = (new_text or "").strip()
    if not new_text:
        return (False, "")

    if CLIENT_BUFFER_WINDOW_SEC <= 0:
        return (True, new_text)

    conn = _db()
    cur = conn.cursor()

    cur.execute("SELECT buffer_text, first_at, last_at FROM client_message_buffer WHERE client_id = ?", (client_id,))
    row = cur.fetchone()

    if not row:
        cur.execute(
            "INSERT INTO client_message_buffer(client_id, buffer_text, first_at, last_at) VALUES(?,?,?,?)",
            (client_id, new_text, now, now),
        )
        conn.commit()
        conn.close()
        return (False, new_text)

    buffer_text = row["buffer_text"]
    first_at = int(row["first_at"])
    last_at = int(row["last_at"])

    # si la fenêtre est dépassée depuis le premier message => flush
    if now - first_at >= CLIENT_BUFFER_WINDOW_SEC:
        merged_prev = (buffer_text or "").strip()
        cur.execute(
            "UPDATE client_message_buffer SET buffer_text=?, first_at=?, last_at=? WHERE client_id=?",
            (new_text, now, now, client_id),
        )
        conn.commit()
        conn.close()
        return (True, merged_prev)

    merged = (buffer_text + "\n" + new_text).strip()
    cur.execute(
        "UPDATE client_message_buffer SET buffer_text=?, last_at=? WHERE client_id=?",
        (merged, now, client_id),
    )
    conn.commit()
    conn.close()
    return (False, merged)


# =========================
# GEO (gratuit) + cache + throttle
# =========================

def _ua() -> str:
    contact = NOMINATIM_EMAIL or "contact@exemple.tld"
    return f"{APP_NAME}/2.2 ({contact})"


def _http_get_json(url: str, timeout: float) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": _ua(), "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="ignore")
    return json.loads(data)


def _kv_get(key: str) -> Optional[str]:
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM kv WHERE key=? LIMIT 1", (key,))
    row = cur.fetchone()
    conn.close()
    return row["value"] if row else None


def _kv_set(key: str, value: str) -> None:
    now = _now_ts()
    conn = _db()
    conn.execute(
        """
        INSERT INTO kv(key, value, updated_at) VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (key, value, now)
    )
    conn.commit()
    conn.close()


def _throttle_nominatim() -> None:
    # Respect de l’intervalle minimal (serveur public)
    try:
        last_s = _kv_get("nominatim_last_ts")
        last = float(last_s) if last_s else 0.0
        now = time.time()
        wait = (last + NOMINATIM_MIN_INTERVAL_SEC) - now
        if wait > 0:
            time.sleep(wait)
        _kv_set("nominatim_last_ts", str(time.time()))
    except Exception:
        pass


def _canon_addr(addr: str) -> str:
    s = re.sub(r"\s+", " ", (addr or "").strip().lower())
    if "france" not in s and GEO_COUNTRYCODES.lower() == "fr":
        s = s + ", france"
    return s


def geocode_address(addr: str) -> Optional[Tuple[float, float, str]]:
    if not GEO_ENABLE:
        return None
    a = _canon_addr(addr)
    if not a:
        return None

    ttl_sec = GEO_CACHE_TTL_DAYS * 86400
    now = _now_ts()

    conn = _db()
    cur = conn.cursor()
    cur.execute("DELETE FROM geo_cache WHERE created_at < ?", (now - ttl_sec,))
    cur.execute("SELECT lat, lon, display_name FROM geo_cache WHERE address=? LIMIT 1", (a,))
    row = cur.fetchone()
    conn.commit()
    conn.close()

    if row:
        return float(row["lat"]), float(row["lon"]), str(row["display_name"] or "")

    _throttle_nominatim()
    params = {
        "q": a,
        "format": "jsonv2",
        "limit": "1",
        "addressdetails": "0",
        "countrycodes": GEO_COUNTRYCODES,
    }
    if NOMINATIM_EMAIL:
        params["email"] = NOMINATIM_EMAIL

    url = f"{NOMINATIM_BASE_URL}/search?{urllib.parse.urlencode(params)}"
    try:
        res = _http_get_json(url, timeout=GEO_TIMEOUT_SEC)
        if not isinstance(res, list) or not res:
            return None
        r0 = res[0]
        lat = float(r0.get("lat"))
        lon = float(r0.get("lon"))
        disp = str(r0.get("display_name") or "")

        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO geo_cache(address, lat, lon, display_name, created_at) VALUES(?,?,?,?,?)",
            (a, lat, lon, disp, now)
        )
        conn.commit()
        conn.close()
        return lat, lon, disp
    except Exception:
        return None


def osrm_route_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[Tuple[float, float]]:
    if not GEO_ENABLE:
        return None

    ttl_sec = ROUTE_CACHE_TTL_DAYS * 86400
    now = _now_ts()

    key = f"{lon1:.6f},{lat1:.6f};{lon2:.6f},{lat2:.6f}"
    kh = hashlib.sha1(key.encode("utf-8")).hexdigest()

    conn = _db()
    cur = conn.cursor()
    cur.execute("DELETE FROM route_cache WHERE created_at < ?", (now - ttl_sec,))
    cur.execute("SELECT distance_m, duration_s FROM route_cache WHERE k=? LIMIT 1", (kh,))
    row = cur.fetchone()
    conn.commit()
    conn.close()

    if row:
        return float(row["distance_m"]), float(row["duration_s"] or 0.0)

    url = (
        f"{OSRM_BASE_URL}/route/v1/driving/"
        f"{lon1:.6f},{lat1:.6f};{lon2:.6f},{lat2:.6f}"
        f"?overview=false&alternatives=false&steps=false"
    )
    try:
        res = _http_get_json(url, timeout=GEO_TIMEOUT_SEC)
        if not isinstance(res, dict) or res.get("code") != "Ok":
            return None
        routes = res.get("routes") or []
        if not routes:
            return None
        r0 = routes[0]
        dist_m = float(r0.get("distance", 0.0))
        dur_s = float(r0.get("duration", 0.0))

        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO route_cache(k, distance_m, duration_s, created_at) VALUES(?,?,?,?)",
            (kh, dist_m, dur_s, now)
        )
        conn.commit()
        conn.close()
        return dist_m, dur_s
    except Exception:
        return None


def compute_distance_km(origin: str, destination: str) -> Optional[float]:
    g1 = geocode_address(origin)
    g2 = geocode_address(destination)
    if not g1 or not g2:
        return None
    lat1, lon1, _ = g1
    lat2, lon2, _ = g2
    r = osrm_route_distance(lat1, lon1, lat2, lon2)
    if not r:
        return None
    dist_m, _dur_s = r
    km = dist_m / 1000.0
    if km <= 0:
        return None
    return km


# =========================
# Péage estimé (gratuit)
# =========================

def estimate_toll_eur(distance_km: float, text: str) -> float:
    if (not TOLL_EST_ENABLE) or (distance_km is None) or distance_km <= 0:
        return 0.0

    t = (text or "").lower()
    is_van = any(k in t for k in [
        "van", "fourgon", "utilitaire", "trafic", "jumper", "ducato",
        "transit", "master", "sprinter", "crafter", "boxer", "iveco"
    ])
    rate = TOLL_EUR_PER_KM_VAN if is_van else TOLL_EUR_PER_KM_CAR

    if distance_km < TOLL_DISTANCE_SHORT_KM:
        share = TOLL_SHARE_SHORT
    elif distance_km > TOLL_DISTANCE_LONG_KM:
        share = TOLL_SHARE_LONG
    else:
        share = TOLL_SHARE_MED

    toll = distance_km * rate * share
    if toll > TOLL_MAX_EUR:
        toll = TOLL_MAX_EUR

    return round(toll, 2)


# =========================
# Pricing (IDF vs Hors IDF)
# =========================

def d2(x: Decimal) -> Decimal:
    return x.quantize(CURRENCY_ROUND, rounding=ROUND_HALF_UP)


def detect_province_supplements(text: str) -> Tuple[Decimal, Decimal]:
    """
    Retourne (extra_fixed_eur, extra_pct)
    - A/R +50%, nuit +30%, week-end +20%
    - colis volumineux/fragile => +20..+60€ (auto)
    """
    t = (text or "").lower()
    extra_fixed = Decimal("0")
    extra_pct = Decimal("0")

    if re.search(r"\b(aller\s*[- ]?\s*retour|a\s*/\s*r|a/r)\b", t):
        extra_pct += PROV_AR_PCT

    if ("nuit" in t) or re.search(r"\b(22h|23h|0h|00h|minuit)\b", t):
        extra_pct += PROV_NIGHT_PCT

    if any(k in t for k in ["week-end", "weekend", "samedi", "dimanche"]):
        extra_pct += PROV_WEEKEND_PCT

    if any(k in t for k in ["volumineux", "volumineuse", "meuble", "palette", "lourd", "gros colis"]):
        extra_fixed = max(extra_fixed, PROV_BULKY_MAX_EUR)
    elif any(k in t for k in ["fragile", "verre", "tv", "ordinateur", "écran", "ecran"]):
        extra_fixed = max(extra_fixed, max(PROV_BULKY_MIN_EUR, Decimal("30")))

    return extra_fixed, extra_pct


def price_idf(urgent: bool) -> Decimal:
    p = IDF_URGENT_EUR if urgent else IDF_STANDARD_EUR
    p = max(p, MIN_PRICE_EUR)
    return d2(p)


def price_province(distance_km: float, toll_eur: float, text: str) -> Decimal:
    """
    base = (1€ * km) + (2 * péage) + 30€ + extras ; total = base * (1+40%)
    """
    km_d = Decimal(str(distance_km))
    toll_d = Decimal(str(toll_eur))

    extra_fixed, extra_pct = detect_province_supplements(text)

    base = (PROV_EUR_PER_KM * km_d) + (PROV_TOLL_MULT * toll_d) + PROV_MEAL_EUR + extra_fixed
    base = base * (Decimal("1") + extra_pct)
    total = base * (Decimal("1") + PROV_MARGIN_PCT)

    total = max(total, MIN_PRICE_EUR)
    return d2(total)


def build_conditions() -> str:
    return (
        "Paiement 100% à l'avance.\n"
        "Preuve de prise en charge et de livraison.\n"
        "Annulation : si l'intervention a déjà commencé, facturation partielle possible."
    )


def summarize(text: str) -> str:
    s = (text or "").strip()
    if len(s) > 160:
        s = s[:157] + "..."
    return s


def make_ref() -> str:
    return "OCF-" + datetime.now(timezone.utc).strftime("%Y%m%d") + "-" + uuid.uuid4().hex[:6].upper()


# =========================
# Missions DB
# =========================

def create_mission(
    ref: str,
    client_id: str,
    message_id: Optional[str],
    raw_request: str,
    merged_request: str,
    resume_client: str,
    type_detecte: str,
    zone_tarifaire: str,
    delai_estime: str,
    prix_recommande_eur: Optional[Decimal],
    conditions: str,
    origin: str,
    destination: str,
    distance_km: Optional[float],
    toll_est_eur: Optional[float],
    statut: str,
) -> int:
    now = utc_now_iso()
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO missions(
            ref, client_id, message_id, raw_request, merged_request, resume_client,
            type_detecte, zone_tarifaire, delai_estime, prix_recommande_eur, conditions,
            origin, destination, distance_km, toll_est_eur, statut, created_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ref, client_id, message_id, raw_request, merged_request, resume_client,
        type_detecte, zone_tarifaire, delai_estime,
        float(prix_recommande_eur) if prix_recommande_eur is not None else None,
        conditions,
        origin, destination, distance_km, toll_est_eur,
        statut, now, now
    ))
    mission_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(mission_id)


def update_mission_status(mission_id: int, statut: str) -> None:
    conn = _db()
    conn.execute(
        "UPDATE missions SET statut=?, updated_at=? WHERE id=?",
        (statut, utc_now_iso(), mission_id),
    )
    conn.commit()
    conn.close()


# =========================
# API
# =========================

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "app": APP_NAME, "utc": utc_now_iso()}


@app.get("/admin/missions")
def admin_missions(limit: int = 30, token: str = "") -> Dict[str, Any]:
    if not _debug_allowed(token):
        raise HTTPException(status_code=401, detail="bad token")
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, ref, client_id, zone_tarifaire, prix_recommande_eur, delai_estime, created_at
        FROM missions
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"items": rows, "count": len(rows)}


@app.get("/debug/last")
def debug_last(limit: int = 30, token: str = "") -> Dict[str, Any]:
    if not _debug_allowed(token):
        raise HTTPException(status_code=401, detail="bad token")
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT id, kind, payload, created_at FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            p = json.loads(r["payload"])
        except Exception:
            p = r["payload"]
        out.append({"id": int(r["id"]), "kind": r["kind"], "payload": p, "created_at": r["created_at"]})
    return {"items": out, "count": len(out)}


@app.post("/mission/demande", response_model=MissionOut)
async def mission_demande(request: Request) -> MissionOut:
    require_token(request)

    raw: Any = None
    try:
        raw = await request.json()
    except Exception:
        raw = None

    if raw is None:
        try:
            form = await request.form()
            raw = dict(form)
        except Exception:
            raw = None

    if raw is None:
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", errors="ignore").strip()
        if body_text:
            try:
                raw = json.loads(body_text)
            except Exception:
                raw = {"texte": body_text}
        else:
            raw = {}

    payload = DemandeMission.model_validate(raw) if hasattr(DemandeMission, "model_validate") else DemandeMission(**raw)
    client_id, message_id, texte, diag = resolve_payload(payload)

    if not client_id:
        _log("bad_request_no_client", {"raw": raw, "diag": diag})
        raise HTTPException(status_code=422, detail="client_id/from manquant (WhatsApp ID).")
    if not texte:
        _log("bad_request_no_text", {"raw": raw, "diag": diag, "client_id": client_id})
        raise HTTPException(status_code=422, detail="texte manquant.")

    rate_limit_or_429(client_id)

    if anti_loop_ignore(client_id):
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="Message ignoré (anti-boucle).",
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="ignored_self",
            ref_mission=None,
        )

    if dedup_check_and_store(message_id, client_id):
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="Message déjà traité.",
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="duplicate_message",
            ref_mission=None,
        )

    if (not message_id) and soft_dedup_check_and_store(client_id, texte):
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="Message déjà reçu (anti-doublon).",
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="duplicate_message",
            ref_mission=None,
        )

    should_process_now, merged = buffer_append_and_maybe_flush(client_id, texte)

    urgent = is_urgent(merged)
    delai_estime = DELAI_URGENT if urgent else DELAI_STANDARD
    type_detecte = detect_type(merged)

    origin, destination = extract_route(merged)

    # Zone tarifaire: IDF uniquement si ORIGIN et DEST sont IDF
    origin_idf = is_idf_text(origin)
    dest_idf = is_idf_text(destination)
    idf_internal = origin_idf and dest_idf

    zone_tarifaire = "IDF" if idf_internal else "Hors IDF"

    # Distance & péage
    distance_km: Optional[float] = None
    toll_est_eur: Optional[float] = None

    if not idf_internal and GEO_ENABLE:
        # Appels réseau => thread (ne bloque pas l'event loop)
        try:
            distance_km = await asyncio.to_thread(compute_distance_km, origin, destination)
        except Exception:
            distance_km = None

        if distance_km is not None and TOLL_EST_ENABLE:
            toll_est_eur = estimate_toll_eur(distance_km, merged)

    # Pricing
    prix: Optional[Decimal] = None
    conditions = build_conditions()

    if idf_internal:
        prix = price_idf(urgent)
    else:
        # Si on ne peut pas calculer la distance => on demande juste adresses complètes
        if distance_km is None:
            prix = None
        else:
            toll_est_eur = toll_est_eur if toll_est_eur is not None else 0.0
            prix = price_province(distance_km, toll_est_eur, merged)

    resume = summarize(merged)
    ref = make_ref()

    # Même en buffering : on renvoie une réponse exploitable (important WhatsApp/Make)
    if not should_process_now:
        statut = "buffering"
        mission_id = 0
    else:
        statut = "en_attente_validation"
        mission_id = create_mission(
            ref=ref,
            client_id=client_id,
            message_id=message_id,
            raw_request=texte,
            merged_request=merged,
            resume_client=resume,
            type_detecte=type_detecte,
            zone_tarifaire=zone_tarifaire,
            delai_estime=delai_estime,
            prix_recommande_eur=prix,
            conditions=conditions,
            origin=origin,
            destination=destination,
            distance_km=distance_km,
            toll_est_eur=toll_est_eur,
            statut=statut,
        )

    _log("mission_processed", {
        "client_id": client_id,
        "message_id": message_id,
        "origin": origin,
        "destination": destination,
        "idf_internal": idf_internal,
        "distance_km": distance_km,
        "toll_est_eur": toll_est_eur,
        "prix": float(prix) if prix is not None else None,
        "statut": statut,
        "ref": ref,
    })

    # Si on n'a pas de distance (donc pas de prix): demander uniquement les adresses
    if prix is None and not idf_internal:
        resume_client = resume
        return MissionOut(
            mission_id=mission_id,
            client_id=client_id,
            resume_client=(
                f"{resume_client}\n\n"
                "Pour vous donner le montant total exact, j’ai besoin des adresses complètes :\n"
                "• récupération (rue + ville + code postal)\n"
                "• livraison (rue + ville + code postal)"
            ),
            prix_recommande_eur=None,
            delai_estime=delai_estime,
            conditions=conditions,
            zone_tarifaire=zone_tarifaire,
            type_detecte=type_detecte,
            statut=statut,
            ref_mission=ref if mission_id != 0 else None,
        )

    return MissionOut(
        mission_id=mission_id,
        client_id=client_id,
        resume_client=resume,
        prix_recommande_eur=float(prix) if prix is not None else None,
        delai_estime=delai_estime,
        conditions=conditions,
        zone_tarifaire=zone_tarifaire,
        type_detecte=type_detecte,
        statut=statut,
        ref_mission=ref if mission_id != 0 else None,
    )


@app.post("/mission/{mission_id}/status")
def set_status(mission_id: int, statut: str, token: str = "") -> Dict[str, Any]:
    if not _debug_allowed(token):
        raise HTTPException(status_code=401, detail="bad token")
    if not statut:
        raise HTTPException(status_code=422, detail="statut manquant")
    update_mission_status(mission_id, statut)
    return {"ok": True, "mission_id": mission_id, "statut": statut}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
