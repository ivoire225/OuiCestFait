# main.py — OuiCestFait (Option A — rapidité/volume/fiabilité)
# - Tarification IDF vs Hors IDF
# - Hors IDF: distance auto (gratuit) + péage estimé (gratuit) => prix total sans demander le péage au client
# - Anti-doublon (message_id + soft dedup), rate limiting, DB WAL/busy_timeout
# - Parsing payload ultra tolérant (Make/WhatsApp/Meta)
# - Base départ: Mormant 77720
# - Urgent keywords => délai "sous 1h" sinon "dans la journée"
#
# Dépendances (requirements.txt): fastapi, uvicorn, pydantic, python-multipart (si form-data) + openai (optionnel si IA)
from __future__ import annotations

import os

# Charge automatiquement les variables d'environnement depuis un fichier .env (optionnel)
# => permet de mettre AI_ENABLE / OPENAI_API_KEY / AI_MODEL sans setx.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass
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

# OpenAI (optionnel) pour l'extraction IA
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

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

# IA (optionnelle) — extraction structurée (adresses, type, urgence) via LLM
AI_ENABLE = os.getenv("AI_ENABLE", "0").strip() in ("1", "true", "True", "yes", "YES")
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini").strip()
AI_TIMEOUT_SEC = float(os.getenv("AI_TIMEOUT_SEC", "12"))
AI_MAX_TEXT_CHARS = int(os.getenv("AI_MAX_TEXT_CHARS", "1400"))
AI_MIN_CONFIDENCE = float(os.getenv("AI_MIN_CONFIDENCE", "0.72"))
AI_FORCE_JSON = os.getenv("AI_FORCE_JSON", "1").strip() not in ("0", "false", "False", "no", "NO")
AI_ONLY_WHEN_AMBIGUOUS = os.getenv("AI_ONLY_WHEN_AMBIGUOUS", "1").strip() not in ("0", "false", "False", "no", "NO")
AI_CACHE_TTL_SEC = int(os.getenv("AI_CACHE_TTL_SEC", "86400"))  # 24h (0 = désactivé)

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

# Tarifs IDF (voiture, départ Mormant 77720)
# - Règle simple (IDF interne uniquement): prix = max(IDF_MIN_EUR, IDF_EUR_PER_KM_CAR * km_total)
# - Urgent: x IDF_URGENT_MULT
# (Les variables IDF_STANDARD_EUR/IDF_URGENT_EUR restent comme filet de sécurité si la distance est indisponible.)
IDF_STANDARD_EUR = Decimal(os.getenv("IDF_STANDARD_EUR", "80"))
IDF_URGENT_EUR = Decimal(os.getenv("IDF_URGENT_EUR", "150"))
IDF_EUR_PER_KM_CAR = Decimal(os.getenv("IDF_EUR_PER_KM_CAR", "1.80"))
IDF_MIN_EUR = Decimal(os.getenv("IDF_MIN_EUR", "90"))
IDF_URGENT_MULT = Decimal(os.getenv("IDF_URGENT_MULT", "1.25"))
MIN_PRICE_EUR = Decimal(os.getenv("MIN_PRICE_EUR", "25"))

# Hors IDF (règle validée)
# base = (1€ * km) + (2 * péage) + 30€ repas ; total = base * (1 + 40%)
PROV_EUR_PER_KM = Decimal(os.getenv("PROV_EUR_PER_KM", "1.0"))
PROV_TOLL_MULT = Decimal(os.getenv("PROV_TOLL_MULT", "2.0"))
PROV_MEAL_EUR = Decimal(os.getenv("PROV_MEAL_EUR", "30"))
PROV_MARGIN_PCT = Decimal(os.getenv("PROV_MARGIN_PCT", "0.40"))  # +40%
PROV_URGENT_MULT = Decimal(os.getenv("PROV_URGENT_MULT", "1.15"))

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

app = FastAPI(title=f"{APP_NAME} API", version="2.4.1")


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
    CREATE TABLE IF NOT EXISTS ai_cache (
        k TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ai_cache_created ON ai_cache(created_at)")


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
# IA — Extraction structurée (optionnelle)
# =========================

class AIExtract(BaseModel):
    origin: Optional[str] = None
    destination: Optional[str] = None
    urgent: Optional[bool] = None
    mission_type: Optional[str] = Field(None, description="livraison|courses|transport|conciergerie")
    confidence: Optional[float] = Field(None, description="0..1")
    missing_fields: Optional[List[str]] = None
    notes: Optional[str] = None

    if ConfigDict is not None:
        model_config = ConfigDict(extra="ignore")
    else:
        class Config:
            extra = "ignore"


_ai_client: Optional[Any] = None


def _get_ai_client() -> Optional[Any]:
    global _ai_client
    if _ai_client is not None:
        return _ai_client
    if OpenAI is None:
        return None
    try:
        _ai_client = OpenAI()  # OPENAI_API_KEY lu depuis l'env
        return _ai_client
    except Exception:
        return None


def _strip_json(text_resp: str) -> str:
    """Nettoie une réponse qui contient éventuellement des ```json ...```."""
    s = (text_resp or "").strip()
    # enlever fences
    s = re.sub(r"^\s*```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```\s*$", "", s)
    # si texte autour, extraire premier objet JSON
    if "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        if 0 <= start < end:
            s = s[start:end+1]
    return s.strip()


def ai_extract_fields(text_in: str) -> Optional[AIExtract]:
    """Appel IA synchrone (exécuté dans un thread côté endpoint).

    Objectif:
    - Extraire origin/destination quand le message est ambigu (ex: 'Adresse1 et Adresse2')
    - Renforcer le type de mission
    - Donner un score de confiance
    """
    if not AI_ENABLE:
        return None

    client = _get_ai_client()
    if client is None:
        return None

    user_text = (text_in or "").strip()
    if not user_text:
        return None
    if len(user_text) > AI_MAX_TEXT_CHARS:
        user_text = user_text[:AI_MAX_TEXT_CHARS]

    cached = ai_cache_get(user_text)
    if cached is not None:
        return cached

    system = (
        "Tu es un extracteur d'informations pour un service de livraison/conciergerie.\n"
        "Ta tâche: convertir un message WhatsApp en données structurées.\n"
        "Réponds UNIQUEMENT avec un JSON (pas de texte), avec ces clés:\n"
        "{"
        "\"origin\": string|null, "
        "\"destination\": string|null, "
        "\"urgent\": boolean|null, "
        "\"mission_type\": \"livraison\"|\"courses\"|\"transport\"|\"conciergerie\"|null, "
        "\"confidence\": number (0..1), "
        "\"missing_fields\": array, "
        "\"notes\": string|null"
        "}\n"
        "Règles:\n"
        "- Si le message contient deux adresses séparées par 'et', la 1ère est origin, la 2ème destination.\n"
        "- Essaie de garder les adresses telles quelles (rue, ville, CP).\n"
        "- confidence haute (>=0.8) seulement si origin ET destination sont clairement identifiées.\n"
        "- missing_fields doit inclure 'origin' et/ou 'destination' si incomplet.\n"
    )

    try:
        # API OpenAI Python (v1+) — Chat Completions
        kwargs = dict(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            temperature=0,
        )
        if AI_FORCE_JSON:
            kwargs["response_format"] = {"type": "json_object"}
        # Certains clients acceptent timeout=...
        try:
            resp = client.chat.completions.create(**kwargs, timeout=AI_TIMEOUT_SEC)
        except TypeError:
            # Fallback: certains SDKs n'acceptent pas timeout/response_format
            kwargs.pop("response_format", None)
            try:
                resp = client.chat.completions.create(**kwargs)
            except Exception:
                return None

        content = None
        try:
            content = resp.choices[0].message.content
        except Exception:
            content = None
        if not content:
            return None

        js = _strip_json(content)
        data = json.loads(js)

        # Validation Pydantic
        try:
            out = AIExtract.model_validate(data) if hasattr(AIExtract, "model_validate") else AIExtract(**data)
        except Exception:
            return None

        # Normalisation légère
        if out.origin:
            out.origin = str(out.origin).strip()
        if out.destination:
            out.destination = str(out.destination).strip()
        if out.mission_type:
            out.mission_type = str(out.mission_type).strip().lower()
        if out.confidence is not None:
            try:
                out.confidence = float(out.confidence)
            except Exception:
                out.confidence = None

        ai_cache_set(user_text, out)
        return out
    except Exception:
        return None


def ai_should_apply(ai: Optional[AIExtract]) -> bool:
    if not ai:
        return False
    if ai.confidence is None:
        return False
    if ai.confidence < AI_MIN_CONFIDENCE:
        return False
    # Applique seulement si au moins une adresse est fournie
    if not (ai.origin or ai.destination):
        return False
    return True


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


def _extract_postal_depts(text: str) -> set[str]:
    t = (text or "").lower()
    cps = re.findall(r"\b(\d{5})\b", t)
    return {cp[:2] for cp in cps}


def is_idf_text(text: str) -> bool:
    """Heuristique IDF sur texte.

    - Si on détecte des codes postaux (5 chiffres), on classe via le département (2 premiers chiffres).
      * Si c'est "mixte" (IDF + non-IDF), on considère **non-IDF** (plus sûr).
    - Sinon, on retombe sur les villes/keywords.
    """
    t = (text or "").lower()

    depts = _extract_postal_depts(t)
    if depts:
        return depts.issubset(IDF_DEPTS)

    if _contains_idf_postal_or_dept(t):
        return True

    for c in IDF_CITIES:
        if c in t:
            return True

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

_STREET_WORDS = (
    "rue", "avenue", "av.", "boulevard", "bd", "chemin", "route", "impasse",
    "allée", "allee", "place", "quai", "cours", "square", "résidence", "residence",
)


def _looks_like_address(part: str) -> bool:
    t = (part or "").lower()
    # Un numéro + un mot de voie => très probable adresse
    has_number = bool(re.search(r"\b\d{1,4}\b", t))
    has_street = any(w in t for w in _STREET_WORDS)
    return has_number and has_street


def _looks_like_place(part: str) -> bool:
    """Heuristique souple: accepte une adresse OU un lieu plausible (ville/quartier + CP éventuel)."""
    t = (part or "").strip().lower()
    if not t:
        return False
    if _looks_like_address(t):
        return True
    # Code postal FR
    if re.search(r"\b\d{5}\b", t):
        return True
    # Mention de département IDF
    if re.search(r"\b(75|77|78|91|92|93|94|95)\b", t):
        return True
    # Présence d'une préposition typique (à/au/aux) + un nom
    if re.search(r"\b(?:à|a|au|aux)\b", t):
        return True
    # Ville/quartier: 1..6 mots, lettres majoritaires
    if re.fullmatch(r"[a-zà-ÿ'\- ]{3,}", t) and len(t.split()) <= 6:
        return True
    # Exemple: 'paris 15e'
    if re.fullmatch(r"[a-zà-ÿ'\- ]{3,}\s*\d{1,2}e?", t):
        return True
    return False


def is_route_ambiguous(full_text: str, origin: str, destination: str) -> bool:
    """Décide si on doit tenter l'IA (coût) pour clarifier origin/destination."""
    ft = (full_text or "").strip()
    o = (origin or "").strip()
    d = (destination or "").strip()
    if not o or not d:
        return True
    if o == d:
        return True
    # Fallback typique: destination = texte complet
    if d == ft or o == ft:
        return True
    if o == BASE_LOCATION and d == BASE_LOCATION:
        return True
    if len(o) > 160 or len(d) > 160:
        return True
    if not _looks_like_place(o) or not _looks_like_place(d):
        return True
    # Message avec 'et' mais extraction encore bancale
    if re.search(r"\bet\b", ft, re.IGNORECASE) and (o == BASE_LOCATION or d == ft):
        return True
    return False


def extract_route(text: str) -> Tuple[str, str]:
    """Extrait (origin, destination) à partir du texte client.

    Cas gérés :
    - "X -> Y" / "X → Y"
    - "de X à Y" / "depuis X vers Y"
    - "récupérer à X ... livrer à Y"
    - "ADRESSE1 et ADRESSE2" (ex: "15 rue ... à Paris ... et 3 rue ... à Orléans")
    - fallback : origin = BASE_LOCATION, destination = texte
    """
    s = re.sub(r"\s+", " ", (text or "").strip())

    # 1) Deux adresses séparées (et / , / ; / & / /) — seulement si ça ressemble à 2 adresses
    m = re.split(r"\s*(?:\bet\b|&|/|,|;)\s*", s, maxsplit=1, flags=re.IGNORECASE)
    if len(m) == 2:
        p1, p2 = (m[0] or "").strip(), (m[1] or "").strip()
        if _looks_like_address(p1) and _looks_like_address(p2):
            return p1, p2

    # 2) X -> Y
    m = re.search(r"(.+?)\s*(?:->|→)\s*(.+)", s)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # 3) de X à Y
    m = re.search(r"(?:de|depuis)\s+(.+?)\s+(?:à|a|vers)\s+(.+)", s, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # 4) récupérer/enlever à X ... livrer à Y
    m = re.search(
        r"(?:r[ée]cup[ée]rer|recuperer|prendre|collecter|enlever|enl[eè]ver|enl[eè]ve|enl[eè]vement|enlevement|ramasser|retirer).{0,120}?\b(?:à|au|aux)\s+(.+?)\b.{0,240}?\b(?:livrer|livraison|d[ée]poser|deposer|remettre).{0,120}?\b(?:à|au|aux)\s+(.+)",
        s,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # 5) si "à Lyon" seulement -> dest = Lyon, origin = base
    m = re.search(r"(?:\bà\b|\ba\b)\s+([A-Za-zÀ-ÿ0-9'\- ,]{3,})", s, re.IGNORECASE)
    if m:
        dest = m.group(1).strip()
        return BASE_LOCATION, dest

    return BASE_LOCATION, s or BASE_LOCATION


# =========================
# Déduplication + buffer
# =========================

def anti_loop_ignore(client_id: str, incoming_text: str) -> bool:
    """Détecte un echo webhook: le message entrant est exactement notre dernière réponse.

    Fonctionne même si Meta/Make rejoue des events, sans dépendre du message_id.
    Stockage dans la table kv (pas de migration SQL nécessaire).
    """
    if not client_id:
        return False
    key = f"anti_loop:{normalize_client_id(client_id)}"
    raw = _kv_get(key)
    if not raw:
        return False
    try:
        payload = json.loads(raw)
    except Exception:
        return False

    last_hash = str(payload.get("h") or "")
    last_ts = int(payload.get("ts") or 0)

    if not last_hash or not last_ts:
        return False
    if _now_ts() - last_ts > int(ANTI_LOOP_WINDOW_SEC):
        return False

    incoming_hash = _sha1(incoming_text or "")
    return incoming_hash == last_hash


def anti_loop_store_last_response(client_id: str, response_text: str) -> None:
    if not client_id:
        return
    key = f"anti_loop:{normalize_client_id(client_id)}"
    payload = {"h": _sha1(response_text or ""), "ts": _now_ts()}
    _kv_set(key, json.dumps(payload, ensure_ascii=False))



def dedup_check_and_store(message_id: Optional[str], client_id: str) -> bool:
    """Retourne True si le message_id a déjà été vu (donc à ignorer)."""
    if not message_id:
        return False

    now = _now_ts()
    conn = _db()
    cur = conn.cursor()
    try:
        # purge TTL
        cur.execute("DELETE FROM message_dedup WHERE created_at < ?", (now - DEDUP_TTL_SEC,))
        # check exist
        cur.execute("SELECT 1 FROM message_dedup WHERE message_id = ? LIMIT 1", (message_id,))
        if cur.fetchone():
            conn.commit()
            return True

        cur.execute(
            "INSERT OR REPLACE INTO message_dedup(message_id, client_id, created_at) VALUES(?,?,?)",
            (message_id, client_id, now),
        )
        conn.commit()
        return False
    finally:
        conn.close()

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
    """Buffer optionnel pour regrouper plusieurs messages proches.

    - Si CLIENT_BUFFER_WINDOW_SEC <= 0 : traitement immédiat => (True, new_text)
    - Sinon :
        * on cumule le texte dans client_message_buffer
        * dès que (now - first_at) >= window, on FLUSH le buffer (en incluant le message courant),
          puis on supprime l'entrée => (True, merged)
        * sinon => (False, merged_buffer)
    """
    now = _now_ts()
    new_text = (new_text or "").strip()
    if not new_text:
        return (False, "")

    if CLIENT_BUFFER_WINDOW_SEC <= 0:
        return (True, new_text)

    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT buffer_text, first_at FROM client_message_buffer WHERE client_id = ?", (client_id,))
        row = cur.fetchone()

        if not row:
            cur.execute(
                "INSERT INTO client_message_buffer(client_id, buffer_text, first_at, last_at) VALUES(?,?,?,?)",
                (client_id, new_text, now, now),
            )
            conn.commit()
            return (False, new_text)

        buffer_text = (row["buffer_text"] or "").strip()
        first_at = int(row["first_at"])

        merged = ("\n".join([buffer_text, new_text])).strip() if buffer_text else new_text

        # fenêtre écoulée depuis le premier message => flush en incluant le message courant
        if now - first_at >= CLIENT_BUFFER_WINDOW_SEC:
            cur.execute("DELETE FROM client_message_buffer WHERE client_id = ?", (client_id,))
            conn.commit()
            return (True, merged)

        # sinon: on continue à bufferiser
        cur.execute(
            "UPDATE client_message_buffer SET buffer_text=?, last_at=? WHERE client_id=?",
            (merged, now, client_id),
        )
        conn.commit()
        return (False, merged)
    finally:
        conn.close()


# =========================
# GEO# =========================
# GEO (gratuit) + cache + throttle
# =========================

def _ua() -> str:
    contact = NOMINATIM_EMAIL or "contact@exemple.tld"
    return f"{APP_NAME}/2.3 ({contact})"


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


def _dept_from_string(s: str) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"\b(\d{5})\b", s)
    if not m:
        return None
    return m.group(1)[:2]


def is_idf_place_geo(place: str) -> Optional[bool]:
    """Détermine IDF via geocoding (si possible).

    Retourne:
    - True/False si un code postal est détecté (place ou display_name)
    - None si on ne peut pas conclure
    """
    g = geocode_address(place)
    if not g:
        return None
    _lat, _lon, disp = g
    dept = _dept_from_string(disp) or _dept_from_string(place)
    if not dept:
        return None
    return dept in IDF_DEPTS



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
def is_precise_place(s: str) -> bool:
    """Heuristique simple: une adresse avec CP/numéro est plus 'verrouillable' qu'un nom de ville seul."""
    if not s:
        return False
    t = (s or "").lower()
    # code postal FR
    if re.search(r"\b\d{5}\b", t):
        return True
    # présence de chiffre (numéro de rue, etc.)
    if re.search(r"\b\d{1,4}\b", t):
        return True
    return False
def build_need_info_message(missing: List[str], origin: str, destination: str, urgent: bool) -> str:
    lines = [
        "Pour te donner un prix précis (au km), il me manque :",
        "",
    ]
    if "origin" in missing:
        lines.append("• Adresse de départ complète (numéro + rue + code postal + ville)")
    if "destination" in missing:
        lines.append("• Adresse d’arrivée complète (numéro + rue + code postal + ville)")
    if "precision" in missing:
        lines.append("• Les adresses doivent être suffisamment précises (idéalement avec code postal)")
    if "route" in missing:
        lines.append("• Je n’arrive pas à calculer l’itinéraire : ajoute au minimum les codes postaux")
    lines += [
        "",
        "Optionnel : volume/poids, fragile, étage/ascenseur, aller-retour.",
    ]
    if urgent:
        lines += ["", "PS : tu as indiqué une urgence — dès que j’ai les adresses complètes, je réponds tout de suite."]
    lines.append(f"• {APP_NAME}")
    return "\n".join(lines)





def place_is_base(place: str) -> bool:
    if not place:
        return False
    t = (place or "").lower()
    if BASE_POSTAL.lower() in t:
        return True
    if BASE_CITY.lower() in t:
        return True
    return _canon_addr(place) == _canon_addr(BASE_LOCATION)


def compute_trip_distance_km(origin: str, destination: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Distance totale en voiture en partant de la base (Mormant 77720).

    Retourne (km_total, km_base_vers_origin, km_origin_vers_destination).

    - Si origin == base => km_total = base->destination
    - Sinon => km_total = base->origin + origin->destination
    """
    o = (origin or BASE_LOCATION).strip()
    d = (destination or BASE_LOCATION).strip()

    # base -> destination (si origin non précisée ou égale à base)
    if place_is_base(o):
        leg_to_pickup = 0.0
        leg_delivery = compute_distance_km(BASE_LOCATION, d)
        if leg_delivery is None:
            return None, None, None
        return float(leg_delivery), float(leg_to_pickup), float(leg_delivery)

    leg_to_pickup = compute_distance_km(BASE_LOCATION, o)
    leg_delivery = compute_distance_km(o, d)

    if leg_to_pickup is None or leg_delivery is None:
        # On renvoie ce qu'on a (utile debug/log), mais total reste None
        return None, leg_to_pickup, leg_delivery

    km_total = float(leg_to_pickup) + float(leg_delivery)
    return km_total, float(leg_to_pickup), float(leg_delivery)



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


def price_idf(distance_km_total: Optional[float], urgent: bool) -> Decimal:
    """Tarification IDF (voiture) en intégrant le départ Mormant 77720.

    Règle:
    - prix = max(IDF_MIN_EUR, IDF_EUR_PER_KM_CAR * km_total)
    - urgent => x IDF_URGENT_MULT
    - filet de sécurité: jamais en dessous de IDF_STANDARD_EUR / IDF_URGENT_EUR
    """
    fixed = IDF_URGENT_EUR if urgent else IDF_STANDARD_EUR

    if distance_km_total is not None and float(distance_km_total) > 0:
        km_d = Decimal(str(distance_km_total))
        p = IDF_EUR_PER_KM_CAR * km_d
        p = max(p, IDF_MIN_EUR, fixed)
        if urgent:
            p = p * IDF_URGENT_MULT
            p = max(p, fixed)
    else:
        # si la distance n'est pas calculable, on applique un minimum sécurisé
        p = max(IDF_MIN_EUR, fixed)
        if urgent:
            p = max(IDF_MIN_EUR * IDF_URGENT_MULT, fixed)

    p = max(p, MIN_PRICE_EUR)
    return d2(p)


def price_province(distance_km_total: float, toll_eur: float, text: str, urgent: bool) -> Decimal:
    """Tarification hors IDF (voiture), total 'tout compris' sans demander le péage au client.

    Règle validée:
    - base = (PROV_EUR_PER_KM * km_total) + (PROV_TOLL_MULT * peage_estime) + PROV_MEAL_EUR + extras
    - total = base * (1 + PROV_MARGIN_PCT)
    - urgent => x PROV_URGENT_MULT
    """
    km_d = Decimal(str(distance_km_total))
    toll_d = Decimal(str(toll_eur))

    extra_fixed, extra_pct = detect_province_supplements(text)

    base = (PROV_EUR_PER_KM * km_d) + (PROV_TOLL_MULT * toll_d) + PROV_MEAL_EUR + extra_fixed
    base = base * (Decimal("1") + extra_pct)
    total = base * (Decimal("1") + PROV_MARGIN_PCT)

    if urgent:
        total = total * PROV_URGENT_MULT

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

    if anti_loop_ignore(client_id, texte):
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

    # Extraction heuristique (fallback)
    origin, destination = extract_route(merged)

    # Zone tarifaire (heuristique texte). Affinée plus bas si le géocodage est dispo.
    origin_idf_txt = is_idf_text(origin)
    dest_idf_txt = is_idf_text(destination)
    idf_internal = bool(origin_idf_txt and dest_idf_txt)
    zone_tarifaire = "IDF" if idf_internal else "Hors IDF"

    # Si buffering activé et fenêtre non atteinte : on répond sans faire d'appels réseau (GEO/OSRM)
    if not should_process_now:
        resume_buf = summarize(merged)
        _log("mission_buffering", {
            "client_id": client_id,
            "message_id": message_id,
            "origin": origin,
            "destination": destination,
            "idf_internal": idf_internal,
            "statut": "buffering",
        })
        msg = (
            f"{resume_buf}\n\n"
            "Message reçu. Ajoute les infos manquantes (adresses complètes, contraintes, volume/fragile, "
            "aller-retour, etc.) et je calcule le montant."
        )
        anti_loop_store_last_response(client_id, msg)
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client=msg,
            prix_recommande_eur=None,
            delai_estime=delai_estime,
            conditions=None,
            zone_tarifaire=zone_tarifaire,
            type_detecte=type_detecte,
            statut="buffering",
            ref_mission=None,
        )


    # IA (optionnelle): extraction des adresses/type quand le texte est ambigu
    ai_info = None
    if AI_ENABLE:
        ai_needed = True
        if AI_ONLY_WHEN_AMBIGUOUS:
            ai_needed = is_route_ambiguous(merged, origin, destination)
        if ai_needed:
            try:
                ai_info = await asyncio.to_thread(ai_extract_fields, merged)
            except Exception:
                ai_info = None

    # Appliquer l'IA uniquement si la confiance est suffisante
    if ai_should_apply(ai_info):
        if ai_info.origin:
            origin = ai_info.origin
        if ai_info.destination:
            destination = ai_info.destination
        if ai_info.mission_type in ("livraison", "courses", "transport", "conciergerie"):
            type_detecte = ai_info.mission_type

        # IMPORTANT : l'urgence reste déterminée par les mots-clés (règle métier validée).
        # On ne sur-écrit pas 'urgent' via l'IA pour éviter les faux positifs.

        # Recalcule la zone tarifaire après ajustement IA (heuristique texte). Affinée GEO plus bas.
        origin_idf_txt = is_idf_text(origin)
        dest_idf_txt = is_idf_text(destination)
        idf_internal = bool(origin_idf_txt and dest_idf_txt)
        zone_tarifaire = "IDF" if idf_internal else "Hors IDF"

        _log("ai_extract_applied", {
            "client_id": client_id,
            "message_id": message_id,
            "confidence": getattr(ai_info, "confidence", None),
            "origin": origin,
            "destination": destination,
            "mission_type": type_detecte,
        })


    # Distance (voiture) en intégrant le départ Mormant 77720
    distance_km: Optional[float] = None
    km_base_to_pickup: Optional[float] = None
    km_pickup_to_drop: Optional[float] = None
    toll_est_eur: Optional[float] = None

    # Mode strict: pas de prix tant qu'on n'a pas 2 adresses suffisamment précises.
    need_info_reasons: List[str] = []
    if not origin:
        need_info_reasons.append("origin")
    if not destination:
        need_info_reasons.append("destination")
    if origin and (not is_precise_place(origin)):
        need_info_reasons.append("precision")
    if destination and (not is_precise_place(destination)):
        need_info_reasons.append("precision")


    if GEO_ENABLE and not need_info_reasons:
        # Appels réseau => thread (ne bloque pas l'event loop)
        try:
            distance_km, km_base_to_pickup, km_pickup_to_drop = await asyncio.to_thread(
                compute_trip_distance_km, origin, destination
            )
        except Exception:
            distance_km = None

    # Raffinage IDF via géocodage (si un code postal est détectable).
    # Important : évite les faux positifs quand le texte contient plusieurs villes (ex: Paris + Orléans).
    if GEO_ENABLE:
        try:
            o_geo = await asyncio.to_thread(is_idf_place_geo, origin)
            d_geo = await asyncio.to_thread(is_idf_place_geo, destination)

            origin_idf = o_geo if o_geo is not None else origin_idf_txt
            dest_idf = d_geo if d_geo is not None else dest_idf_txt

            idf_internal = bool(origin_idf and dest_idf)
            zone_tarifaire = "IDF" if idf_internal else "Hors IDF"
        except Exception:
            pass

    # Péage estimé (gratuit) — seulement hors IDF
    if (not idf_internal) and (distance_km is not None) and TOLL_EST_ENABLE:
        toll_est_eur = estimate_toll_eur(distance_km, merged)
    elif distance_km is not None:
        toll_est_eur = 0.0

    # Pricing
    prix: Optional[Decimal] = None
    conditions = build_conditions()

    if idf_internal:
        # Mode strict: IDF aussi => on ne chiffre que si la distance est calculable et les adresses sont précises
        if need_info_reasons or distance_km is None:
            prix = None
        else:
            prix = price_idf(distance_km, urgent)
    else:
        # Hors IDF: si on ne peut pas calculer la distance => on demande les adresses complètes
        if distance_km is None:
            prix = None
        else:
            toll_est_eur = toll_est_eur if toll_est_eur is not None else 0.0
            prix = price_province(distance_km, toll_est_eur, merged, urgent)

    resume = summarize(merged)
    resume_out = resume
    if prix is not None and distance_km is not None:
        if (not is_precise_place(origin)) or (not is_precise_place(destination)):
            resume_out = (
                resume
                + "\n\nNote : montant basé sur la distance routière entre les villes. "
                + "L’adresse exacte peut ajuster légèrement."
            )
    ref = make_ref()
    statut = "en_attente_validation"
    mission_id = create_mission(
            ref=ref,
            client_id=client_id,
            message_id=message_id,
            raw_request=texte,
            merged_request=merged,
            resume_client=resume_out,
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
        "km_base_to_pickup": km_base_to_pickup,
        "km_pickup_to_drop": km_pickup_to_drop,
        "toll_est_eur": toll_est_eur,
        "prix": float(prix) if prix is not None else None,
        "statut": statut,
        "ref": ref,
    })

    # Mode strict: pas de prix => on demande les infos manquantes
    if prix is None:
        missing = need_info_reasons[:] if 'need_info_reasons' in locals() else []
        # Si la distance a échoué malgré des adresses précises, on indique un problème d'itinéraire
        if not missing:
            missing = ['route']
        msg = build_need_info_message(missing, origin, destination, urgent)
        anti_loop_store_last_response(client_id, msg)
        return MissionOut(
            mission_id=mission_id,
            client_id=client_id,
            resume_client=msg,
            prix_recommande_eur=None,
            delai_estime=delai_estime,
            conditions=conditions,
            zone_tarifaire=zone_tarifaire,
            type_detecte=type_detecte,
            statut='need_info',
            ref_mission=ref if mission_id != 0 else None,
        )

        anti_loop_store_last_response(client_id, resume_out)

    return MissionOut(
        mission_id=mission_id,
        client_id=client_id,
        resume_client=resume_out,
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
