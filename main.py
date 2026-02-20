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
BUFFER_WINDOW_SEC = float(os.getenv("BUFFER_WINDOW_SEC", "0").strip() or "0")
# Nettoyage du buffer : on purge les buffers trop anciens.
BUFFER_TTL_SEC = float(os.getenv("BUFFER_TTL_SEC", "120").strip() or "120")

# Anti-boucle : si le même client_id renvoie un message identique à notre réponse, on ignore.
ANTI_LOOP_WINDOW_SEC = float(os.getenv("ANTI_LOOP_WINDOW_SEC", "20").strip() or "20")

# Soft dedup (sans message_id): dédoublonne sur (client_id + hash(texte canonisé)) sur une fenêtre
SOFT_DEDUP_WINDOW_SEC = float(os.getenv("SOFT_DEDUP_WINDOW_SEC", "120").strip() or "120")

# Rate limiting (simple) par client_id
RATE_LIMIT_MAX = int(os.getenv("RATE_LIMIT_MAX", "8").strip() or "8")
RATE_LIMIT_WINDOW_SEC = float(os.getenv("RATE_LIMIT_WINDOW_SEC", "60").strip() or "60")

# GEO / OSRM
GEO_ENABLE = os.getenv("GEO_ENABLE", "1").strip() == "1"
NOMINATIM_BASE_URL = os.getenv("NOMINATIM_BASE_URL", "https://nominatim.openstreetmap.org").strip()
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "").strip()
GEO_TIMEOUT_SEC = float(os.getenv("GEO_TIMEOUT_SEC", "6").strip() or "6")
GEO_CACHE_TTL_DAYS = int(os.getenv("GEO_CACHE_TTL_DAYS", "14").strip() or "14")
GEO_COUNTRYCODES = os.getenv("GEO_COUNTRYCODES", "fr").strip()  # "fr" recommandé
OSRM_BASE_URL = os.getenv("OSRM_BASE_URL", "https://router.project-osrm.org").strip()
OSRM_TIMEOUT_SEC = float(os.getenv("OSRM_TIMEOUT_SEC", "6").strip() or "6")

# Tarification
BASE_DEPART = os.getenv("BASE_DEPART", "Mormant 77720").strip()

# IDF keywords
IDF_KEYWORDS = [
    "paris", "ile-de-france", "île-de-france", "idf",
    "75", "77", "78", "91", "92", "93", "94", "95",
    "seine-et-marne", "yvelines", "essonne", "hauts-de-seine", "seine-saint-denis", "val-de-marne", "val-d'oise",
]

# Prix
# IDF: forfait + majoration urgent
IDF_PRICE_BASE = Decimal(os.getenv("IDF_PRICE_BASE", "70").strip() or "70")
IDF_PRICE_PER_KM = Decimal(os.getenv("IDF_PRICE_PER_KM", "2.2").strip() or "2.2")
IDF_MIN_PRICE = Decimal(os.getenv("IDF_MIN_PRICE", "60").strip() or "60")
IDF_URGENT_SURCHARGE = Decimal(os.getenv("IDF_URGENT_SURCHARGE", "25").strip() or "25")

# Hors IDF: base + €/km + péage estimé (si dispo) + min
OUT_PRICE_BASE = Decimal(os.getenv("OUT_PRICE_BASE", "85").strip() or "85")
OUT_PRICE_PER_KM = Decimal(os.getenv("OUT_PRICE_PER_KM", "1.65").strip() or "1.65")
OUT_MIN_PRICE = Decimal(os.getenv("OUT_MIN_PRICE", "150").strip() or "150")
OUT_URGENT_SURCHARGE = Decimal(os.getenv("OUT_URGENT_SURCHARGE", "35").strip() or "35")

# Péage: estimation simple €/km autoroute (fallback)
TOLL_EUR_PER_KM = Decimal(os.getenv("TOLL_EUR_PER_KM", "0.14").strip() or "0.14")
TOLL_MAX_EUR = Decimal(os.getenv("TOLL_MAX_EUR", "80").strip() or "80")

# Délai estimé
DELAI_URGENT = os.getenv("DELAI_URGENT", "sous 1h").strip() or "sous 1h"
DELAI_STANDARD = os.getenv("DELAI_STANDARD", "dans la journée").strip() or "dans la journée"

# IA
AI_ENABLE = os.getenv("AI_ENABLE", "0").strip() == "1"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
AI_ONLY_WHEN_AMBIGUOUS = os.getenv("AI_ONLY_WHEN_AMBIGUOUS", "1").strip() == "1"

# =========================
# App
# =========================

app = FastAPI(title=APP_NAME)

# =========================
# Utils
# =========================


def _now_ts() -> int:
    return int(time.time())


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()


def normalize_client_id(client_id: str) -> str:
    """Normalise un identifiant client (WhatsApp/Twilio/Make).
    Exemples acceptés: '+336...', '336...', 'whatsapp:+336...', 'whatsapp:336...'
    """
    x = (client_id or "").strip()
    x = re.sub(r"^whatsapp:\s*", "", x, flags=re.IGNORECASE)
    x = x.replace(" ", "")
    # Conserver uniquement les chiffres pour éviter les variations de format
    x = re.sub(r"\D", "", x)
    return x


def require_token(request: Request) -> None:
    if not OCF_TOKEN:
        return
    token = (request.headers.get("X-OCF-TOKEN") or "").strip()
    if token != OCF_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _log(event: str, data: Dict[str, Any]) -> None:
    # Log JSON simple, compatible plateformes
    try:
        payload = {"ts": _utc_iso(), "event": event, **(data or {})}
        print(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


# =========================
# DB
# =========================


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    conn = _db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS missions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT NOT NULL,
            message_id TEXT,
            texte_original TEXT,
            texte_merged TEXT,
            origin TEXT,
            destination TEXT,
            distance_km REAL,
            toll_est_eur REAL,
            prix_recommande_eur REAL,
            delai_estime TEXT,
            zone_tarifaire TEXT,
            type_detecte TEXT,
            statut TEXT,
            ref_mission TEXT,
            created_at INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS message_dedup (
            message_id TEXT PRIMARY KEY,
            client_id TEXT,
            created_at INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS soft_dedup (
            key TEXT PRIMARY KEY,
            created_at INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS buffers (
            client_id TEXT PRIMARY KEY,
            merged_text TEXT,
            last_update INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS anti_loop (
            client_id TEXT PRIMARY KEY,
            last_response_hash TEXT,
            last_ts INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rate_limit (
            client_id TEXT PRIMARY KEY,
            count INTEGER,
            window_start INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS geo_cache (
            address TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            display_name TEXT,
            created_at INTEGER
        )
        """
    )

    conn.commit()
    conn.close()


init_db()


# =========================
# Parsing payload (tolérant)
# =========================

class DemandeMission(BaseModel):
    # Champs attendus, mais on tolère tout
    client_id: Optional[str] = None
    from_: Optional[str] = Field(default=None, alias="from")
    message_id: Optional[str] = None
    texte: Optional[str] = None
    body: Optional[str] = None
    text: Optional[str] = None
    message: Optional[str] = None

    # Meta / Make / WhatsApp variations
    data: Optional[Dict[str, Any]] = None
    payload: Optional[Dict[str, Any]] = None
    entry: Optional[Any] = None

    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow", populate_by_name=True)  # type: ignore
    else:
        class Config:
            extra = "allow"
            allow_population_by_field_name = True


def resolve_payload(p: DemandeMission) -> Tuple[str, str, str, Dict[str, Any]]:
    """
    Retourne (client_id, message_id, texte, diag)
    """
    diag: Dict[str, Any] = {}
    raw = p.__dict__.copy()

    # client_id
    client_id = (p.client_id or p.from_ or "").strip()
    message_id = (p.message_id or "").strip()

    # texte
    texte = (p.texte or p.body or p.text or p.message or "").strip()

    # Essais sur structures imbriquées courantes (Make / Meta / WhatsApp)
    try:
        # Twilio style: From, Body, MessageSid
        if not client_id:
            client_id = str(raw.get("From") or raw.get("from") or raw.get("waId") or raw.get("WaId") or "")
        if not texte:
            texte = str(raw.get("Body") or raw.get("body") or raw.get("Text") or raw.get("text") or "")
        if not message_id:
            message_id = str(raw.get("MessageSid") or raw.get("messageSid") or raw.get("messagesid") or "")
    except Exception:
        pass

    # Meta webhook style
    try:
        if not client_id or not texte or not message_id:
            entry = raw.get("entry") or raw.get("Entry")
            if isinstance(entry, list) and entry:
                changes = entry[0].get("changes") or []
                if isinstance(changes, list) and changes:
                    value = changes[0].get("value") or {}
                    messages = value.get("messages") or []
                    if isinstance(messages, list) and messages:
                        m0 = messages[0]
                        if not client_id:
                            client_id = str(m0.get("from") or "")
                        if not message_id:
                            message_id = str(m0.get("id") or "")
                        if not texte:
                            txt = m0.get("text") or {}
                            texte = str(txt.get("body") or "")
    except Exception:
        pass

    client_id = normalize_client_id(client_id)
    message_id = (message_id or "").strip()

    diag["resolved_client_id"] = client_id
    diag["resolved_message_id"] = message_id
    diag["resolved_texte_len"] = len(texte or "")

    return client_id, message_id, texte, diag


# =========================
# Dédoublonnage / Rate limit / Buffer
# =========================

def dedup_check_and_store(message_id: str, client_id: str) -> bool:
    if not message_id:
        return False
    conn = _db()
    cur = conn.cursor()
    cur.execute("DELETE FROM message_dedup WHERE created_at < ?", (_now_ts() - 86400,))
    cur.execute("SELECT 1 FROM message_dedup WHERE message_id=? LIMIT 1", (message_id,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("INSERT OR REPLACE INTO message_dedup(message_id, client_id, created_at) VALUES(?,?,?)", (message_id, client_id, _now_ts()))
    conn.commit()
    conn.close()
    return exists


def soft_dedup_key(client_id: str, texte: str) -> str:
    canon = re.sub(r"\s+", " ", (texte or "").strip().lower())
    return f"{client_id}:{_sha1(canon)}"


def soft_dedup_check_and_store(client_id: str, texte: str) -> bool:
    key = soft_dedup_key(client_id, texte)
    conn = _db()
    cur = conn.cursor()
    cur.execute("DELETE FROM soft_dedup WHERE created_at < ?", (_now_ts() - int(SOFT_DEDUP_WINDOW_SEC),))
    cur.execute("SELECT 1 FROM soft_dedup WHERE key=? LIMIT 1", (key,))
    exists = cur.fetchone() is not None
    if not exists:
        cur.execute("INSERT OR REPLACE INTO soft_dedup(key, created_at) VALUES(?,?)", (key, _now_ts()))
    conn.commit()
    conn.close()
    return exists


def anti_loop_ignore(client_id: str) -> bool:
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT last_response_hash, last_ts FROM anti_loop WHERE client_id=? LIMIT 1", (client_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    last_ts = int(row["last_ts"] or 0)
    if _now_ts() - last_ts > int(ANTI_LOOP_WINDOW_SEC):
        return False
    return False


def anti_loop_store_last_response(client_id: str, response_text: str) -> None:
    h = _sha1(response_text or "")
    conn = _db()
    conn.execute(
        "INSERT OR REPLACE INTO anti_loop(client_id, last_response_hash, last_ts) VALUES(?,?,?)",
        (client_id, h, _now_ts()),
    )
    conn.commit()
    conn.close()


def rate_limit_or_429(client_id: str) -> None:
    conn = _db()
    cur = conn.cursor()
    now = _now_ts()
    cur.execute("SELECT count, window_start FROM rate_limit WHERE client_id=? LIMIT 1", (client_id,))
    row = cur.fetchone()
    if not row:
        cur.execute("INSERT OR REPLACE INTO rate_limit(client_id, count, window_start) VALUES(?,?,?)", (client_id, 1, now))
        conn.commit()
        conn.close()
        return

    count = int(row["count"] or 0)
    start = int(row["window_start"] or now)
    if now - start > int(RATE_LIMIT_WINDOW_SEC):
        # reset fenêtre
        cur.execute("INSERT OR REPLACE INTO rate_limit(client_id, count, window_start) VALUES(?,?,?)", (client_id, 1, now))
        conn.commit()
        conn.close()
        return

    if count >= RATE_LIMIT_MAX:
        conn.close()
        raise HTTPException(status_code=429, detail="Trop de messages en peu de temps. Réessaie dans 1 minute.")

    cur.execute("UPDATE rate_limit SET count=? WHERE client_id=?", (count + 1, client_id))
    conn.commit()
    conn.close()


def buffer_append_and_maybe_flush(client_id: str, texte: str) -> Tuple[bool, str]:
    """
    Retourne (should_process_now, merged_text)
    """
    if BUFFER_WINDOW_SEC <= 0:
        return True, texte

    now = _now_ts()
    conn = _db()
    cur = conn.cursor()
    # purge anciens
    cur.execute("DELETE FROM buffers WHERE last_update < ?", (now - int(BUFFER_TTL_SEC),))
    cur.execute("SELECT merged_text, last_update FROM buffers WHERE client_id=? LIMIT 1", (client_id,))
    row = cur.fetchone()

    if not row:
        merged = texte
        cur.execute("INSERT OR REPLACE INTO buffers(client_id, merged_text, last_update) VALUES(?,?,?)", (client_id, merged, now))
        conn.commit()
        conn.close()
        return False, merged

    merged_prev = str(row["merged_text"] or "")
    last_update = int(row["last_update"] or now)

    merged = (merged_prev + "\n" + texte).strip() if merged_prev else texte

    if now - last_update < int(BUFFER_WINDOW_SEC):
        # update buffer et on attend
        cur.execute("UPDATE buffers SET merged_text=?, last_update=? WHERE client_id=?", (merged, now, client_id))
        conn.commit()
        conn.close()
        return False, merged

    # fenêtre atteinte => flush (on supprime le buffer)
    cur.execute("DELETE FROM buffers WHERE client_id=?", (client_id,))
    conn.commit()
    conn.close()
    return True, merged


# =========================
# Heuristiques de compréhension
# =========================

URGENT_WORDS = [
    "urgent", "au plus vite", "asap", "tout de suite", "immédiat", "immediat", "ce soir",
    "maintenant", "dans 1h", "dans 1 heure", "rapidement", "express",
]

TYPE_KEYWORDS = {
    "livraison": ["livraison", "livrer", "déposer", "deposer", "colis", "paquet", "envoi"],
    "courses": ["courses", "acheter", "supermarché", "supermarche", "carrefour", "auchan", "leclerc", "monoprix"],
    "transport": ["transport", "déménagement", "demenagement", "camion", "utilitaire", "chargement"],
    "conciergerie": ["conciergerie", "service", "aide", "assistance", "récupérer", "recuperer"],
}


def is_urgent(texte: str) -> bool:
    t = (texte or "").lower()
    return any(w in t for w in URGENT_WORDS)


def detect_type(texte: str) -> str:
    t = (texte or "").lower()
    for k, kws in TYPE_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return k
    return "livraison"


def summarize(texte: str, max_len: int = 160) -> str:
    t = re.sub(r"\s+", " ", (texte or "").strip())
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def extract_route(texte: str) -> Tuple[str, str]:
    """
    Heuristique simple: détecte "A -> B" ou "A vers B" etc.
    """
    t = (texte or "").strip()
    # Flèches / séparateurs
    m = re.search(r"(.+?)\s*(?:->|→|=>)\s*(.+)", t)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    m = re.search(r"(.+?)\s+(?:vers|pour|à|a)\s+(.+)", t, flags=re.IGNORECASE)
    if m:
        # attention: risque de faux positifs, mais utile en fallback
        a = m.group(1).strip()
        b = m.group(2).strip()
        # limiter si trop long
        if len(a) > 2 and len(b) > 2:
            return a, b

    return "", ""


def _canon_addr(addr: str) -> str:
    a = re.sub(r"\s+", " ", (addr or "").strip())
    return a


def is_idf_text(addr: str) -> bool:
    a = (addr or "").lower()
    return any(k in a for k in IDF_KEYWORDS)


# =========================
# HTTP helpers
# =========================

def _http_get_json(url: str, timeout: float = 6.0) -> Any:
    headers = {
        "User-Agent": f"{APP_NAME}/1.0 (contact: {NOMINATIM_EMAIL or 'n/a'})"
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="ignore")
        return json.loads(data)


# =========================
# GEO / OSRM
# =========================

_last_nominatim_ts = 0.0


def _throttle_nominatim() -> None:
    global _last_nominatim_ts
    # règle de politesse: 1 req/sec
    now = time.time()
    dt = now - _last_nominatim_ts
    if dt < 1.05:
        time.sleep(1.05 - dt)
    _last_nominatim_ts = time.time()


def geocode_address(addr: str) -> Optional[Tuple[float, float, str]]:
    if not GEO_ENABLE:
        return None

    # Nettoie les mentions de timing/urgence qui cassent le géocodage (ex: "Torcy ce soir")
    addr = re.sub(r"\b(?:ce\s+soir|aujourd['’]hui|demain|urgent|immédiat|immediat|tout\s+de\s+suite|rapidement)\b", " ", addr or "", flags=re.IGNORECASE)
    addr = re.sub(r"\s{2,}", " ", addr).strip()
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


def osrm_route_km(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
    # OSRM expects lon,lat
    coords = f"{lon1},{lat1};{lon2},{lat2}"
    url = f"{OSRM_BASE_URL}/route/v1/driving/{coords}?overview=false"
    try:
        res = _http_get_json(url, timeout=OSRM_TIMEOUT_SEC)
        routes = res.get("routes") if isinstance(res, dict) else None
        if not routes:
            return None
        dist_m = float(routes[0].get("distance") or 0.0)
        if dist_m <= 0:
            return None
        return dist_m / 1000.0
    except Exception:
        return None


def estimate_toll(distance_km: float) -> Decimal:
    # Estimation simple proportionnelle
    d = Decimal(str(distance_km or 0))
    est = (d * TOLL_EUR_PER_KM).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if est > TOLL_MAX_EUR:
        est = TOLL_MAX_EUR
    if est < Decimal("0"):
        est = Decimal("0")
    return est


# =========================
# IA (optionnelle)
# =========================

class AIInfo(BaseModel):
    origin: Optional[str] = None
    destination: Optional[str] = None
    mission_type: Optional[str] = None
    confidence: Optional[float] = None

    if ConfigDict is not None:
        model_config = ConfigDict(extra="ignore")  # type: ignore
    else:
        class Config:
            extra = "ignore"


def is_route_ambiguous(texte: str, origin: str, destination: str) -> bool:
    # Ambigu si on n'a pas de route claire
    if origin and destination:
        return False
    t = (texte or "").strip()
    if len(t) < 10:
        return True
    # Beaucoup de mots, pas de séparateur
    if not re.search(r"(->|→|=>|vers)", t, flags=re.IGNORECASE):
        return True
    return True


def ai_should_apply(ai: Optional[AIInfo]) -> bool:
    if not ai:
        return False
    try:
        c = float(ai.confidence or 0)
    except Exception:
        c = 0.0
    return c >= 0.55 and (ai.origin or ai.destination or ai.mission_type)


def ai_extract_fields(texte: str) -> Optional[AIInfo]:
    if not AI_ENABLE or OpenAI is None:
        return None
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        return None

    client = OpenAI(api_key=key)

    system = (
        "Tu es un extracteur d'informations pour une demande de livraison/mission.\n"
        "Retourne uniquement un JSON minifié avec les clés: origin, destination, mission_type, confidence.\n"
        "origin et destination doivent être des adresses/lieux en France (ou vide si inconnu).\n"
        "mission_type dans {livraison, courses, transport, conciergerie}.\n"
        "confidence entre 0 et 1.\n"
    )
    user = f"Texte client:\n{texte}\n"

    try:
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.1,
            max_tokens=120,
        )
        content = resp.choices[0].message.content or ""
        # JSON tolerant
        jtxt = content.strip()
        # Try to locate JSON
        m = re.search(r"\{.*\}", jtxt, flags=re.S)
        if m:
            jtxt = m.group(0)
        data = json.loads(jtxt)
        return AIInfo(**data)
    except Exception:
        return None


# =========================
# Pricing
# =========================

def _round2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute_price(distance_km: Optional[float], urgent: bool, idf_internal: bool) -> Tuple[Decimal, Optional[Decimal], str]:
    """
    Retourne (prix_total, toll_est, zone_tarifaire)
    """
    zone = "IDF" if idf_internal else "Hors IDF"

    if distance_km is None or distance_km <= 0:
        # fallback minimum
        if idf_internal:
            price = IDF_MIN_PRICE
        else:
            price = OUT_MIN_PRICE
        if urgent:
            price = _round2(price + (IDF_URGENT_SURCHARGE if idf_internal else OUT_URGENT_SURCHARGE))
        return _round2(price), None, zone

    d = Decimal(str(distance_km))
    if idf_internal:
        price = IDF_PRICE_BASE + (d * IDF_PRICE_PER_KM)
        if price < IDF_MIN_PRICE:
            price = IDF_MIN_PRICE
        if urgent:
            price += IDF_URGENT_SURCHARGE
        return _round2(price), None, zone

    # Hors IDF: ajoute estimation péage
    toll = estimate_toll(distance_km)
    price = OUT_PRICE_BASE + (d * OUT_PRICE_PER_KM) + toll
    if price < OUT_MIN_PRICE:
        price = OUT_MIN_PRICE
    if urgent:
        price += OUT_URGENT_SURCHARGE
    return _round2(price), _round2(toll), zone


# =========================
# API models
# =========================

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
    ref_mission: Optional[str] = None


# =========================
# Routes
# =========================

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "app": APP_NAME, "ts": _utc_iso()}


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
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client=(
                f"{resume_buf}\n\n"
                "Message reçu. Ajoute les infos manquantes (adresses complètes, contraintes, volume/fragile, "
                "aller-retour, etc.) et je calcule le montant."
            ),
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
            "origin": origin,
            "destination": destination,
            "type_detecte": type_detecte,
            "confidence": getattr(ai_info, "confidence", None),
        })

    # GEO: si origin/destination manquants, on utilise BASE_DEPART comme origine
    origin_eff = origin or BASE_DEPART
    destination_eff = destination or ""

    # Géocode
    geo_o = geocode_address(origin_eff) if origin_eff else None
    geo_d = geocode_address(destination_eff) if destination_eff else None

    distance_km: Optional[float] = None
    if geo_o and geo_d:
        lat1, lon1, _ = geo_o
        lat2, lon2, _ = geo_d
        distance_km = osrm_route_km(lat1, lon1, lat2, lon2)

    # Affiner IDF via résultat géocode (display_name)
    # (optionnel mais utile quand le texte n'a pas de 75/77 etc.)
    if geo_o and geo_d:
        disp_o = (geo_o[2] or "").lower()
        disp_d = (geo_d[2] or "").lower()
        origin_idf_geo = any(k in disp_o for k in IDF_KEYWORDS)
        dest_idf_geo = any(k in disp_d for k in IDF_KEYWORDS)
        if origin_idf_geo and dest_idf_geo:
            idf_internal = True
            zone_tarifaire = "IDF"

    prix, toll, zone_tarifaire = compute_price(distance_km, urgent, idf_internal)

    # Conditions (exemple simple)
    conditions = (
        "Paiement 100% à l'avance.\n"
        "Preuve de prise en charge et de livraison.\n"
        "Annulation : si l'intervention a déjà commencé, facturation partielle possible."
    )

    # Construction réponse client
    resume = summarize(merged)
    parts = [
        "Bonjour ! 👋",
        "Voici mon offre pour votre demande :",
        "",
        f"📝 {resume}",
        f"💰 Prix recommandé : {prix} €",
        f"⏱️ Délai estimé : {delai_estime}",
    ]
    if toll is not None:
        parts.append(f"🛣️ Péage estimé : {toll} € (inclus)")
    parts.append(f"📌 Conditions : {conditions.splitlines()[0]}")
    parts.append("")
    parts.append("👉 Pour confirmer et payer :")
    parts.append("[lien Stripe]")
    parts.append("")
    parts.append("Merci et à très vite 😊")
    parts.append(f"• {APP_NAME}")

    resume_client = "\n".join(parts)

    ref_mission = str(uuid.uuid4())[:8]
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO missions(
            client_id, message_id, texte_original, texte_merged, origin, destination,
            distance_km, toll_est_eur, prix_recommande_eur, delai_estime, zone_tarifaire,
            type_detecte, statut, ref_mission, created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            client_id,
            message_id or None,
            texte,
            merged,
            origin_eff,
            destination_eff,
            float(distance_km) if distance_km is not None else None,
            float(toll) if toll is not None else None,
            float(prix),
            delai_estime,
            zone_tarifaire,
            type_detecte,
            "quoted",
            ref_mission,
            _now_ts(),
        )
    )
    mission_id = int(cur.lastrowid or 0)
    conn.commit()
    conn.close()

    anti_loop_store_last_response(client_id, resume_client)

    _log("mission_quoted", {
        "mission_id": mission_id,
        "client_id": client_id,
        "message_id": message_id,
        "origin": origin_eff,
        "destination": destination_eff,
        "distance_km": distance_km,
        "toll_est": float(toll) if toll is not None else None,
        "prix": float(prix),
        "zone": zone_tarifaire,
        "urgent": urgent,
        "type": type_detecte,
        "ref_mission": ref_mission,
    })

    return MissionOut(
        mission_id=mission_id,
        client_id=client_id,
        resume_client=resume_client,
        prix_recommande_eur=float(prix),
        delai_estime=delai_estime,
        conditions=conditions,
        zone_tarifaire=zone_tarifaire,
        type_detecte=type_detecte,
        statut="quoted",
        ref_mission=ref_mission,
    )
