import os
import re
import json
import math
import time
import uuid
import sqlite3
import hashlib
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

import requests
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel


# =========================
# Config
# =========================

APP_NAME = "OuiCestFait"
DB_PATH = os.getenv("DB_PATH", "data.sqlite")

# Dédoublonnage (évite les doubles envois si Meta/Make retry)
DEDUP_TTL_SEC = int(os.getenv("DEDUP_TTL_SEC", "7200"))  # 2h

# Soft-dedup (si Make renvoie un message sans message_id ou quasi-identique)
SOFT_DEDUP_WINDOW_SEC = int(os.getenv("SOFT_DEDUP_WINDOW_SEC", "30"))

# Anti-boucle (si le bot reçoit ses propres messages)
SELF_LOOP_GUARD = os.getenv("SELF_LOOP_GUARD", "1") == "1"
BOT_NUMBERS = set(
    x.strip() for x in os.getenv("BOT_NUMBERS", "").split(",") if x.strip()
)

# Pricing
BASE_FEE_EUR = float(os.getenv("BASE_FEE_EUR", "25"))         # prise en charge
EUR_PER_KM = float(os.getenv("EUR_PER_KM", "1.2"))            # variable km
MIN_PRICE_EUR = float(os.getenv("MIN_PRICE_EUR", "35"))
ROUND_TO = float(os.getenv("ROUND_TO", "1"))                  # arrondi (1€ par défaut)

# Simple ETA rule of thumb (can be overridden by your own logic)
DEFAULT_ETA_SHORT = "sous 1h"
DEFAULT_ETA_LONG = "dans la journée"

# Optional: if you use an external geocoding API (OpenRouteService, Google, etc.)
# Here we keep placeholders to not break your current setup.
GEOCODER = os.getenv("GEOCODER", "nominatim")  # "nominatim" or "none"
NOMINATIM_URL = os.getenv("NOMINATIM_URL", "https://nominatim.openstreetmap.org/search")

# Auth (optional)
API_TOKEN = os.getenv("API_TOKEN", "")

# Buffering (fusion de messages successifs)
BUFFER_ENABLED = os.getenv("BUFFER_ENABLED", "1") == "1"
BUFFER_MAX_SEC = int(os.getenv("BUFFER_MAX_SEC", "45"))
BUFFER_JOINER = os.getenv("BUFFER_JOINER", "\n")


# =========================
# FastAPI app
# =========================

app = FastAPI(title=APP_NAME)


# =========================
# Models
# =========================

class MissionIn(BaseModel):
    client_id: str
    texte: str
    message_id: Optional[str] = None


class MissionOut(BaseModel):
    mission_id: int
    client_id: str
    resume_client: str
    should_reply: bool = True
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
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _now_ts() -> int:
    return int(time.time())


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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
    CREATE TABLE IF NOT EXISTS message_response_cache (
        message_id TEXT PRIMARY KEY,
        response_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mrc_created ON message_response_cache(created_at)")

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
        v TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()


init_db()


# =========================
# DEDUP + CACHE
# =========================

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


def cache_get_response(message_id: Optional[str]) -> Optional[MissionOut]:
    """Retourne la réponse précédemment calculée pour ce message_id (si Make/Meta renvoie le même message)."""
    if not message_id:
        return None
    now = _now_ts()
    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM message_response_cache WHERE created_at < ?", (now - DEDUP_TTL_SEC,))
        cur.execute("SELECT response_json FROM message_response_cache WHERE message_id = ? LIMIT 1", (message_id,))
        row = cur.fetchone()
        if not row:
            conn.commit()
            return None
        conn.commit()
        try:
            data = json.loads(row[0])
            return MissionOut(**data)
        except Exception:
            return None
    finally:
        conn.close()


def cache_store_response(message_id: Optional[str], out: MissionOut) -> None:
    if not message_id:
        return
    now = _now_ts()
    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR REPLACE INTO message_response_cache(message_id, response_json, created_at) VALUES(?,?,?)",
            (message_id, out.model_dump_json(), now),
        )
        conn.commit()
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

    purge_before = now
    try:
        cur.execute("DELETE FROM soft_dedup WHERE created_at < ?", (purge_before - 300,))
        cur.execute("SELECT 1 FROM soft_dedup WHERE k = ? LIMIT 1", (k,))
        if cur.fetchone():
            conn.commit()
            return True
        cur.execute("INSERT OR REPLACE INTO soft_dedup(k, created_at) VALUES(?,?)", (k, now))
        conn.commit()
        return False
    finally:
        conn.close()


# =========================
# Buffer: fusionner messages proches (si user envoie en 2 fois)
# =========================

def buffer_add_and_maybe_flush(client_id: str, texte: str) -> Tuple[bool, str]:
    """
    Retourne (flushed, merged_text).
    - flushed=True => on doit traiter merged_text maintenant
    - flushed=False => on attend encore (pas de réponse)
    """
    if not BUFFER_ENABLED:
        return True, texte

    now = _now_ts()
    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT buffer_text, first_at, last_at FROM client_message_buffer WHERE client_id = ?",
            (client_id,),
        )
        row = cur.fetchone()

        if row is None:
            cur.execute(
                "INSERT OR REPLACE INTO client_message_buffer(client_id, buffer_text, first_at, last_at) VALUES(?,?,?,?)",
                (client_id, texte, now, now),
            )
            conn.commit()
            return False, texte

        buffer_text, first_at, last_at = row
        # If too old, flush previous and start new
        if now - last_at > BUFFER_MAX_SEC:
            # flush old buffer; start new with current
            merged = buffer_text
            cur.execute(
                "INSERT OR REPLACE INTO client_message_buffer(client_id, buffer_text, first_at, last_at) VALUES(?,?,?,?)",
                (client_id, texte, now, now),
            )
            conn.commit()
            return True, merged

        # else merge into buffer
        merged_buf = buffer_text + BUFFER_JOINER + texte
        cur.execute(
            "UPDATE client_message_buffer SET buffer_text = ?, last_at = ? WHERE client_id = ?",
            (merged_buf, now, client_id),
        )
        conn.commit()
        return False, merged_buf
    finally:
        conn.close()


def buffer_force_flush(client_id: str) -> Optional[str]:
    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT buffer_text FROM client_message_buffer WHERE client_id = ?",
            (client_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("DELETE FROM client_message_buffer WHERE client_id = ?", (client_id,))
        conn.commit()
        return row[0]
    finally:
        conn.close()


# =========================
# Text cleaning
# =========================

STOP_WORDS_FOR_GEOCODE = [
    r"\baujourd['’]hui\b",
    r"\bce\s+soir\b",
    r"\bdemain\b",
    r"\burgent(?:e)?\b",
    r"\brasap\b",
    r"\bsvp\b",
    r"\bplease\b",
    r"\bmaintenant\b",
]

def strip_time_words(s: str) -> str:
    out = s
    for pat in STOP_WORDS_FOR_GEOCODE:
        out = re.sub(pat, " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s{2,}", " ", out).strip()
    return out


# =========================
# Parse mission (simple heuristics)
# =========================

ARROW_PAT = re.compile(r"\s*(?:->|→|vers)\s*", flags=re.IGNORECASE)

def extract_origin_destination(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Heuristique:
    - si contient '->' / '→' / 'vers' => split en 2
    - sinon si contient 2 adresses (lignes) => 1ère = origin, 2ème = destination
    """
    t = text.strip()
    if not t:
        return None, None

    if "->" in t or "→" in t or re.search(r"\bvers\b", t, flags=re.IGNORECASE):
        parts = ARROW_PAT.split(t, maxsplit=1)
        if len(parts) == 2:
            o = parts[0].strip(" ,;-")
            d = parts[1].strip(" ,;-")
            return o if o else None, d if d else None

    lines = [x.strip() for x in t.splitlines() if x.strip()]
    if len(lines) >= 2:
        return lines[0], lines[1]

    return None, None


def looks_like_address(s: str) -> bool:
    if not s:
        return False
    # simple: has a number + street-like word
    return bool(re.search(r"\b\d{1,4}\b", s)) and bool(re.search(r"\b(rue|avenue|av\.|bd|boulevard|place|chemin|impasse|route)\b", s, re.I))


def needs_more_info(origin: Optional[str], destination: Optional[str]) -> bool:
    if not origin or not destination:
        return True
    # if not address-like and too short => ask more info
    if len(origin) < 3 or len(destination) < 3:
        return True
    return False


# =========================
# Geocoding + distance (optional / best-effort)
# =========================

def geocode_nominatim(q: str) -> Optional[Tuple[float, float]]:
    if not q:
        return None
    q = strip_time_words(q)
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": q, "format": "json", "limit": 1},
            headers={"User-Agent": f"{APP_NAME}/1.0"},
            timeout=12,
        )
        r.raise_for_status()
        data = r.json()
        if not data:
            return None
        return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        return None


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    x = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(x), math.sqrt(1-x))
    return R * c


def compute_distance_km(origin: str, destination: str) -> Optional[float]:
    if GEOCODER == "none":
        return None
    if GEOCODER == "nominatim":
        o = geocode_nominatim(origin)
        d = geocode_nominatim(destination)
        if not o or not d:
            return None
        return haversine_km(o, d)
    return None


# =========================
# Pricing
# =========================

def round_price(x: float) -> float:
    if ROUND_TO <= 0:
        return float(x)
    return float(round(x / ROUND_TO) * ROUND_TO)


def price_from_distance(distance_km: Optional[float]) -> float:
    if distance_km is None:
        # fallback minimal if no distance
        return float(max(MIN_PRICE_EUR, BASE_FEE_EUR))
    raw = BASE_FEE_EUR + EUR_PER_KM * float(distance_km)
    raw = max(raw, MIN_PRICE_EUR)
    return round_price(raw)


def estimate_eta(distance_km: Optional[float]) -> str:
    if distance_km is None:
        return DEFAULT_ETA_LONG
    return DEFAULT_ETA_SHORT if distance_km <= 20 else DEFAULT_ETA_LONG


def detect_zone(origin: Optional[str], destination: Optional[str]) -> Optional[str]:
    # simple placeholder
    if not origin or not destination:
        return None
    o = origin.lower()
    d = destination.lower()
    if "paris" in o and "paris" in d:
        return "intra-paris"
    if "paris" in o or "paris" in d:
        return "idf"
    return "france"


def detect_type(text: str) -> Optional[str]:
    t = text.lower()
    if "enlèvement" in t or "enlevement" in t:
        return "enlevement_livraison"
    if "livraison" in t:
        return "livraison"
    if "course" in t:
        return "course"
    return "transport"


# =========================
# Auth
# =========================

def check_token(authorization: Optional[str]) -> None:
    if not API_TOKEN:
        return
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization")
    if authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1].strip()
    else:
        token = authorization.strip()
    if token != API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


# =========================
# API
# =========================

@app.post("/mission/demande", response_model=MissionOut)
def mission_demande(payload: MissionIn, authorization: Optional[str] = Header(default=None)):
    check_token(authorization)

    client_id = (payload.client_id or "").strip()
    texte = (payload.texte or "").strip()
    message_id = (payload.message_id or "").strip() or None

    if not client_id or not texte:
        raise HTTPException(status_code=400, detail="client_id et texte requis")

    # Anti-boucle: si on reçoit nos propres messages (optionnel)
    if SELF_LOOP_GUARD and client_id in BOT_NUMBERS:
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
            should_reply=False,
        )

    if dedup_check_and_store(message_id, client_id):
        # Si Make/Meta a renvoyé le même message (retry), on renvoie la même réponse (idempotent).
        cached = cache_get_response(message_id)
        if cached is not None:
            return cached
        # fallback : ne pas répondre (évite un message vide côté WhatsApp)
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="",
            should_reply=False,
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="duplicate",
            ref_mission=None,
        )

    # Soft-dedup si pas de message_id (ou instable)
    if soft_dedup_check_and_store(client_id, texte):
        # On renvoie silencieusement rien (mais Make enverra quand même si tu n'ajoutes pas de filtre)
        # => au moins, on ne renvoie pas "Message déjà traité" qui fait moche.
        out = MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="",
            should_reply=False,
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="duplicate_soft",
            ref_mission=None,
        )
        cache_store_response(message_id, out)
        return out

    # Buffering: fusionner messages rapprochés
    flushed, merged = buffer_add_and_maybe_flush(client_id, texte)
    if not flushed:
        # On attend encore; pas de réponse.
        out = MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="",
            should_reply=False,
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="buffering",
            ref_mission=None,
        )
        cache_store_response(message_id, out)
        return out

    raw_request = texte
    merged_request = merged

    type_detecte = detect_type(merged_request)

    # Parse origin/destination
    origin, destination = extract_origin_destination(merged_request)

    zone_tarifaire = detect_zone(origin, destination)

    if needs_more_info(origin, destination):
        delai_estime = DEFAULT_ETA_LONG
        resume_buf = merged_request.strip()
        out = MissionOut(
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
            statut="need_info",
            ref_mission=None,
            should_reply=True,
        )
        cache_store_response(message_id, out)
        return out

    # Distance (best effort)
    dist_km = compute_distance_km(origin, destination)

    prix = price_from_distance(dist_km)
    delai_estime = estimate_eta(dist_km)

    conditions = "Paiement 100% à l'avance. Preuve de prise en charge et de livraison."

    # Create mission
    ref = uuid.uuid4().hex[:8].upper()
    now = _now_iso()
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO missions(ref, client_id, message_id, raw_request, merged_request, resume_client,
                            type_detecte, zone_tarifaire, delai_estime, prix_recommande_eur, conditions,
                            origin, destination, distance_km, toll_est_eur, statut, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            ref,
            client_id,
            message_id,
            raw_request,
            merged_request,
            "",
            type_detecte,
            zone_tarifaire,
            delai_estime,
            float(prix),
            conditions,
            origin,
            destination,
            float(dist_km) if dist_km is not None else None,
            None,
            "propose",
            now,
            now,
        ),
    )
    mission_id = int(cur.lastrowid)
    conn.commit()
    conn.close()

    resume_client = f"{origin} → {destination}"
    resume_out = resume_client

    out = MissionOut(
        mission_id=mission_id,
        client_id=client_id,
        resume_client=resume_out,
        prix_recommande_eur=float(prix) if prix is not None else None,
        delai_estime=delai_estime,
        conditions=conditions,
        zone_tarifaire=zone_tarifaire,
        type_detecte=type_detecte,
        statut="propose",
        ref_mission=ref if mission_id != 0 else None,
        should_reply=True,
    )
    cache_store_response(message_id, out)
    return out


@app.post("/mission/{mission_id}/status")
def set_status(mission_id: int, statut: str, authorization: Optional[str] = Header(default=None)):
    check_token(authorization)
    statut = (statut or "").strip()
    if not statut:
        raise HTTPException(status_code=400, detail="statut requis")

    now = _now_iso()
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM missions WHERE id = ?", (mission_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="mission introuvable")

    cur.execute("UPDATE missions SET statut = ?, updated_at = ? WHERE id = ?", (statut, now, mission_id))
    conn.commit()
    conn.close()
    return {"ok": True, "mission_id": mission_id, "statut": statut}
