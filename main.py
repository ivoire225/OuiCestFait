# main.py — OuiCestFait (Option A, robuste Make/WhatsApp)
# Fixes:
# - Plus de champs vides: même en "buffering", on renvoie prix/délai/conditions calculés.
# - Buffer désactivé par défaut (CLIENT_BUFFER_WINDOW_SEC=0), réactivable via variable d'env.
# - Tolérance forte sur les payloads (from/From, Body content, Messages[], webhook Meta).
# - Déduplication: message_id si fourni + "soft dedup" si message_id absent.
# - Support JSON + form-data + texte brut.

from __future__ import annotations

import os
import re
import json
import time
import sqlite3
import hashlib
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
# Config
# =========================

APP_NAME = "OuiCestFait"
DB_PATH = os.getenv("DB_PATH", "ouicestfait.db")

# Par défaut: 0 => offre immédiate.
# Pour regrouper plusieurs messages: mets par exemple 15 ou 25 sur Railway.
CLIENT_BUFFER_WINDOW_SEC = int(os.getenv("CLIENT_BUFFER_WINDOW_SEC", "0"))

# Anti-boucle optionnel si tu connais ton propre numéro (sans + ni espaces)
WHATSAPP_OWN_NUMBER = os.getenv("WHATSAPP_OWN_NUMBER", "").strip()

# Pricing (base)
MIN_PRICE_EUR = Decimal(os.getenv("MIN_PRICE_EUR", "25"))
URGENT_BASE_EUR = Decimal(os.getenv("URGENT_BASE_EUR", "150"))
LONG_DISTANCE_BASE_EUR = Decimal(os.getenv("LONG_DISTANCE_BASE_EUR", "300"))

# Soft-dedup quand message_id n'est pas fourni
SOFT_DEDUP_WINDOW_SEC = int(os.getenv("SOFT_DEDUP_WINDOW_SEC", "12"))

# Debug (log en DB)
DEBUG = os.getenv("DEBUG", "0") == "1"
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "").strip()


# =========================
# App
# =========================

app = FastAPI(title=f"{APP_NAME} API", version="1.3.2")


# =========================
# Modèles Pydantic
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


class MissionHistoryItem(BaseModel):
    mission_id: int
    created_at: str
    statut: str
    resume_client: str
    prix_recommande_eur: Optional[float] = None
    delai_estime: Optional[str] = None
    type_detecte: Optional[str] = None


# =========================
# SQLite helpers
# =========================

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    conn = _db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS missions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id TEXT NOT NULL,
        raw_request TEXT NOT NULL,
        merged_request TEXT NOT NULL,
        resume_client TEXT,
        type_detecte TEXT,
        zone_tarifaire TEXT,
        delai_estime TEXT,
        prix_recommande_eur REAL,
        conditions TEXT,
        statut TEXT NOT NULL DEFAULT 'en_attente_validation',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS message_dedup (
        message_id TEXT PRIMARY KEY,
        client_id TEXT,
        created_at TEXT NOT NULL
    )
    """)

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS soft_dedup (
        k TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_missions_client_id ON missions(client_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_missions_created_at ON missions(created_at)")

    conn.commit()
    conn.close()


init_db()


# =========================
# Utils
# =========================

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


def normalize_client_id(client_id: str) -> str:
    x = (client_id or "").strip()
    x = x.replace(" ", "").replace("+", "")
    return x


def safe_round_eur(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


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
    cid: Optional[Any] = payload.client_id or _get_any_fuzzy(raw, "client_id", "clientId", "from", "From", "wa_id", "waId", "sender")
    txt: Optional[Any] = payload.texte or _get_any_fuzzy(raw, "texte", "text", "Text", "body", "Body", "Body content", "content", "message")

    if isinstance(txt, dict):
        txt = _get_any_fuzzy(txt, "body", "Body", "text", "Text")

    for wrapper_key in ("data", "payload", "body", "request", "input"):
        w = _get_any_fuzzy(raw, wrapper_key)
        if isinstance(w, dict):
            if not msg_id:
                msg_id = _get_any_fuzzy(w, "message_id", "messageId", "id", "Message ID")
            if not cid:
                cid = _get_any_fuzzy(w, "client_id", "from", "From", "wa_id", "sender")
            if (not txt or str(txt).strip() == ""):
                t2 = _get_any_fuzzy(w, "texte", "text", "body", "Body", "Body content", "message", "content")
                if isinstance(t2, dict):
                    t2 = _get_any_fuzzy(t2, "body", "Body")
                txt = t2 or txt

    messages = _get_any_fuzzy(raw, "Messages", "messages")
    m0 = _first_in_list(messages)
    if isinstance(m0, dict):
        if not msg_id:
            msg_id = _get_any_fuzzy(m0, "id", "message_id", "messageId", "Message ID")
        if not cid:
            cid = _get_any_fuzzy(m0, "from", "From", "wa_id", "sender", "client_id")
        if (not txt or str(txt).strip() == ""):
            txt = (
                _deep_get(m0, ["Text", "Body"])
                or _deep_get(m0, ["text", "body"])
                or _get_any_fuzzy(m0, "Body", "body", "text", "Text", "Body content")
                or ""
            )

    contacts = _get_any_fuzzy(raw, "Contacts", "contacts", "contact")
    c0 = _first_in_list(contacts)
    if isinstance(c0, dict) and not cid:
        cid = _get_any_fuzzy(c0, "wa_id", "waId", "WhatsApp ID", "from", "From", "id")

    # Webhook Meta
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
# Détection + pricing
# =========================

def detect_intent(text: str) -> Dict[str, Any]:
    t = (text or "").lower()

    if any(k in t for k in ["livraison", "colis", "document", "dossier", "remettre", "déposer", "deposer", "récupérer", "recuperer"]):
        type_detecte = "livraison"
    elif any(k in t for k in ["courses", "supermarché", "supermarche", "acheter", "liste de courses"]):
        type_detecte = "courses"
    elif any(k in t for k in ["transport", "déménagement", "demenagement", "véhicule", "vehicule", "conduire"]):
        type_detecte = "transport"
    else:
        type_detecte = "conciergerie"

    urgent = any(k in t for k in ["urgent", "urgence", "rapidement", "tout de suite", "sous 1h", "immédiat", "immediat", "aujourd'hui", "aujourdhui", "ce soir", "dans l'heure"])
    delai_estime = "sous 1h" if urgent else "dans la journée"

    zone_tarifaire = "IDF/Paris" if any(k in t for k in ["paris", "75", "île-de-france", "ile-de-france", "idf"]) else None

    return {
        "type_detecte": type_detecte,
        "urgent": urgent,
        "delai_estime": delai_estime,
        "zone_tarifaire": zone_tarifaire,
    }


def _extract_city_pair(text: str) -> Optional[Tuple[str, str]]:
    if not text:
        return None
    s = text

    m = re.search(r"([A-Za-zÀ-ÿ'\- ]+)\s*(?:->|→)\s*([A-Za-zÀ-ÿ'\- ]+)", s)
    if m:
        return (m.group(1).strip(), m.group(2).strip())

    m = re.search(r"(?:de|depuis)\s+([A-Za-zÀ-ÿ'\- ]+)\s+(?:à|a|vers)\s+([A-Za-zÀ-ÿ'\- ]+)", s, re.IGNORECASE)
    if m:
        return (m.group(1).strip(), m.group(2).strip())

    m = re.search(r"(?:récupérer|recuperer|prendre|pickup|pick up).{0,60}?\b(?:à|a)\s+([A-Za-zÀ-ÿ'\- ]+).{0,120}?\b(?:livrer|deliver|déposer|deposer|remettre).{0,60}?\b(?:à|a)\s+([A-Za-zÀ-ÿ'\- ]+)", s, re.IGNORECASE)
    if m:
        return (m.group(1).strip(), m.group(2).strip())

    m = re.search(r"\b(?:à|a)\s+([A-Za-zÀ-ÿ'\- ]+).{0,80}?\b(?:à|a|vers)\s+([A-Za-zÀ-ÿ'\- ]+)", s, re.IGNORECASE)
    if m:
        return (m.group(1).strip(), m.group(2).strip())

    return None


def _simple_pricing(text: str, intent: Dict[str, Any]) -> Tuple[Decimal, str, str]:
    t = text or ""
    urgent = intent["urgent"]
    type_detecte = intent["type_detecte"]

    resume = t.strip()
    if len(resume) > 120:
        resume = resume[:117] + "..."

    price = URGENT_BASE_EUR if urgent else Decimal("80")

    long_distance = False
    pair = _extract_city_pair(t)
    if pair:
        frm, to = pair
        if frm.strip().lower() != to.strip().lower():
            big = {"paris","lyon","marseille","lille","bordeaux","nantes","toulouse","strasbourg","orleans","nice","rennes","grenoble","montpellier"}
            if (frm.strip().lower() in big) or (to.strip().lower() in big):
                long_distance = True
    if long_distance:
        price = max(price, LONG_DISTANCE_BASE_EUR)

    if type_detecte == "courses":
        price += Decimal("30")
    elif type_detecte == "transport":
        price += Decimal("80")

    price = max(price, MIN_PRICE_EUR)
    price = safe_round_eur(price)

    conditions = (
        "Paiement 100% à l'avance.\n"
        "Preuve de prise en charge et de livraison (photo / signature / message).\n"
        "Annulation : si l'intervention a déjà commencé, facturation partielle possible.\n"
        "Objet/documents à remettre dans une enveloppe/packaging adapté (si nécessaire)."
    )

    return (price, resume, conditions)


# =========================
# Dédup & buffer
# =========================

def anti_loop_ignore(client_id: str) -> bool:
    if not WHATSAPP_OWN_NUMBER:
        return False
    return normalize_client_id(client_id) == normalize_client_id(WHATSAPP_OWN_NUMBER)


def dedup_check_and_store(message_id: Optional[str], client_id: str) -> bool:
    if not message_id:
        return False
    conn = _db()
    cur = conn.cursor()

    cur.execute("SELECT message_id FROM message_dedup WHERE message_id = ?", (message_id,))
    if cur.fetchone():
        conn.close()
        return True

    cur.execute(
        "INSERT INTO message_dedup(message_id, client_id, created_at) VALUES(?,?,?)",
        (message_id, client_id, utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return False


def soft_dedup_check_and_store(client_id: str, texte: str) -> bool:
    if SOFT_DEDUP_WINDOW_SEC <= 0:
        return False

    now = int(time.time())
    bucket = now // SOFT_DEDUP_WINDOW_SEC
    key_raw = f"{client_id}|{texte}|{bucket}"
    k = hashlib.sha1(key_raw.encode("utf-8", errors="ignore")).hexdigest()

    conn = _db()
    cur = conn.cursor()

    purge_before = now - (SOFT_DEDUP_WINDOW_SEC * 3)
    cur.execute("DELETE FROM soft_dedup WHERE created_at < ?", (purge_before,))

    cur.execute("SELECT k FROM soft_dedup WHERE k = ?", (k,))
    if cur.fetchone():
        conn.commit()
        conn.close()
        return True

    cur.execute("INSERT INTO soft_dedup(k, created_at) VALUES(?,?)", (k, now))
    conn.commit()
    conn.close()
    return False


def buffer_append_and_maybe_flush(client_id: str, new_text: str) -> Tuple[bool, str]:
    now = int(time.time())
    new_text = (new_text or "").strip()
    if not new_text:
        return (False, "")

    if CLIENT_BUFFER_WINDOW_SEC <= 0:
        return (True, new_text)

    conn = _db()
    cur = conn.cursor()

    cur.execute("SELECT buffer_text, last_at FROM client_message_buffer WHERE client_id = ?", (client_id,))
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
    last_at = int(row["last_at"])

    if now - last_at > CLIENT_BUFFER_WINDOW_SEC:
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
# Missions DB
# =========================

def create_mission(
    client_id: str,
    raw_request: str,
    merged_request: str,
    resume_client: str,
    type_detecte: str,
    zone_tarifaire: Optional[str],
    delai_estime: str,
    prix_recommande_eur: Decimal,
    conditions: str,
) -> int:
    now = utc_now_iso()
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO missions(
            client_id, raw_request, merged_request, resume_client,
            type_detecte, zone_tarifaire, delai_estime, prix_recommande_eur,
            conditions, statut, created_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        client_id, raw_request, merged_request, resume_client,
        type_detecte, zone_tarifaire, delai_estime, float(prix_recommande_eur),
        conditions, "en_attente_validation", now, now
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


@app.post("/mission/demande", response_model=MissionOut)
async def mission_demande(request: Request) -> MissionOut:
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
    client_id, message_id, texte, _ = resolve_payload(payload)

    if not client_id:
        raise HTTPException(status_code=422, detail="client_id/from manquant (WhatsApp ID).")

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
        )

    should_process_now, merged = buffer_append_and_maybe_flush(client_id, texte)
    intent = detect_intent(merged)
    prix, resume, conditions = _simple_pricing(merged, intent)

    # ✅ Même en buffering : on renvoie des champs complets (plus de messages vides côté WhatsApp)
    if not should_process_now:
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client=resume,
            prix_recommande_eur=float(prix),
            delai_estime=intent["delai_estime"],
            conditions=conditions,
            zone_tarifaire=intent["zone_tarifaire"],
            type_detecte=intent["type_detecte"],
            statut="buffering",
        )

    mission_id = create_mission(
        client_id=client_id,
        raw_request=texte,
        merged_request=merged,
        resume_client=resume,
        type_detecte=intent["type_detecte"],
        zone_tarifaire=intent["zone_tarifaire"],
        delai_estime=intent["delai_estime"],
        prix_recommande_eur=prix,
        conditions=conditions,
    )

    return MissionOut(
        mission_id=mission_id,
        client_id=client_id,
        resume_client=resume,
        prix_recommande_eur=float(prix),
        delai_estime=intent["delai_estime"],
        conditions=conditions,
        zone_tarifaire=intent["zone_tarifaire"],
        type_detecte=intent["type_detecte"],
        statut="en_attente_validation",
    )


@app.post("/mission/{mission_id}/status")
def set_status(mission_id: int, statut: str) -> Dict[str, Any]:
    if not statut:
        raise HTTPException(status_code=422, detail="statut manquant")
    update_mission_status(mission_id, statut)
    return {"ok": True, "mission_id": mission_id, "statut": statut}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
