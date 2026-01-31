# main.py — OuiCestFait (mode pro)
# Objectif : API + logique robuste pour WhatsApp/Make
# - Déduplication anti-boucle (message_id unique)
# - Regroupement de plusieurs messages d’un même client (buffer + fenêtre)
# - Historique / suivi mission en SQLite
# - Tolérance MAX sur le payload Make (plat OU imbriqué Messages[] / webhook Meta)
#
# IMPORTANT (Make) - recommandé:
# Dans le module HTTP (POST /mission/demande), envoie un JSON (application/json).
# Idéalement via "Body type: Raw" + "Content type: application/json" OU "JSON" mappé.
#
# Exemple JSON "plat" idéal:
# {
#   "message_id": "{{1.Messages[1].id}}",
#   "client_id": "{{1.Messages[1].from}}",
#   "texte": "{{1.Messages[1].Text.Body}}"
# }
#
# Si Make n'arrive pas à mapper `from`, l'API ci-dessous va aussi tenter de le retrouver
# dans des structures imbriquées (Messages[0].from, entry[0].changes[0].value.messages[0].from, etc.)

from __future__ import annotations

import os
import re
import json
import time
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List, Tuple, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# =========================
# Config
# =========================

APP_NAME = "OuiCestFait"
DB_PATH = os.getenv("DB_PATH", "ouicestfait.db")

# Fenêtre pour regrouper plusieurs messages d’un même client (secondes)
CLIENT_BUFFER_WINDOW_SEC = int(os.getenv("CLIENT_BUFFER_WINDOW_SEC", "25"))

# Optionnel: éviter de répondre à ton propre numéro (anti-boucle complémentaire)
# Exemple: "33759055781" (sans espaces)
WHATSAPP_OWN_NUMBER = os.getenv("WHATSAPP_OWN_NUMBER", "").strip()

# Pricing (base)
MIN_PRICE_EUR = Decimal(os.getenv("MIN_PRICE_EUR", "25"))
URGENT_BASE_EUR = Decimal(os.getenv("URGENT_BASE_EUR", "150"))
LONG_DISTANCE_BASE_EUR = Decimal(os.getenv("LONG_DISTANCE_BASE_EUR", "300"))

# Debug (log en DB)
DEBUG = os.getenv("DEBUG", "0") == "1"


# =========================
# App
# =========================

app = FastAPI(title=f"{APP_NAME} API", version="1.2.0")


# =========================
# Modèles Pydantic
# =========================

class DemandeMission(BaseModel):
    """
    Requête reçue depuis Make/WhatsApp.
    On tolère des variations et payloads imbriqués (Make peut changer selon modules / tests).
    """
    message_id: Optional[str] = Field(None, description="Identifiant unique du message WhatsApp (anti-doublon).")
    client_id: Optional[str] = Field(None, description="Identifiant client (WhatsApp ID / téléphone).")
    texte: Optional[str] = Field(None, description="Demande client en texte libre.")

    class Config:
        extra = "allow"  # accepte champs inattendus


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
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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

    conn.commit()
    conn.close()


init_db()


# =========================
# Utils
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_client_id(client_id: str) -> str:
    x = (client_id or "").strip()
    x = x.replace(" ", "").replace("+", "")
    return x


def safe_round_eur(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


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


def _get_any(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None


def _deep_get(obj: Any, path: List[Union[str, int]]) -> Optional[Any]:
    """
    Récupère un champ dans une structure imbriquée dict/list, sans crash.
    Exemple: _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "from"])
    """
    cur = obj
    try:
        for p in path:
            if isinstance(p, int):
                if not isinstance(cur, list) or len(cur) <= p:
                    return None
                cur = cur[p]
            else:
                if not isinstance(cur, dict) or p not in cur:
                    return None
                cur = cur[p]
        if cur in (None, "", []):
            return None
        return cur
    except Exception:
        return None


def _first_in_list(x: Any) -> Optional[Any]:
    if isinstance(x, list) and len(x) > 0:
        return x[0]
    return None


def resolve_payload(payload: DemandeMission) -> Tuple[str, Optional[str], str]:
    """
    Rend l'API tolérante si Make envoie des clés différentes ou un payload imbriqué.
    On cherche:
      - client_id via client_id OU from OU wa_id OU Messages[0].from OU webhook Meta
      - message_id via message_id OU id OU Messages[0].id OU webhook Meta
      - texte via texte OU text.body OU Messages[0].Text.Body OU webhook Meta (messages[0].text.body)
    """
    raw = payload.dict()

    # --- 1) message_id (plat)
    msg_id = payload.message_id or _get_any(raw, "id", "messageId", "messageID")

    # --- 2) client_id (plat)
    cid = payload.client_id or _get_any(raw, "from", "wa_id", "whatsapp_id", "whatsappId", "sender")

    # --- 3) texte (plat)
    txt = payload.texte or _get_any(raw, "text", "body", "message", "content")

    # Si txt est dict du style {"body": "..."}
    if isinstance(txt, dict):
        txt = _get_any(txt, "body", "Body", "text", "Text")

    # ----------------------------
    # Fallback 1 : structure Make "Messages[]" (parfois Make envoie un objet complet)
    # raw peut contenir: {"Messages": [{"from": "...", "id": "...", "Text": {"Body": "..."}}], ...}
    # ----------------------------
    messages = _get_any(raw, "Messages", "messages")
    m0 = _first_in_list(messages)

    if not msg_id and isinstance(m0, dict):
        msg_id = _get_any(m0, "id", "message_id", "messageId")

    if not cid and isinstance(m0, dict):
        cid = _get_any(m0, "from", "wa_id", "sender", "client_id")

    if (not txt or str(txt).strip() == "") and isinstance(m0, dict):
        # Text.Body ou text.body
        t1 = _deep_get(m0, ["Text", "Body"])
        t2 = _deep_get(m0, ["text", "body"])
        txt = t1 or t2 or _get_any(m0, "body", "text")

    # ----------------------------
    # Fallback 2 : webhook Meta complet (entry/changes/value/messages)
    # entry[0].changes[0].value.messages[0].from
    # entry[0].changes[0].value.messages[0].id
    # entry[0].changes[0].value.messages[0].text.body
    # ----------------------------
    if not cid:
        cid = _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "from"])
    if not msg_id:
        msg_id = _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "id"])
    if (not txt or str(txt).strip() == ""):
        txt = _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "text", "body"]) or ""

    # Normalisations
    if isinstance(msg_id, (int, float)):
        msg_id = str(msg_id)
    msg_id = (str(msg_id).strip() if msg_id else None) or None

    if isinstance(cid, (int, float)):
        cid = str(cid)
    cid = normalize_client_id(str(cid)) if cid else ""

    txt = (str(txt).strip() if txt else "")

    return cid, msg_id, txt


# =========================
# Détection + pricing
# =========================

def detect_intent(text: str) -> Dict[str, Any]:
    t = (text or "").lower()

    if any(k in t for k in ["livraison", "colis", "document", "dossier", "remettre", "déposer"]):
        type_detecte = "livraison"
    elif any(k in t for k in ["courses", "supermarché", "acheter", "liste de courses"]):
        type_detecte = "courses"
    elif any(k in t for k in ["transport", "déménagement", "véhicule", "conduire"]):
        type_detecte = "transport"
    else:
        type_detecte = "conciergerie"

    urgent = any(k in t for k in ["urgent", "urgence", "rapidement", "tout de suite", "sous 1h", "immédiat"])
    delai_estime = "sous 1h" if urgent else "dans la journée"

    zone_tarifaire = "IDF/Paris" if any(k in t for k in ["paris", "75"]) else None

    return {
        "type_detecte": type_detecte,
        "urgent": urgent,
        "delai_estime": delai_estime,
        "zone_tarifaire": zone_tarifaire,
    }


def _extract_city_pair(text: str) -> Optional[Tuple[str, str]]:
    if not text:
        return None
    m = re.search(r"([A-Za-zÀ-ÿ'\- ]+)\s*(?:->|→)\s*([A-Za-zÀ-ÿ'\- ]+)", text)
    if m:
        return (m.group(1).strip(), m.group(2).strip())
    m = re.search(r"(?:de|depuis)\s+([A-Za-zÀ-ÿ'\- ]+)\s+(?:à|vers)\s+([A-Za-zÀ-ÿ'\- ]+)", text, re.IGNORECASE)
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

    pair = _extract_city_pair(t)
    if pair:
        _, to = pair
        if any(k in to.lower() for k in [
            "lyon", "marseille", "lille", "bordeaux", "nantes", "toulouse", "strasbourg", "orleans"
        ]):
            price = max(price, LONG_DISTANCE_BASE_EUR)

    if type_detecte == "courses":
        price += Decimal("30")
    elif type_detecte == "transport":
        price += Decimal("80")

    price = max(price, MIN_PRICE_EUR)

    conditions = (
        "Paiement 100% à l'avance.\n"
        "Preuve de prise en charge et de livraison (photo / signature / message).\n"
        "Annulation : si l'intervention a déjà commencé, facturation partielle possible.\n"
        "Objet/documents à remettre dans une enveloppe/packaging adapté (si nécessaire)."
    )

    return (price, resume, conditions)


# =========================
# Anti-boucle / dédup
# =========================

def dedup_check_and_store(message_id: Optional[str], client_id: str) -> bool:
    if not message_id:
        return False

    conn = _db()
    cur = conn.cursor()

    cur.execute("SELECT message_id FROM message_dedup WHERE message_id = ?", (message_id,))
    row = cur.fetchone()
    if row:
        conn.close()
        return True

    cur.execute(
        "INSERT INTO message_dedup(message_id, client_id, created_at) VALUES(?,?,?)",
        (message_id, client_id, utc_now_iso()),
    )
    conn.commit()
    conn.close()
    return False


def anti_loop_ignore(client_id: str) -> bool:
    if not WHATSAPP_OWN_NUMBER:
        return False
    return normalize_client_id(client_id) == normalize_client_id(WHATSAPP_OWN_NUMBER)


# =========================
# Buffer multi-messages client
# =========================

def buffer_append_and_maybe_flush(client_id: str, new_text: str) -> Tuple[bool, str]:
    now = int(time.time())
    new_text = (new_text or "").strip()
    if not new_text:
        return (False, "")

    # Si la fenêtre = 0 => traitement immédiat
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


def get_client_history(client_id: str, limit: int = 10) -> List[MissionHistoryItem]:
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, created_at, statut, resume_client, prix_recommande_eur, delai_estime, type_detecte
        FROM missions
        WHERE client_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (client_id, limit))
    rows = cur.fetchall()
    conn.close()

    out: List[MissionHistoryItem] = []
    for r in rows:
        out.append(MissionHistoryItem(
            mission_id=int(r["id"]),
            created_at=str(r["created_at"]),
            statut=str(r["statut"]),
            resume_client=str(r["resume_client"] or ""),
            prix_recommande_eur=float(r["prix_recommande_eur"]) if r["prix_recommande_eur"] is not None else None,
            delai_estime=str(r["delai_estime"] or "") if r["delai_estime"] is not None else None,
            type_detecte=str(r["type_detecte"] or "") if r["type_detecte"] is not None else None,
        ))
    return out


# =========================
# API
# =========================

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "app": APP_NAME, "utc": utc_now_iso()}


@app.post("/mission/demande", response_model=MissionOut)
def mission_demande(payload: DemandeMission) -> MissionOut:
    client_id, message_id, texte = resolve_payload(payload)

    if not client_id:
        # Pour aider au debug, on logge les clés reçues si DEBUG=1
        _log("bad_payload_no_client_id", {
            "keys": list(payload.dict().keys()),
            "payload": payload.dict()
        })
        raise HTTPException(
            status_code=422,
            detail="client_id manquant. Côté Make: mappe Messages[1].from (ou Contacts[1].WhatsApp ID si dispo)."
        )

    if anti_loop_ignore(client_id):
        _log("ignore_self", {"client_id": client_id})
        raise HTTPException(status_code=200, detail="ignored_self")

    if dedup_check_and_store(message_id, client_id):
        _log("dedup_hit", {"client_id": client_id, "message_id": message_id})
        raise HTTPException(status_code=200, detail="duplicate_message")

    should_process_now, merged = buffer_append_and_maybe_flush(client_id, texte)

    # Si tu branches HTTP -> Send a message direct, filtre Make:
    # n'envoie vers WhatsApp que si statut != "buffering"
    if not should_process_now:
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="Message reçu, analyse en cours…",
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="buffering",
        )

    merged_request = merged
    raw_request = texte

    intent = detect_intent(merged_request)
    prix, resume, conditions = _simple_pricing(merged_request, intent)

    # Arrondi monétaire forcé
    prix = safe_round_eur(prix)

    mission_id = create_mission(
        client_id=client_id,
        raw_request=raw_request,
        merged_request=merged_request,
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


@app.get("/client/{client_id}/history", response_model=List[MissionHistoryItem])
def client_history(client_id: str, limit: int = 10) -> List[MissionHistoryItem]:
    client_id = normalize_client_id(client_id)
    return get_client_history(client_id, limit=limit)
