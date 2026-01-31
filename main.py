from __future__ import annotations

import os
import re
import json
import time
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List, Tuple, Union

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, ConfigDict


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
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "").strip()  # optionnel: protège /debug/* si rempli


# =========================
# App
# =========================

app = FastAPI(title=f"{APP_NAME} API", version="1.3.0")


# =========================
# Modèles Pydantic (Pydantic v2)
# =========================

class DemandeMission(BaseModel):
    """
    Requête reçue depuis Make/WhatsApp.
    On tolère des variations et payloads imbriqués (Make peut changer selon modules / tests).
    """
    model_config = ConfigDict(extra="allow")

    message_id: Optional[str] = Field(None, description="Identifiant unique du message WhatsApp (anti-doublon).")
    client_id: Optional[str] = Field(None, description="Identifiant client (WhatsApp ID / téléphone).")
    texte: Optional[str] = Field(None, description="Demande client en texte libre.")


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


def _norm_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (k or "").lower())


def _get_any_fuzzy(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    """
    Récupère une valeur dans un dict en tolérant :
    - casse différente (From vs from)
    - espaces/underscores/ponctuation (Body content, body_content, BodyContent, etc.)
    """
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
    """
    Récupère un champ dans une structure imbriquée dict/list, sans crash.
    Tolère aussi des clés "Entry"/"entry", "From"/"from", etc.
    Exemple:
      _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "from"])
    """
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


def resolve_payload(payload: DemandeMission) -> Tuple[str, Optional[str], str]:
    """
    Rend l'API tolérante si Make envoie des clés différentes ou un payload imbriqué.
    On cherche:
      - client_id via client_id / from / From / wa_id / Contacts[] / webhook Meta
      - message_id via message_id / id / Messages[0].id / webhook Meta
      - texte via texte / text.body / Messages[0].Text.Body / Body content / webhook Meta
    """
    raw = payload.model_dump()

    msg_id: Optional[Any] = payload.message_id or _get_any_fuzzy(raw, "message_id", "messageId", "messageID", "id", "Message ID", "MessageId")
    cid: Optional[Any] = payload.client_id or _get_any_fuzzy(raw, "client_id", "clientId", "from", "From", "wa_id", "waId", "whatsapp_id", "WhatsApp ID", "sender")
    txt: Optional[Any] = payload.texte or _get_any_fuzzy(raw, "texte", "text", "Text", "body", "Body", "Body content", "content", "message")

    if isinstance(txt, dict):
        txt = _get_any_fuzzy(txt, "body", "Body", "text", "Text")

    # Wrappers fréquents dans Make (data/payload/body/request)
    for wrapper_key in ("data", "payload", "body", "request", "input"):
        w = _get_any_fuzzy(raw, wrapper_key)
        if isinstance(w, dict):
            if not msg_id:
                msg_id = _get_any_fuzzy(w, "message_id", "messageId", "id", "Message ID")
            if not cid:
                cid = _get_any_fuzzy(w, "client_id", "from", "From", "wa_id", "WhatsApp ID", "sender")
            if (not txt or str(txt).strip() == ""):
                t2 = _get_any_fuzzy(w, "texte", "text", "body", "Body", "Body content", "message", "content")
                if isinstance(t2, dict):
                    t2 = _get_any_fuzzy(t2, "body", "Body")
                txt = t2 or txt

    # Fallback Make "Messages[]"
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

    # Fallback Make "Contacts[]"
    contacts = _get_any_fuzzy(raw, "Contacts", "contacts", "contact")
    c0 = _first_in_list(contacts)
    if isinstance(c0, dict) and not cid:
        cid = _get_any_fuzzy(c0, "wa_id", "waId", "WhatsApp ID", "from", "From", "id")

    # Fallback Webhook Meta complet (entry/changes/value/messages | contacts)
    if not cid:
        cid = (
            _deep_get(raw, ["entry", 0, "changes", 0, "value", "messages", 0, "from"])
            or _deep_get(raw, ["entry", 0, "changes", 0, "value", "contacts", 0, "wa_id"])
        )
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
async def mission_demande(request: Request) -> MissionOut:
    """
    Endpoint principal (Option A).
    Accepte:
    - JSON (recommandé)
    - form-data / x-www-form-urlencoded (si Make est configuré ainsi)
    - raw text (fallback)
    """
    raw: Any = None

    # 1) JSON
    try:
        raw = await request.json()
    except Exception:
        raw = None

    # 2) Form data
    if raw is None:
        try:
            form = await request.form()
            raw = dict(form)
        except Exception:
            raw = None

    # 3) Body texte brut
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

    payload = DemandeMission.model_validate(raw)
    client_id, message_id, texte = resolve_payload(payload)

    if not client_id:
        _log("bad_payload_no_client_id", {
            "headers": dict(request.headers),
            "keys": list(payload.model_dump().keys()),
            "payload": payload.model_dump(),
        })
        raise HTTPException(
            status_code=422,
            detail="client_id manquant. Côté Make: mappe Messages[1].from (ou Contacts[1].WhatsApp ID)."
        )

    if anti_loop_ignore(client_id):
        _log("ignore_self", {"client_id": client_id})
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
        _log("dedup_hit", {"client_id": client_id, "message_id": message_id})
        return MissionOut(
            mission_id=0,
            client_id=client_id,
            resume_client="Message déjà traité (déduplication).",
            prix_recommande_eur=None,
            delai_estime="",
            conditions=None,
            zone_tarifaire=None,
            type_detecte=None,
            statut="duplicate_message",
        )

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


@app.get("/debug/last")
def debug_last(limit: int = 20, token: str = "") -> Dict[str, Any]:
    """
    Récupère les derniers logs (uniquement utile si DEBUG=1).
    Si DEBUG_TOKEN est défini, ajoute ?token=... pour accéder.
    """
    if not DEBUG:
        raise HTTPException(status_code=403, detail="DEBUG=0")
    if DEBUG_TOKEN and token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="bad_token")

    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT id, kind, payload, created_at FROM audit_log ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        try:
            payload = json.loads(r["payload"])
        except Exception:
            payload = r["payload"]
        out.append({
            "id": int(r["id"]),
            "kind": str(r["kind"]),
            "payload": payload,
            "created_at": str(r["created_at"]),
        })
    return {"items": out, "count": len(out)}


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
