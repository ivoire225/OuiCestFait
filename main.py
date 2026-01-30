# main.py — OuiCestFait (mode pro)
# Objectif : API + logique robuste pour WhatsApp/Make
# - Déduplication anti-boucle (message_id unique)
# - Regroupement de plusieurs messages d’un même client (buffer + fenêtre)
# - Historique / suivi mission en SQLite
# - Prix final arrondi (2 décimales) de façon sûre
#
# IMPORTANT (Make):
# 1) Dans le module HTTP (POST /mission/demande), envoie un JSON string:
#    {
#      "message_id": "{{1.Messages[1].id}}",
#      "client_id": "{{1.Contacts[1].WhatsApp ID}}",
#      "texte": "{{1.Messages[1].Text.Body}}"
#    }
# 2) N’envoie pas WhatsApp Business Account ID comme client_id.
# 3) Active le scenario (ON). "Run once" sert uniquement pour tester.
#
# Lancement local:
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# Déploiement Railway:
#   commande: uvicorn main:app --host 0.0.0.0 --port $PORT

from __future__ import annotations

import os
import re
import json
import time
import sqlite3
import hashlib
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# =========================
# Config
# =========================

APP_NAME = "OuiCestFait"
DB_PATH = os.getenv("DB_PATH", "ouicestfait.db")

# Fenêtre pour regrouper plusieurs messages d’un même client (secondes)
CLIENT_BUFFER_WINDOW_SEC = int(os.getenv("CLIENT_BUFFER_WINDOW_SEC", "25"))

# Optionnel: si tu veux éviter de répondre à ton propre numéro / WABA (anti-boucle complémentaire)
# Exemple: "33759055781" (sans espaces)
WHATSAPP_OWN_NUMBER = os.getenv("WHATSAPP_OWN_NUMBER", "").strip()

# Limites / pricing
WORST_CASE_DEPARTURE_BUFFER_KM = int(os.getenv("WORST_CASE_DEPARTURE_BUFFER_KM", "70"))
MIN_PRICE_EUR = Decimal(os.getenv("MIN_PRICE_EUR", "25"))
URGENT_BASE_EUR = Decimal(os.getenv("URGENT_BASE_EUR", "150"))
LONG_DISTANCE_BASE_EUR = Decimal(os.getenv("LONG_DISTANCE_BASE_EUR", "300"))

# Debug
DEBUG = os.getenv("DEBUG", "0") == "1"


# =========================
# App
# =========================

app = FastAPI(title=f"{APP_NAME} API", version="1.0.0")


# =========================
# Modèles Pydantic
# =========================

class DemandeMission(BaseModel):
    """
    Requête reçue depuis Make/WhatsApp.
    """
    message_id: Optional[str] = Field(None, description="Identifiant unique du message WhatsApp (pour anti-doublon).")
    client_id: str = Field(..., description="Identifiant client (WhatsApp ID / téléphone).")
    texte: str = Field(..., description="Demande client en texte libre.")


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

    # Table missions
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

    # Table message_dedup (anti-boucle / anti-doublons)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS message_dedup (
        message_id TEXT PRIMARY KEY,
        client_id TEXT,
        created_at TEXT NOT NULL
    )
    """)

    # Buffer multi-messages par client (regroupement)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS client_message_buffer (
        client_id TEXT PRIMARY KEY,
        buffer_text TEXT NOT NULL,
        first_at INTEGER NOT NULL,
        last_at INTEGER NOT NULL
    )
    """)

    # Audit / logs (optionnel)
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
    # Supprime espaces, +, etc pour normaliser (WhatsApp ID est souvent déjà numérique)
    x = (client_id or "").strip()
    x = x.replace(" ", "").replace("+", "")
    return x


def safe_round_eur(value: Decimal) -> Decimal:
    # Arrondi monétaire fiable à 2 décimales (0.01)
    q = Decimal("0.01")
    return value.quantize(q, rounding=ROUND_HALF_UP)


def as_float_or_none(x: Optional[Decimal]) -> Optional[float]:
    return float(x) if x is not None else None


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


def detect_intent(text: str) -> Dict[str, Any]:
    """
    Détection simple (sans IA) : tu peux remplacer par ton moteur IA plus tard.
    """
    t = (text or "").lower()

    # types
    if any(k in t for k in ["livraison", "colis", "document", "dossier", "remettre", "déposer"]):
        type_detecte = "livraison"
    elif any(k in t for k in ["courses", "supermarché", "acheter", "liste de courses"]):
        type_detecte = "courses"
    elif any(k in t for k in ["transport", "déménagement", "véhicule", "conduire"]):
        type_detecte = "transport"
    else:
        type_detecte = "conciergerie"

    # urgence
    urgent = any(k in t for k in ["urgent", "urgence", "rapidement", "tout de suite", "sous 1h", "immédiat"])
    delai_estime = "sous 1h" if urgent else "dans la journée"

    # zone (très simplifiée)
    zone_tarifaire = None
    if any(k in t for k in ["paris", "75"]):
        zone_tarifaire = "IDF/Paris"

    return {
        "type_detecte": type_detecte,
        "urgent": urgent,
        "delai_estime": delai_estime,
        "zone_tarifaire": zone_tarifaire,
    }


def _extract_city_pair(text: str) -> Optional[Tuple[str, str]]:
    """
    Extrait des patterns style 'Paris -> Torcy' / 'Paris à Lyon' (approx).
    Retourne (from, to) si trouvé.
    """
    if not text:
        return None
    # Paris -> Torcy
    m = re.search(r"([A-Za-zÀ-ÿ'\- ]+)\s*(?:->|→)\s*([A-Za-zÀ-ÿ'\- ]+)", text)
    if m:
        return (m.group(1).strip(), m.group(2).strip())
    # de Paris à Lyon
    m = re.search(r"(?:de|depuis)\s+([A-Za-zÀ-ÿ'\- ]+)\s+(?:à|vers)\s+([A-Za-zÀ-ÿ'\- ]+)", text, re.IGNORECASE)
    if m:
        return (m.group(1).strip(), m.group(2).strip())
    return None


def _simple_pricing(text: str, intent: Dict[str, Any]) -> Tuple[Decimal, str, str]:
    """
    Pricing très simple et cohérent pour démarrer (à améliorer ensuite).
    Retour:
      prix (Decimal), resume_client, conditions
    """
    t = text or ""
    urgent = intent["urgent"]
    type_detecte = intent["type_detecte"]

    # résumé court
    resume = t.strip()
    if len(resume) > 120:
        resume = resume[:117] + "..."

    # base
    price = URGENT_BASE_EUR if urgent else Decimal("80")

    # distance heuristique via villes
    pair = _extract_city_pair(t)
    if pair:
        frm, to = pair
        # heuristique "longue distance" si destination hors IDF courante
        if any(k in to.lower() for k in ["lyon", "marseille", "lille", "bordeaux", "nantes", "toulouse", "strasbourg", "orleans"]):
            price = max(price, LONG_DISTANCE_BASE_EUR)

    # bonus selon type
    if type_detecte == "courses":
        price += Decimal("30")
    elif type_detecte == "transport":
        price += Decimal("80")

    # plancher
    price = max(price, MIN_PRICE_EUR)

    # conditions
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
    """
    Retourne True si c'est un doublon (déjà vu), False sinon.
    Stocke le message_id si nouveau.
    """
    if not message_id:
        return False  # pas d'id -> on ne peut pas dédupliquer côté serveur

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
    """
    Bloque les réponses si le client_id correspond à ton propre numéro (optionnel).
    """
    if not WHATSAPP_OWN_NUMBER:
        return False
    return normalize_client_id(client_id) == normalize_client_id(WHATSAPP_OWN_NUMBER)


# =========================
# Buffer multi-messages client
# =========================

def buffer_append_and_maybe_flush(client_id: str, new_text: str) -> Tuple[bool, str]:
    """
    Ajoute le message au buffer du client. Si fenêtre expirée => flush.
    Retour:
      (should_process_now, merged_text)
    """
    now = int(time.time())
    new_text = (new_text or "").strip()
    if not new_text:
        return (False, "")

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

    # Si dernier message est trop ancien => flush le précédent, puis démarre nouveau buffer
    if now - last_at > CLIENT_BUFFER_WINDOW_SEC:
        # flush précédent
        merged_prev = buffer_text.strip()
        # reset buffer avec nouveau message
        cur.execute(
            "UPDATE client_message_buffer SET buffer_text=?, first_at=?, last_at=? WHERE client_id=?",
            (new_text, now, now, client_id),
        )
        conn.commit()
        conn.close()
        return (True, merged_prev)

    # Sinon, concatène et continue
    merged = (buffer_text + "\n" + new_text).strip()
    cur.execute(
        "UPDATE client_message_buffer SET buffer_text=?, last_at=? WHERE client_id=?",
        (merged, now, client_id),
    )
    conn.commit()
    conn.close()
    return (False, merged)


def buffer_force_flush(client_id: str) -> Optional[str]:
    """
    Force le flush du buffer (utile si tu veux déclencher via un cron/worker plus tard).
    """
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT buffer_text FROM client_message_buffer WHERE client_id = ?", (client_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    merged = (row["buffer_text"] or "").strip()
    cur.execute("DELETE FROM client_message_buffer WHERE client_id = ?", (client_id,))
    conn.commit()
    conn.close()
    return merged if merged else None


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
    """
    Endpoint appelé par Make (HTTP module).
    Gère:
      - anti-boucle / dedupe
      - buffer multi-messages
      - création mission + pricing + arrondi
    """
    client_id = normalize_client_id(payload.client_id)
    texte = (payload.texte or "").strip()
    message_id = (payload.message_id or "").strip() or None

    if not client_id:
        raise HTTPException(status_code=422, detail="client_id manquant")

    if anti_loop_ignore(client_id):
        _log("ignore_self", {"client_id": client_id})
        raise HTTPException(status_code=200, detail="ignored_self")

    # anti-boucle / dedupe
    if dedup_check_and_store(message_id, client_id):
        _log("dedup_hit", {"client_id": client_id, "message_id": message_id})
        raise HTTPException(status_code=200, detail="duplicate_message")

    # buffer multi-messages
    should_process_now, merged = buffer_append_and_maybe_flush(client_id, texte)

    # Si on vient juste d'ajouter au buffer, on ne répond pas tout de suite (pour regrouper)
    # NOTE: Pour un mode 100% instantané, mets CLIENT_BUFFER_WINDOW_SEC=0 et/ou renvoie une ack.
    if not should_process_now:
        # On renvoie une réponse "ack" (mais Make/WhatsApp va quand même envoyer si tu le branches direct).
        # Astuce Make: place un filtre pour n'envoyer vers WhatsApp que si "statut" != "buffering".
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

    # Arrondi forcé du prix (important pour éviter 325.78000000000003)
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
