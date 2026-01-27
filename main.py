from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import Optional
import sqlite3
import json
from fastapi.openapi.utils import get_openapi
from openai import OpenAI
from jose import JWTError, jwt
import bcrypt
from datetime import datetime, timedelta
from telegram import Bot
import os

# =========================
# CONFIG
# =========================
DB_NAME = "ouicestfait.db"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# =========================
# ZONES / PRICING RULES
# =========================
# IDF: prix marché (IA) fourchette haute x 3
COEF_IDF = 3.0

# Province France (hors IDF):
# Prix = 1€/km + 15€ (repas) + (péage estimé x2) + 30% marge globale
PRICE_PER_KM = 1.0
MEAL_FEE = 15.0
MARGIN_RATE = 0.30

# Départ interne: Mormant 77720 OU Paris
# -> on retient le pire cas (distance/peage max) entre les deux.
STARTPOINT_A = "Mormant 77720"
STARTPOINT_B = "Paris"

IDF_KEYWORDS = [
    "ile-de-france", "île-de-france", "idf",
    "paris",
    "75", "77", "78", "91", "92", "93", "94", "95",
    "hauts-de-seine", "seine-saint-denis", "val-de-marne",
    "seine-et-marne", "yvelines", "essonne", "val-d'oise",
]

# Heuristique France: si un terme IDF est présent -> France
# Sinon, on cherche quelques villes + mentions explicites
FRANCE_KEYWORDS = [
    "france", "république française",
    "lyon", "marseille", "lille", "bordeaux", "toulouse", "nantes", "nice",
    "strasbourg", "rennes", "grenoble", "montpellier", "dijon", "reims",
    "orleans", "orléans", "tours", "clermont-ferrand", "metz", "nancy",
    "saint-etienne", "saint-étienne", "angers", "le havre", "cannes",
    "avignon", "annecy", "chambery", "chambéry",
]


def is_idf(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in IDF_KEYWORDS)


def is_france(text: str) -> bool:
    t = (text or "").lower()
    if is_idf(t):
        return True
    return any(k in t for k in FRANCE_KEYWORDS)


def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


# =========================
# CONDITIONS PRO (paiement à l'avance obligatoire)
# =========================
DEFAULT_CONDITIONS_GENERIC = (
    "Paiement à l’avance obligatoire.\n"
    "Merci d’indiquer : adresse de départ, adresse d’arrivée, nom + numéro de contact, et contrainte horaire."
)

DEFAULT_CONDITIONS_BY_TYPE = {
    "livraison": (
        "Paiement à l’avance obligatoire.\n"
        "À fournir : adresse départ/arrivée, nom + téléphone du destinataire, créneau souhaité, type de colis (taille/fragile)."
    ),
    "courses": (
        "Paiement à l’avance obligatoire.\n"
        "À fournir : liste des articles + budget max, magasin/zone, adresse de livraison, substitutions acceptées (oui/non)."
    ),
    "transport": (
        "Paiement à l’avance obligatoire.\n"
        "À fournir : nombre de passagers, adresses départ/arrivée, heure souhaitée, bagages, étage/ascenseur si objet volumineux."
    ),
    "urgence": (
        "Paiement à l’avance obligatoire.\n"
        "URGENT : confirmer immédiatement adresses + disponibilité + téléphone. Départ sous 30–60 min selon faisabilité."
    ),
}


def detect_mission_type(text: str) -> str:
    t = (text or "").lower()

    urgent_kw = ["urgent", "urgence", "tout de suite", "immédiat", "immediat", "asap", "ce soir", "dans 1h", "rapidement"]
    if any(k in t for k in urgent_kw):
        return "urgence"

    courses_kw = ["courses", "supermarché", "carrefour", "auchan", "leclerc", "monoprix", "acheter", "pharmacie"]
    if any(k in t for k in courses_kw):
        return "courses"

    transport_kw = ["transport", "trajet", "m'emmener", "voiture", "taxi", "vtc", "déposer", "deposer", "récupérer", "recuperer", "aéroport", "aeroport", "gare"]
    if any(k in t for k in transport_kw):
        return "transport"

    livraison_kw = ["livraison", "livrer", "colis", "documents", "document", "dossier", "enveloppe", "remettre"]
    if any(k in t for k in livraison_kw):
        return "livraison"

    return "livraison"


def build_default_conditions(text: str) -> str:
    mtype = detect_mission_type(text)
    return DEFAULT_CONDITIONS_BY_TYPE.get(mtype, DEFAULT_CONDITIONS_GENERIC)


def enforce_advance_payment(conditions: str) -> str:
    base = "Paiement à l’avance obligatoire.\n"
    if not conditions or not isinstance(conditions, str) or conditions.strip() == "":
        return DEFAULT_CONDITIONS_GENERIC
    c = conditions.strip()
    low = c.lower()
    if "paiement" in low and ("avance" in low or "à l’avance" in low or "a l'avance" in low):
        return c
    return base + c


# =========================
# MODELS
# =========================
class DemandeMission(BaseModel):
    # WhatsApp wa_id -> string
    client_id: str
    texte: str


class Mission(BaseModel):
    client_id: str
    resume_client: str
    prix_recommande_eur: float
    delai_estime: str
    conditions: Optional[str] = None


class User(BaseModel):
    username: str


class UserInDB(User):
    hashed_password: str


# =========================
# DB
# =========================
def create_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS missions
           (id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT,
            resume_client TEXT,
            prix_recommande_eur REAL,
            delai_estime TEXT,
            conditions TEXT,
            status TEXT DEFAULT 'pending',
            date_creation DATETIME DEFAULT CURRENT_TIMESTAMP)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS users
           (username TEXT PRIMARY KEY,
            hashed_password TEXT)"""
    )
    conn.commit()
    conn.close()


create_db()


# =========================
# AUTH HELPERS
# =========================
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    if not SECRET_KEY:
        raise HTTPException(status_code=500, detail="SECRET_KEY manquante côté serveur")
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        if not SECRET_KEY:
            raise credentials_exception
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    user = c.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    if user is None:
        raise credentials_exception
    return User(username=username)


# =========================
# ROUTES AUTH
# =========================
@app.post("/users/")
def create_user(form_data: OAuth2PasswordRequestForm = Depends()):
    hashed_password = hash_password(form_data.password)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO users (username, hashed_password) VALUES (?, ?)",
            (form_data.username, hashed_password),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already registered")
    finally:
        conn.close()
    return {"message": "User created"}


@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    user = c.execute("SELECT * FROM users WHERE username = ?", (form_data.username,)).fetchone()
    conn.close()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    if not verify_password(form_data.password, user[1]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


# =========================
# ROUTE PRINCIPALE
# =========================
@app.post("/mission/demande", response_model=Mission)
async def demander_mission(demande: DemandeMission):
    # user = await get_current_user()  # Réactiver en prod si besoin

    if not demande.texte or demande.texte.strip() == "":
        raise HTTPException(status_code=422, detail="Champ 'texte' vide")

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY manquante côté serveur")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # Prompt: on demande aussi des estimations distance/péage depuis 2 départs (Mormant ou Paris)
    # et une estimation de prix "marché - fourchette haute" pour IDF.
    prompt = f"""Réponds UNIQUEMENT avec un JSON valide, sans texte avant/après, sans markdown.
Structure exacte :
{{
  "resume_client": "résumé court et clair de la demande",
  "prix_recommande_eur": 0,
  "delai_estime": "sous 1h",
  "conditions": "",
  "distance_km_mormant": 0,
  "distance_km_paris": 0,
  "peage_mormant": 0,
  "peage_paris": 0
}}

Consignes :
- Pour une demande en Île-de-France, propose un prix du marché en FOURCHETTE HAUTE.
- Pour une demande hors Île-de-France (France), estime :
  - distance_km_mormant = distance totale (km) en partant de "{STARTPOINT_A}"
  - distance_km_paris = distance totale (km) en partant de "{STARTPOINT_B}"
  - peage_mormant = péage estimé (€) depuis "{STARTPOINT_A}"
  - peage_paris = péage estimé (€) depuis "{STARTPOINT_B}"
- Si la demande contient plusieurs lieux (récupération + livraison), estime le trajet complet le plus pertinent.
- Les "conditions" peuvent être vides.

Analyse cette demande : "{demande.texte}"
"""

    # --- IA ---
    try:
        response = client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=280,
            temperature=0.3,
        )
        ai_response = response.choices[0].text.strip()
        try:
            ai_data = json.loads(ai_response)
        except json.JSONDecodeError:
            ai_data = {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # --- ROBUSTESSE ---
    if not isinstance(ai_data, dict):
        ai_data = {}

    # Defaults sûrs
    ai_data.setdefault("resume_client", demande.texte)
    ai_data.setdefault("prix_recommande_eur", 0)
    ai_data.setdefault("delai_estime", "sous 2h")
    ai_data.setdefault("conditions", "")

    # --- CONDITIONS (paiement à l'avance obligatoire + variantes pro) ---
    cond = ai_data.get("conditions")
    if cond is None or (isinstance(cond, str) and cond.strip() == ""):
        ai_data["conditions"] = build_default_conditions(demande.texte)
    else:
        ai_data["conditions"] = enforce_advance_payment(cond)

    # =========================
    # TARIFICATION FINALE (avec pire cas Mormant/Paris)
    # IDF: prix marché (IA) x 3
    # Province FR: 1€/km + 15€ + (péage x2) puis +30% marge
    # Hors France: devis
    # =========================
    if not is_france(demande.texte):
        ai_data["prix_recommande_eur"] = 0.0
        ai_data["delai_estime"] = "sur devis"
        ai_data["conditions"] = (
            "Mission hors France.\n"
            "Un devis personnalisé est obligatoire.\n"
            "Paiement à l’avance après validation du devis."
        )

    elif is_idf(demande.texte):
        raw_price_ia = _safe_float(ai_data.get("prix_recommande_eur", 0.0), 0.0)
        if raw_price_ia <= 0:
            raw_price_ia = 40.0  # sécurité minimale
        ai_data["prix_recommande_eur"] = round(raw_price_ia * COEF_IDF, 2)

    else:
        # Province: pire cas = max(Mormant, Paris)
        distance_km = max(
            _safe_float(ai_data.get("distance_km_mormant", 0.0), 0.0),
            _safe_float(ai_data.get("distance_km_paris", 0.0), 0.0),
        )
        peage = max(
            _safe_float(ai_data.get("peage_mormant", 0.0), 0.0),
            _safe_float(ai_data.get("peage_paris", 0.0), 0.0),
        )

        # Sécurités conservatrices
        if distance_km <= 0:
            distance_km = 300.0
        if peage < 0:
            peage = 0.0

        subtotal = (distance_km * PRICE_PER_KM) + MEAL_FEE + (peage * 2.0)
        final_price = round(subtotal * (1.0 + MARGIN_RATE), 2)

        ai_data["prix_recommande_eur"] = final_price
        ai_data["delai_estime"] = ai_data.get("delai_estime", "dans la journée")

    # --- DB INSERT ---
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """INSERT INTO missions (client_id, resume_client, prix_recommande_eur, delai_estime, conditions)
           VALUES (?, ?, ?, ?, ?)""",
        (
            demande.client_id,
            ai_data["resume_client"],
            float(_safe_float(ai_data["prix_recommande_eur"], 0.0)),
            ai_data["delai_estime"],
            ai_data["conditions"],
        ),
    )
    mission_id = c.lastrowid
    conn.commit()
    conn.close()

    # --- TELEGRAM (ne doit pas casser la mission si Telegram tombe) ---
    try:
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            bot = Bot(token=TELEGRAM_TOKEN)
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=(
                    f"Nouvelle mission {mission_id} : {ai_data['resume_client']} "
                    f"- Prix : {ai_data['prix_recommande_eur']} € - Délai : {ai_data['delai_estime']}"
                ),
            )
    except Exception:
        pass

    # Nettoyage: on ne renvoie que ce que Mission attend
    return Mission(
        client_id=demande.client_id,
        resume_client=str(ai_data.get("resume_client", demande.texte)),
        prix_recommande_eur=float(_safe_float(ai_data.get("prix_recommande_eur", 0.0), 0.0)),
        delai_estime=str(ai_data.get("delai_estime", "sous 2h")),
        conditions=str(ai_data.get("conditions", DEFAULT_CONDITIONS_GENERIC)),
    )


# =========================
# READ / UPDATE
# =========================
@app.get("/missions/{mission_id}")
def get_mission(mission_id: int, current_user: User = Depends(get_current_user)):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    mission = c.execute("SELECT * FROM missions WHERE id = ?", (mission_id,)).fetchone()
    conn.close()
    if not mission:
        raise HTTPException(status_code=404, detail="Mission not found")
    return {
        "id": mission[0],
        "client_id": mission[1],
        "resume_client": mission[2],
        "prix_recommande_eur": mission[3],
        "delai_estime": mission[4],
        "conditions": mission[5],
        "status": mission[6],
        "date_creation": mission[7],
    }


@app.patch("/missions/{mission_id}")
def update_mission(mission_id: int, status: Optional[str] = None, current_user: User = Depends(get_current_user)):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    if status:
        c.execute("UPDATE missions SET status = ? WHERE id = ?", (status, mission_id))
    conn.commit()
    conn.close()
    return {"message": "Mission updated"}


# =========================
# OPENAPI
# =========================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="OuiCestFait API",
        version="0.1.5",
        description="API MVP pour OuiCestFait - Gestion des missions",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
