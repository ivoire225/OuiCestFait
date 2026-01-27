"""
OuiCestFait - FastAPI MVP (monofichier)
- Endpoint principal: POST /mission/demande
- Reçoit: { "client_id": "<whatsapp_id>", "texte": "..." }
- Retourne: résumé + prix + délai + conditions
- Notifie Telegram (si variables d'env renseignées)
- Auth JWT (création user + token) conservée, mais /mission/demande est laissé en mode "test" (sans auth) comme dans ton code.

✅ Correctifs clés:
- client_id => str (corrige l'erreur 422: "Input should be a valid string")
- bcrypt stocké en texte (pas bytes) -> verify fiable
- Tarification finale intégrée (IDF / Province / Hors France)
- Conditions par défaut + variante “PRO” (selon type détecté)
- Paiement 100% à l’avance (toujours)
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Tuple
import sqlite3
import json
from fastapi.openapi.utils import get_openapi
from openai import OpenAI
from jose import JWTError, jwt
import bcrypt
from datetime import datetime, timedelta
import os

# Telegram (optionnel)
try:
    from telegram import Bot
except Exception:
    Bot = None  # type: ignore


# =========================
# CONFIG
# =========================
DB_NAME = os.getenv("DB_NAME", "ouicestfait.db")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # tu peux changer via .env

SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_ME_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# =========================
# CONDITIONS (DEFAULT + PRO)
# =========================
CONDITIONS_DEFAULT = (
    "Paiement 100% à l’avance.\n"
    "Preuve de prise en charge et de livraison (photo / signature / message).\n"
    "Annulation : si l’intervention a déjà commencé, facturation partielle possible.\n"
    "Objet/documents à remettre dans une enveloppe/packaging adapté (si nécessaire).\n"
)

CONDITIONS_BY_TYPE_PRO: Dict[str, str] = {
    "livraison": (
        "Paiement 100% à l’avance.\n"
        "Créneau et adresse précis obligatoires.\n"
        "Preuve de prise en charge + preuve de livraison.\n"
        "Si attente > 10 min sur place : supplément possible.\n"
    ),
    "courses": (
        "Paiement 100% à l’avance.\n"
        "Liste d’achats + budget + préférence de marques à préciser.\n"
        "Ticket de caisse fourni.\n"
        "Si article indisponible : remplacement validé par message.\n"
    ),
    "transport": (
        "Paiement 100% à l’avance.\n"
        "Point de départ / arrivée + horaire.\n"
        "Bagages/volume à préciser.\n"
        "Supplément possible en cas de détour ou attente longue.\n"
    ),
    "urgence": (
        "Paiement 100% à l’avance.\n"
        "Priorité immédiate selon disponibilité.\n"
        "Adresse / contact sur place obligatoire.\n"
        "Supplément urgence inclus dans le tarif.\n"
    ),
    "autre": CONDITIONS_DEFAULT,
}


# =========================
# TARIFICATION FINALE (ton choix)
# =========================
# IDF: "prix du marché fourchette haute" * 3
IDF_MARKET_COEF = 3.0

# Province/Hors IDF:
# Prix = (1€/km + 15€ + (péage estimé * 2)) puis +30% marge globale
EUR_PER_KM = 1.0
MEAL_EUR = 15.0
TOLL_EST_PER_KM = 0.12  # estimation simple (à défaut d'API péage)
TOLL_MULTIPLIER = 2.0
GLOBAL_MARGIN = 0.30

# Hors France: sur devis
INTERNATIONAL_ON_QUOTE = True

# “Pire cas” départ: Mormant 77720 ou Paris => on prend le plus long
# Sans géocodage, on intègre un buffer km "pire cas".
# (Distance approx Paris↔Mormant ~ 60 km ; buffer volontairement prudent)
WORST_CASE_DEPARTURE_BUFFER_KM = 70


# =========================
# MODELES Pydantic
# =========================
class DemandeMission(BaseModel):
    # ✅ IMPORTANT: string (WhatsApp ID / numéro)
    client_id: str = Field(..., description="Identifiant client (WhatsApp ID / téléphone).")
    texte: str = Field(..., description="Demande client en texte libre.")


class Mission(BaseModel):
    client_id: str
    resume_client: str
    prix_recommande_eur: Optional[float] = None
    delai_estime: str
    conditions: Optional[str] = None
    zone_tarifaire: Optional[str] = None
    type_detecte: Optional[str] = None


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
        """CREATE TABLE IF NOT EXISTS missions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id TEXT,
            resume_client TEXT,
            prix_recommande_eur REAL,
            delai_estime TEXT,
            conditions TEXT,
            zone_tarifaire TEXT,
            type_detecte TEXT,
            status TEXT DEFAULT 'pending',
            date_creation DATETIME DEFAULT CURRENT_TIMESTAMP
        )"""
    )

    c.execute(
        """CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            hashed_password TEXT
        )"""
    )

    conn.commit()
    conn.close()


create_db()


# =========================
# AUTH
# =========================
def hash_password(password: str) -> str:
    # bcrypt renvoie bytes => on stocke en string utf-8
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed_password: str) -> bool:
    # hashed_password stocké en texte utf-8
    return bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta if expires_delta else timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: Optional[str] = payload.get("sub")
        if not username:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    user = c.execute("SELECT username, hashed_password FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()
    if user is None:
        raise credentials_exception

    return User(username=username)


@app.post("/users/")
def create_user(form_data: OAuth2PasswordRequestForm = Depends()):
    hashed = hash_password(form_data.password)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, hashed_password) VALUES (?, ?)", (form_data.username, hashed))
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
    user = c.execute("SELECT username, hashed_password FROM users WHERE username = ?", (form_data.username,)).fetchone()
    conn.close()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    username, hashed_password = user[0], user[1]
    if not verify_password(form_data.password, hashed_password):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


# =========================
# IA + TARIFICATION
# =========================
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _clamp_price(price: Optional[float], minimum: float = 0.0, maximum: float = 99999.0) -> Optional[float]:
    if price is None:
        return None
    return max(minimum, min(maximum, price))


def detect_zone_and_type_fallback(texte: str) -> Tuple[str, str]:
    t = texte.lower()

    # Hors France
    international_keywords = ["maroc", "tunisie", "algérie", "senegal", "côte d'ivoire", "cote d'ivoire", "belgique", "suisse", "italie", "espagne", "allemagne", "portugal", "londres", "uk", "usa"]
    if any(k in t for k in international_keywords):
        return "INTERNATIONAL", "autre"

    # IDF heuristique
    idf_keywords = [
        "paris", "ile-de-france", "île-de-france", "idf",
        "77", "78", "91", "92", "93", "94", "95",
        "seine-et-marne", "yvelines", "essonne", "hauts-de-seine",
        "seine-saint-denis", "val-de-marne", "val-d'oise",
        "mormant", "melun", "fontainebleau", "meaux", "evry", "créteil", "nanterre", "versailles"
    ]
    zone = "IDF" if any(k in t for k in idf_keywords) else "PROVINCE"

    # type heuristique
    if any(k in t for k in ["urgence", "urgent", "immédiat", "immediat", "ce soir", "dans 1h", "dans une heure"]):
        service_type = "urgence"
    elif any(k in t for k in ["courses", "supermarché", "supermarche", "pharmacie", "acheter", "achats"]):
        service_type = "courses"
    elif any(k in t for k in ["transport", "déplacer", "deplacer", "trajet", "navette", "voiture", "chauffeur"]):
        service_type = "transport"
    elif any(k in t for k in ["livrer", "livraison", "colis", "documents", "récupérer", "recuperer", "remettre"]):
        service_type = "livraison"
    else:
        service_type = "autre"

    return zone, service_type


def estimate_distance_km_basic(texte: str) -> float:
    """
    Sans géocodage, on ne peut pas calculer des km réels.
    On applique une estimation prudente pour la province, basée sur indices.
    - Si le texte contient 'Paris' + grande ville de province => on met une valeur haute "standard".
    - Sinon, fallback: 120 km.
    """
    t = texte.lower()

    # cas typiques "Paris -> Lyon" etc (valeurs prudentes)
    pairs = [
        (["paris", "lyon"], 520),
        (["paris", "lille"], 230),
        (["paris", "marseille"], 780),
        (["paris", "bordeaux"], 590),
        (["paris", "nantes"], 385),
        (["paris", "toulouse"], 680),
        (["paris", "strasbourg"], 490),
        (["paris", "nice"], 930),
        (["mormant", "lyon"], 480),
        (["mormant", "lille"], 280),
        (["mormant", "marseille"], 760),
        (["mormant", "bordeaux"], 600),
        (["mormant", "nantes"], 420),
        (["mormant", "toulouse"], 690),
        (["mormant", "strasbourg"], 420),
        (["mormant", "nice"], 900),
    ]
    for keys, km in pairs:
        if all(k in t for k in keys):
            return float(km)

    # Si on voit un indice "province"
    province_cities = ["lyon", "lille", "marseille", "bordeaux", "nantes", "toulouse", "strasbourg", "nice", "rennes", "grenoble", "montpellier"]
    if any(c in t for c in province_cities):
        return 450.0

    return 120.0


def compute_price_idf(market_high_price: float) -> float:
    return _clamp_price(market_high_price * IDF_MARKET_COEF, 0.0, 99999.0) or 0.0


def compute_price_province(distance_km: float) -> Tuple[float, float, float]:
    """
    Prix province:
    base = 1€/km + 15€ + (péage_estimé*2)
    puis total = base * (1 + 30% marge)
    péage_estimé = distance_km * 0.12 (heuristique) ; ensuite *2 comme demandé
    """
    toll_est = distance_km * TOLL_EST_PER_KM
    base = (EUR_PER_KM * distance_km) + MEAL_EUR + (toll_est * TOLL_MULTIPLIER)
    total = base * (1.0 + GLOBAL_MARGIN)
    return _clamp_price(total, 0.0, 99999.0) or 0.0, toll_est, base


def build_conditions(service_type: str, pro: bool = True) -> str:
    base = CONDITIONS_BY_TYPE_PRO.get(service_type, CONDITIONS_DEFAULT) if pro else CONDITIONS_DEFAULT
    # Toujours paiement d'avance (déjà inclus), on ajoute une ligne "sûreté"
    return base.strip()


def ai_extract_market_high_and_delay(client: OpenAI, texte: str) -> Dict[str, Any]:
    """
    Objectif IA: donner une estimation "fourchette haute marché" (IDF) + délai + résumé + type/zone + si possible distance/péage.
    On ne lui demande PAS de faire une étude de marché web (elle ne peut pas).
    """
    zone_guess, type_guess = detect_zone_and_type_fallback(texte)

    system = (
        "Tu es un assistant de devis pour une conciergerie. "
        "Tu dois répondre uniquement en JSON valide. Pas de texte, pas de markdown."
    )
    user = f"""
Donne un JSON STRICT avec ces clés (toutes obligatoires) :
{{
  "resume_client": "résumé court et clair",
  "delai_estime": "ex: sous 1h, demain matin",
  "zone": "IDF ou PROVINCE ou INTERNATIONAL",
  "type_service": "livraison ou courses ou transport ou urgence ou autre",
  "market_high_price_eur": nombre (fourchette haute) OU null,
  "distance_km": nombre OU null
}}

Contexte:
- Le départ opérationnel peut être Mormant (77720) OU Paris ; on prendra le pire cas.
- Si INTERNATIONAL: mets market_high_price_eur = null et distance_km = null.
- Si IDF: mets market_high_price_eur (fourchette haute) cohérente avec la demande.
- Si PROVINCE: mets distance_km (approx) si tu peux déduire (sinon null).
- Si tu hésites, garde zone="{zone_guess}" et type_service="{type_guess}".

Demande client: {texte}
""".strip()

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.2,
            max_tokens=250,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        return data
    except Exception:
        # fallback robust
        return {
            "resume_client": texte,
            "delai_estime": "à confirmer",
            "zone": zone_guess,
            "type_service": type_guess,
            "market_high_price_eur": None,
            "distance_km": None,
        }


async def notify_telegram(mission_id: int, resume: str, prix: Optional[float], delai: str):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID and Bot):
        return
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        price_txt = f"{prix:.2f} €" if isinstance(prix, (int, float)) else "sur devis"
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"Nouvelle mission {mission_id} : {resume} - Prix : {price_txt} - Délai : {delai}",
        )
    except Exception:
        # on ne bloque pas l’API si Telegram fail
        return


# =========================
# ENDPOINTS
# =========================
@app.post("/mission/demande", response_model=Mission)
async def demander_mission(demande: DemandeMission):
    # (option) Activer l'auth plus tard:
    # user = await get_current_user()

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY manquante dans l'environnement.")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # 1) IA (résumé/délai + zone/type + base marché/distance)
    ai = ai_extract_market_high_and_delay(client, demande.texte)

    resume_client = str(ai.get("resume_client") or demande.texte).strip()
    delai_estime = str(ai.get("delai_estime") or "à confirmer").strip()

    zone = str(ai.get("zone") or "").upper().strip()
    type_service = str(ai.get("type_service") or "autre").lower().strip()

    if zone not in {"IDF", "PROVINCE", "INTERNATIONAL"}:
        zone, _ = detect_zone_and_type_fallback(demande.texte)

    if type_service not in {"livraison", "courses", "transport", "urgence", "autre"}:
        _, type_service = detect_zone_and_type_fallback(demande.texte)

    # 2) Tarification finale
    prix_final: Optional[float] = None

    if zone == "INTERNATIONAL" and INTERNATIONAL_ON_QUOTE:
        prix_final = None  # sur devis
    elif zone == "IDF":
        market_high = _safe_float(ai.get("market_high_price_eur"))
        # fallback si IA ne fournit pas de base marché
        if market_high is None:
            market_high = 80.0  # valeur haute par défaut (modifiable)
        prix_final = compute_price_idf(market_high)
    else:
        # PROVINCE
        km = _safe_float(ai.get("distance_km"))
        if km is None:
            km = estimate_distance_km_basic(demande.texte)

        # pire cas départ Mormant/Paris => buffer km
        km = float(km) + WORST_CASE_DEPARTURE_BUFFER_KM

        prix, _toll_est, _base = compute_price_province(km)
        prix_final = prix

    # 3) Conditions (PRO par défaut)
    conditions = build_conditions(type_service, pro=True)

    # 4) DB save
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """INSERT INTO missions
           (client_id, resume_client, prix_recommande_eur, delai_estime, conditions, zone_tarifaire, type_detecte)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            demande.client_id,
            resume_client,
            prix_final if prix_final is not None else None,
            delai_estime,
            conditions,
            zone,
            type_service,
        ),
    )
    mission_id = c.lastrowid
    conn.commit()
    conn.close()

    # 5) Telegram (optionnel)
    await notify_telegram(mission_id, resume_client, prix_final, delai_estime)

    return Mission(
        client_id=demande.client_id,
        resume_client=resume_client,
        prix_recommande_eur=prix_final,
        delai_estime=delai_estime,
        conditions=conditions,
        zone_tarifaire=zone,
        type_detecte=type_service,
    )


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
        "zone_tarifaire": mission[6],
        "type_detecte": mission[7],
        "status": mission[8],
        "date_creation": mission[9],
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
# OpenAPI custom
# =========================
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="OuiCestFait API",
        version="0.2.0",
        description="API MVP pour OuiCestFait - Gestion des missions",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
