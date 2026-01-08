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

DB_NAME = "ouicestfait.db"
BASE_MARGIN = 1.25
COMMISSION = 0.15

# Clé OpenAI (ta vraie clé)
OPENAI_API_KEY = "sk-XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"  # ← TA VRAIE CLÉ !
client = OpenAI(api_key=OPENAI_API_KEY)

# Telegram pour notifications
TELEGRAM_TOKEN = "ton_token_bot_ici"  # ← Remplace par ton token BotFather
TELEGRAM_CHAT_ID = "ton_chat_id_ici"  # ← Remplace par ton chat ID (ex. '123456789')

bot = Bot(token=TELEGRAM_TOKEN)

# Auth JWT
SECRET_KEY = "ton_secret_super_long_change_le_par_quelque_chose_de_robuste"  # Change ça !
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440  # 24h

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Users MVP (mot de passe hashé avec bcrypt)
users_db = {
    "client": {
        "username": "client",
        "full_name": "Client MVP",
        "hashed_password": bcrypt.hashpw(b"clientpass", bcrypt.gensalt()).decode('utf-8'),
        "role": "client"
    },
    "operateur": {
        "username": "operateur",
        "full_name": "Opérateur MVP",
        "hashed_password": bcrypt.hashpw(b"operateurpass", bcrypt.gensalt()).decode('utf-8'),
        "role": "operateur"
    }
}

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Dépendance pour vérifier le token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None:
            raise HTTPException(status_code=401, detail="Token invalide")
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide")

app = FastAPI(
    title="OuiCestFait API",
    version="0.1.3",
    description="API MVP pour OuiCestFait - Gestion des missions de conciergerie"
)

def get_db():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    db = get_db()
    cur = db.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT CHECK(role IN ('client','operateur','admin')) NOT NULL,
        name TEXT,
        email TEXT,
        phone TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS missions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER,
        raw_request TEXT NOT NULL,
        type_service TEXT,
        depart TEXT,
        arrivee TEXT,
        delai_estime TEXT,
        prix_recommande REAL,
        conditions TEXT,
        niveau_risque TEXT,
        statut TEXT DEFAULT 'en_attente_validation',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS ia_decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mission_id INTEGER,
        ia_output_json TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS validations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mission_id INTEGER,
        operateur_id INTEGER,
        action TEXT,
        commentaire TEXT,
        prix_final REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """)
    db.commit()
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (role, name) VALUES ('client', 'Client MVP')")
        cur.execute("INSERT INTO users (role, name) VALUES ('operateur', 'Opérateur MVP')")
        db.commit()
    db.close()

@app.on_event("startup")
def startup():
    init_db()

class MissionRequest(BaseModel):
    texte: str
    client_id: int = 1

    class Config:
        json_schema_extra = {
            "example": {
                "texte": "Urgent : aller chercher un colis ce soir à Paris",
                "client_id": 1
            }
        }

class ValidationRequest(BaseModel):
    mission_id: int
    operateur_id: int = 2
    action: str
    prix_final: Optional[float] = None
    commentaire: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "mission_id": 1,
                "operateur_id": 2,
                "action": "validee",
                "prix_final": 180.0,
                "commentaire": "Prix ajusté pour l'urgence"
            }
        }

def ia_analyse(texte: str) -> dict:
    prompt = f"""
Analyse ce texte de demande client pour une mission de conciergerie :
"{texte}"

Retourne UNIQUEMENT un JSON valide avec ces clés exactes :
{{
  "resume_client": "résumé court de la demande",
  "type_service": "conciergerie" ou "convoyage" ou "livraison" ou "courses" ou "autre",
  "solution": "phrase courte expliquant la solution",
  "depart": "ville ou 'À préciser'",
  "arrivee": "ville ou 'À préciser'",
  "delai_estime": "estimation en texte (ex. 'sous 2h', '24h')",
  "prix_recommande_eur": nombre entier,
  "conditions": ["condition1", "condition2"],
  "niveau_risque": "faible" ou "moyen" ou "élevé",
  "commentaire_operateur": "texte pour l'opérateur"
}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=500
        )
        raw_content = response.choices[0].message.content.strip()
        if raw_content.startswith("```json"):
            raw_content = raw_content[7:-3].strip()
        ia_output = json.loads(raw_content)
    except Exception as e:
        print(f"Erreur OpenAI : {e}")
        # Fallback
        t = texte.lower()
        type_service = "conciergerie"
        if "voiture" in t or "garage" in t:
            type_service = "convoyage"
        elif "colis" in t or "livraison" in t or "document" in t:
            type_service = "livraison"
        elif "courses" in t or "pharmacie" in t:
            type_service = "courses"

        distance_km = 100
        delai = "24 à 48h"
        risque = "faible"
        if "urgent" in t or "ce soir" in t:
            delai, risque = "sous 2 à 6h (premium)", "moyen"

        cout_estime = 0.8 * distance_km
        prix_interne = cout_estime * BASE_MARGIN
        prix_final = prix_interne * (1 + COMMISSION)

        ia_output = {
            "resume_client": texte,
            "type_service": type_service,
            "solution": "Toujours une solution : immédiate, différée ou premium",
            "depart": "À préciser",
            "arrivee": "À préciser",
            "delai_estime": delai,
            "prix_recommande_eur": round(prix_final, 2),
            "conditions": ["paiement à l’avance", "preuve de réalisation"],
            "niveau_risque": risque,
            "commentaire_operateur": "Validation humaine requise (MVP)"
        }
    return ia_output

@app.get("/health", summary="Vérifie l'état de l'API et de la base de données")
def health():
    return {"status": "ok", "db": DB_NAME}

@app.post("/token", summary="Login client ou opérateur")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = users_db.get(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    access_token = create_access_token(
        data={"sub": form_data.username, "role": user["role"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/mission/demande", summary="Créer une nouvelle mission (client authentifié)")
def creer_mission(data: MissionRequest, current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in ["client", "admin"]:
        raise HTTPException(status_code=403, detail="Accès réservé aux clients")
    ia_output = ia_analyse(data.texte)
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """INSERT INTO missions (client_id, raw_request, type_service, depart, arrivee, delai_estime,
                                 prix_recommande, conditions, niveau_risque, statut)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data.client_id,
            data.texte,
            ia_output["type_service"],
            ia_output["depart"],
            ia_output["arrivee"],
            ia_output["delai_estime"],
            ia_output["prix_recommande_eur"],
            json.dumps(ia_output["conditions"], ensure_ascii=False),
            ia_output["niveau_risque"],
            "en_attente_validation",
        ),
    )
    mission_id = cur.lastrowid
    cur.execute(
        "INSERT INTO ia_decisions (mission_id, ia_output_json) VALUES (?, ?)",
        (mission_id, json.dumps(ia_output, ensure_ascii=False)),
    )
    db.commit()
    db.close()
    return {
        "mission_id": mission_id,
        "proposition_ia": ia_output,
        "message_client": f"✅ Mission possible\n💰 Prix estimé : {ia_output['prix_recommande_eur']} €\n⏱ Délai : {ia_output['delai_estime']}\n🔒 Paiement requis pour lancement"
    }

@app.post("/mission/validation", summary="Valider ou ajuster une mission (opérateur uniquement)")
def valider_mission(data: ValidationRequest, current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "operateur":
        raise HTTPException(status_code=403, detail="Accès réservé aux opérateurs")
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT prix_recommande FROM missions WHERE id=?", (data.mission_id,))
    row = cur.fetchone()
    if not row:
        db.close()
        raise HTTPException(status_code=404, detail="Mission introuvable")
    prix_actuel = row[0]
    prix_final = data.prix_final if data.prix_final is not None else prix_actuel
    cur.execute(
        "UPDATE missions SET statut='validee', prix_recommande=? WHERE id=?",
        (prix_final, data.mission_id),
    )
    cur.execute(
        "INSERT INTO validations (mission_id, operateur_id, action, commentaire, prix_final) VALUES (?, ?, ?, ?, ?)",
        (data.mission_id, data.operateur_id, data.action, data.commentaire, prix_final),
    )
    db.commit()
    db.close()
    # Notification Telegram
    message = f"✅ Mission {data.mission_id} validée !\nPrix final : {prix_final} €\nCommentaire : {data.commentaire or 'Aucun'}\nPar opérateur : {current_user['username']}"
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
    return {
        "status": "ok",
        "mission_id": data.mission_id,
        "prix_final": prix_final,
        "message_client": "✅ Mission validée. Procédez au paiement pour lancement."
    }

# Personnalisation OpenAPI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="OuiCestFait API",
        version="0.1.3",
        description="API MVP pour OuiCestFait - Gestion des missions",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi