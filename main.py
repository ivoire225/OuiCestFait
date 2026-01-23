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
import os  # For os.getenv

DB_NAME = "ouicestfait.db"
BASE_PRICE = 50
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class DemandeMission(BaseModel):
    client_id: int
    texte: str

class Mission(BaseModel):
    client_id: int
    resume_client: str
    prix_recommande_eur: float
    delai_estime: str
    conditions: Optional[str] = None

class User(BaseModel):
    username: str

class UserInDB(User):
    hashed_password: str

def create_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS missions
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  client_id INTEGER,
                  resume_client TEXT,
                  prix_recommande_eur REAL,
                  delai_estime TEXT,
                  conditions TEXT,
                  status TEXT DEFAULT 'pending',
                  date_creation DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY,
                  hashed_password TEXT)''')
    conn.commit()
    conn.close()

create_db()

def hash_password(password: str):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

def verify_password(password: str, hashed_password: str):
    return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
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

@app.post("/users/")
def create_user(form_data: OAuth2PasswordRequestForm = Depends()):
    hashed_password = hash_password(form_data.password)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, hashed_password) VALUES (?, ?)", (form_data.username, hashed_password))
        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already registered")
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
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/mission/demande", response_model=Mission)
async def demander_mission(demande: DemandeMission):
    # user = await get_current_user()  # Commenté pour test

    client = OpenAI(api_key=OPENAI_API_KEY)
    prompt = f"""Réponds UNIQUEMENT avec un JSON valide, sans aucun texte avant ou après, sans explication, sans markdown, sans guillemets supplémentaires. Structure exacte :
    {{
      "resume_client": "résumé court et clair de la demande",
      "prix_recommande_eur": nombre entier ou décimal (ex. 80.0),
      "delai_estime": "délai court (ex. sous 1h, demain matin)",
      "conditions": "notes courtes ou vide"
    }}
    Analyse cette demande : '{demande.texte}'"""

    try:
        response = client.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=prompt,
            max_tokens=200,
            temperature=0.7
        )
        ai_response = response.choices[0].text.strip()
        try:
            ai_data = json.loads(ai_response)
        except json.JSONDecodeError:
            ai_data = {
                "resume_client": demande.texte,
                "prix_recommande_eur": 80.0,
                "delai_estime": "sous 2h",
                "conditions": "Erreur d'analyse IA, prix par défaut"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''INSERT INTO missions (client_id, resume_client, prix_recommande_eur, delai_estime, conditions) VALUES (?, ?, ?, ?, ?)''',
              (demande.client_id, ai_data['resume_client'], ai_data['prix_recommande_eur'], ai_data['delai_estime'], ai_data.get('conditions', None)))
    mission_id = c.lastrowid
    conn.commit()
    conn.close()

    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"Nouvelle mission {mission_id} : {ai_data['resume_client']} - Prix : {ai_data['prix_recommande_eur']} € - Délai : {ai_data['delai_estime']}")

    return Mission(client_id=demande.client_id, **ai_data)

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
        "date_creation": mission[7]
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
