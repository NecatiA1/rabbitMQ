# Gerekli kütüphaneleri içeri aktarıyoruz
import pika
import json
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from typing import List, Dict
from datetime import datetime

# --- Veri Modelleri (Pydantic) ---

class Message(BaseModel):
    sender_id: int
    receiver_id: int
    subject: str
    content: str
    timestamp: str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class User(BaseModel):
    id: int
    name: str
    email: EmailStr # E-posta formatını doğrular

class NewUser(BaseModel):
    name: str
    email: EmailStr

class LoginRequest(BaseModel):
    email: EmailStr

# --- FastAPI Uygulaması ve Ayarları ---

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- RabbitMQ Ayarları ---
RABBITMQ_URL = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672')
url_params = pika.URLParameters(RABBITMQ_URL)

# --- Kullanıcı Veri Yönetimi ---
USER_FILE = "kullanicilar.json"

def load_users() -> List[Dict]:
    """Kullanıcıları JSON dosyasından okur."""
    if not os.path.exists(USER_FILE):
        return []
    try:
        with open(USER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_users(users: List[Dict]):
    """Kullanıcı listesini JSON dosyasına yazar."""
    with open(USER_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2, ensure_ascii=False)

# --- API Endpoint'leri ---

@app.get("/")
def read_root():
    return {"mesaj": "Mail Uygulaması API'sine Hoş Geldiniz!"}

@app.get("/users", response_model=List[User])
def get_users():
    return load_users()

@app.post("/register", response_model=User)
def register_user(new_user: NewUser):
    """Yeni bir kullanıcı kaydeder."""
    users = load_users()
    
    # E-postanın zaten kayıtlı olup olmadığını kontrol et
    if any(user['email'] == new_user.email for user in users):
        raise HTTPException(status_code=400, detail="Bu e-posta adresi zaten kayıtlı.")

    # Yeni kullanıcı için bir ID oluştur
    new_id = max([user['id'] for user in users]) + 1 if users else 1
    
    user_dict = {
        "id": new_id,
        "name": new_user.name,
        "email": new_user.email
    }
    
    users.append(user_dict)
    save_users(users)
    
    return user_dict

@app.post("/login", response_model=User)
def login_user(login_request: LoginRequest):
    """Kullanıcının e-posta ile giriş yapmasını sağlar."""
    users = load_users()
    
    for user in users:
        if user['email'] == login_request.email:
            return user
            
    raise HTTPException(status_code=404, detail="Bu e-posta adresine sahip kullanıcı bulunamadı.")


@app.post("/messages")
def send_message(msg: Message):
    users = load_users()
    user_map = {user["id"]: user for user in users}
    
    if msg.sender_id not in user_map or msg.receiver_id not in user_map:
        raise HTTPException(status_code=404, detail="Gönderici veya alıcı bulunamadı.")

    receiver_queue_name = f"user_queue_{msg.receiver_id}"
    connection = None
    try:
        connection = pika.BlockingConnection(url_params)
        channel = connection.channel()
        channel.queue_declare(queue=receiver_queue_name, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=receiver_queue_name,
            body=json.dumps(msg.dict()),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        return {"status": "Mesaj başarıyla gönderildi."}
    except Exception as e:
        print(f"Mesaj gönderilirken hata: {e}")
        raise HTTPException(status_code=500, detail="Mesaj gönderilemedi.")
    finally:
        if connection and connection.is_open:
            connection.close()


@app.get("/messages/check/{user_id}")
def check_for_messages(user_id: int):
    users = load_users()
    user_map = {user["id"]: user for user in users}

    if user_id not in user_map:
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı.")

    user_queue_name = f"user_queue_{user_id}"
    messages_to_return = []
    connection = None
    try:
        connection = pika.BlockingConnection(url_params)
        channel = connection.channel()
        channel.queue_declare(queue=user_queue_name, durable=True)

        while True:
            method_frame, properties, body = channel.basic_get(queue=user_queue_name, auto_ack=False)
            if method_frame is None:
                break
            
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            message_data = json.loads(body)
            sender_info = user_map.get(message_data.get("sender_id"))
            if sender_info:
                message_data["sender_name"] = sender_info.get("name")
            messages_to_return.append(message_data)
        
        return {"status": "ok", "messages": messages_to_return}
    except Exception as e:
        print(f"Mesaj kontrol edilirken hata: {e}")
        raise HTTPException(status_code=500, detail="Mesajlar kontrol edilemedi.")
    finally:
        if connection and connection.is_open:
            connection.close()
