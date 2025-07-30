# Gerekli kütüphaneleri içeri aktarıyoruz
import pika
import json
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
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
    email: str

# --- FastAPI Uygulaması ve Ayarları ---

app = FastAPI()

# Gelen isteklere izin vermek için CORS ayarları
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Herkese izin ver (geliştirme için)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- RabbitMQ Ayarları ---
RABBITMQ_URL = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672')
url_params = pika.URLParameters(RABBITMQ_URL)

# --- Başlangıç Verisi ---
try:
    with open("kullanicilar.json", "r", encoding="utf-8") as f:
        users = json.load(f)
        user_map = {user["id"]: user for user in users}
except FileNotFoundError:
    print("HATA: kullanicilar.json dosyası bulunamadı!")
    users = []
    user_map = {}

# --- API Endpoint'leri ---

@app.get("/")
def read_root():
    return {"mesaj": "Mail Uygulaması API'sine Hoş Geldiniz!"}

@app.get("/users", response_model=List[User])
def get_users():
    return users

@app.post("/messages")
def send_message(msg: Message):
    if msg.sender_id not in user_map or msg.receiver_id not in user_map:
        raise HTTPException(status_code=404, detail="Gönderici veya alıcı bulunamadı.")

    receiver_queue_name = f"user_queue_{msg.receiver_id}"

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
        connection.close()
        return {"status": "Mesaj başarıyla gönderildi."}
    except Exception as e:
        print(f"Mesaj gönderilirken hata: {e}")
        raise HTTPException(status_code=500, detail="Mesaj gönderilemedi.")


@app.get("/messages/check/{user_id}")
def check_for_messages(user_id: int): # Fonksiyon adı güncellendi
    """
    Belirtilen kullanıcının kişisel kuyruğundaki TÜM mesajları çekip döndürür.
    Bu, polling için çok daha verimli bir yöntemdir.
    """
    if user_id not in user_map:
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı.")

    user_queue_name = f"user_queue_{user_id}"
    messages_to_return = []

    try:
        connection = pika.BlockingConnection(url_params)
        channel = connection.channel()
        channel.queue_declare(queue=user_queue_name, durable=True)

        # Kuyrukta mesaj kalmayana kadar hepsini çek
        while True:
            # auto_ack=False, mesajı bizim onayımızla sileceğimizi belirtir
            method_frame, properties, body = channel.basic_get(queue=user_queue_name, auto_ack=False)
            
            # Kuyrukta başka mesaj yoksa döngüden çık
            if method_frame is None:
                break

            # Mesajı işlediğimizi ve kuyruktan silebileceğini hemen bildiriyoruz.
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            message_data = json.loads(body)
            sender_info = user_map.get(message_data.get("sender_id"))
            if sender_info:
                message_data["sender_name"] = sender_info.get("name")
            
            messages_to_return.append(message_data)

        connection.close()
        
        # Dönen veri yapısı güncellendi: 'messages' adında bir liste içeriyor
        return {"status": "ok", "messages": messages_to_return}

    except Exception as e:
        print(f"Mesaj kontrol edilirken hata: {e}")
        raise HTTPException(status_code=500, detail="Mesajlar kontrol edilemedi.")
