from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

# Inicjalizacja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest' # Zmienione na latest, by monitorować ruch "na żywo"
)

# Słownik: user_id -> [lista obiektów datetime]
user_history = defaultdict(list)

print("Detektor anomalii uruchomiony. Monitorowanie transakcji...")

try:
    for message in consumer:
        tx = message.value
        user_id = tx['user_id']
        # Parsowanie timestampu z ISO format
        current_tx_time = datetime.fromisoformat(tx['timestamp'])
        
        # 1. Dodaj bieżącą transakcję do historii usera
        user_history[user_id].append(current_tx_time)
        
        # 2. Usuń stare transakcje (starsze niż 60s względem obecnej)
        threshold = current_tx_time - timedelta(seconds=60)
        user_history[user_id] = [t for t in user_history[user_id] if t > threshold]
        
        # 3. Sprawdź warunek alertu (> 3 transakcje)
        tx_count = len(user_history[user_id])
        if tx_count > 3:
            print(f"⚠️  ALERT: Wykryto serię transakcji!")
            print(f"   Użytkownik: {user_id}")
            print(f"   Liczba operacji: {tx_count} w ciągu ostatnich 60s")
            print(f"   Ostatnia transakcja: {tx['tx_id']} ({tx['amount']} PLN)\n")
        else:
            # Opcjonalny log dla potwierdzenia działania
            print(f"OK: {tx['tx_id']} dla {user_id} (Suma w oknie: {tx_count})")

except KeyboardInterrupt:
    print("\nZatrzymywanie konsumenta...")
finally:
    consumer.close()
