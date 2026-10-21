import time
import json
import random
from kafka import KafkaProducer

# Configuración del Productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'transacciones_bancarias'
ciudades = ['Bogota', 'Medellin', 'Cali', 'Barranquilla', 'Bucaramanga']
tipos_transaccion = ['RETIRO', 'TRANSFERENCIA', 'PAGO_TIENDA', 'COMPRA_ONLINE']

print(f"=== INICIANDO GENERADOR DE TRANSACCIONES BANCARIAS EN TIEMPO REAL EN TÓPICO: '{topic_name}' ===")

try:
    while True:
        # Generar evento de transacción simulado
        data = {
            "id_transaccion": f"TX-{random.randint(10000, 99999)}",
            "id_cuenta": f"CTA-{random.randint(100, 500)}",
            "monto": round(random.uniform(5000, 3500000), 2),  # Montos entre $5,000 y $3,500,000 COP
            "ciudad": random.choice(ciudades),
            "tipo": random.choice(tipos_transaccion),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        producer.send(topic_name, value=data)
        print(f"[EVENTO ENVIADO] {data}")
        time.sleep(1) # Envía un evento cada segundo

except KeyboardInterrupt:
    print("\nProductor detenido exitosamente.")
    producer.close()
