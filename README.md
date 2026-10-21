# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Curso:** Big Data - UNAD  
**Estudiante:** [Juan Sebastian Jimenez Caicedo]  
**Código/CC:** [202016911_85]  

---

## 📌 Descripción del Proyecto

Este repositorio contiene la solución práctica a la Tarea 3 del curso, donde se abordan dos modalidades de procesamiento masivo de datos utilizando el ecosistema de Apache Spark:

1. **Procesamiento Batch (Análisis de Datos Abiertos de Salud):** Consumo vía API SODA2 del dataset de atenciones RIPS en Colombia para identificar los principales diagnósticos, la distribución por departamento y los tipos de atención médica prestados.
2. **Procesamiento en Tiempo Real (Spark Streaming + Apache Kafka):** Configuración de un clúster donde un script productor genera métricas simuladas de sensores en tiempo real enviadas a Kafka, y un consumidor en PySpark calcula promedios mediante ventanas de tiempo de 1 minuto.

---

## 📁 Archivos del Repositorio

* `batch_analysis.py`: Script PySpark para el procesamiento Batch consumiendo la API de Datos Abiertos Colombia (`4k9h-8qiu`).
* `kafka_producer.py`: Script en Python para la generación continua de eventos hacia el tópico `sensor_data` de Kafka.
* `spark_streaming_consumer.py`: Consumidor en PySpark Streaming para análisis en ventanas de tiempo.

---

## 🚀 Requisitos e Instalación

* Apache Spark 3.5.x
* Apache Kafka 3.6.2
* Python 3.10+
* Librería PySpark (`pip install pyspark kafka-python`)

---

## 💻 Ejecución

### 1. Procesamiento Batch
```bash
python3 batch_analysis.py
