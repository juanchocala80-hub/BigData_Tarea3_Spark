# Tarea 3 - Procesamiento de Datos con Apache Spark y Kafka

**Curso:** Big Data - UNAD  
**Estudiante:** Juan Sebastian Jimenez Caicedo  
**Código/CC:** 202016911_85  

---

## 📋 Descripción del Proyecto

Este proyecto implementa dos arquitecturas de procesamiento de Big Data utilizando el ecosistema de **Apache Spark**:

1. **Procesamiento Batch (Lotes):** Análisis exploratorio de datos (EDA) sobre 50.000 registros médicos obtenidos desde la API SODA2 de **Datos Abiertos Colombia (RIPS)**.
2. **Procesamiento Streaming (Tiempo Real):** Sistema original de **Detección de Transacciones Bancarias Sospechosas (Alerta de Fraude)** conectando **Apache Kafka** con **PySpark Structured Streaming**.

---

## 📁 Estructura del Repositorio

* `README.md`: Documentación completa del proyecto e instrucciones de ejecución.
* `batch_analysis.py`: Script PySpark para la extracción desde API REST, limpieza y análisis exploratorio del dataset RIPS.
* `kafka_producer_custom.py`: Generador/Productor en Python que simula un flujo continuo de transacciones financieras hacia el tópico `transacciones_bancarias`.
* `spark_streaming_consumer_custom.py`: Consumidor PySpark Streaming que procesa los eventos en tiempo real, clasifica el nivel de riesgo y calcula agregaciones financieras en vivo.

---

## 🛠️ Requisitos e Instalación

### Requisitos del Sistema
* Java OpenJDK 11 / 17
* Apache Spark 3.5.x
* Apache Kafka 3.6.x (con Zookeeper)
* Python 3.10+
* Biblioteca de Python: `kafka-python` (`pip install kafka-python`)

---

## 🚀 Instrucciones de Ejecución

### 1. Procesamiento Batch (RIPS Salud Colombia)
Para ejecutar el análisis exploratorio de los 50.000 registros médicos:

```bash
python3 batch_analysis.py
