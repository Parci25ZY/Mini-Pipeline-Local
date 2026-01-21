# Mini Pipeline Local - Sales Data Analytics

## Descripción del Proyecto

Pipeline de datos batch local que demuestra capacidades fundamentales de **Analytics Engineer**, simulando un flujo de ingeniería de datos productivo sin dependencias de cloud ni herramientas avanzadas.

Este proyecto implementa el ciclo completo de un pipeline de datos:
```
CSV crudo → Python (limpieza) → PostgreSQL (almacenamiento) → SQL (análisis)
```

---

## DataSet
link: "https://www.kaggle.com/datasets/kyanyoga/sample-sales-data"

## Problema de Negocio

**Situación:** Una empresa retail necesita analizar sus transacciones de ventas históricas (2003-2005) para tomar decisiones informadas sobre:
- Tendencias de ventas mensuales
- Productos y clientes más rentables
- Distribución geográfica del revenue
- Crecimiento del negocio mes a mes

**Solución:** Pipeline automatizado que transforma datos crudos de ventas en métricas accionables mediante SQL analítico.

---

## Arquitectura del Pipeline

### Flujo de Datos

```
┌─────────────────┐
│  CSV File       │  ← Datos crudos (sales_data_sample.csv)
│  2,823 records  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  ingest.py      │  ← Limpieza y transformación
│  - Parsing      │     • Fechas inconsistentes
│  - Validación   │     • Tipos de datos
│  - Deduplicación│     • Valores nulos
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL 17  │  ← Almacenamiento persistente
│  raw_sales      │     • Tabla normalizada
│  table          │     • Índices optimizados
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  queries.sql    │  ← Análisis de negocio
│  7 analytical   │     • Métricas clave
│  queries        │     • Window functions
└─────────────────┘     • CTEs
```

---

## Stack Tecnológico

| Componente | Tecnología | Justificación |
|------------|------------|---------------|
| **Lenguaje de ingesta** | Python 3.x | Manejo eficiente de CSV y transformaciones |
| **Librerías Python** | pandas, psycopg2, python-dotenv | Estándar de la industria para data engineering |
| **Base de datos** | PostgreSQL 17 | Motor SQL robusto con soporte para análisis avanzado |
| **Control de versiones** | Git | Seguimiento de cambios y colaboración |
| **Formato de código** | .py scripts (no notebooks) | Producción-ready, ejecutable desde terminal |

---

## Estructura del Proyecto

```
mini_pipeline_local/
├── schema.sql                  # Definición de tabla y índices
├── ingest.py                   # Script de ingesta de datos
├── queries.sql                 # 7 queries analíticas
├── .env                        # Credenciales de DB (NO en Git)
├── .gitignore                  # Archivos excluidos de Git
├── README.md                   # Este archivo
└── sales_data_sample.csv       # Dataset fuente (Kaggle)
```

---

## Decisiones Técnicas

### 1. **Clave Primaria Simple vs Compuesta**

**Decisión:** `id SERIAL PRIMARY KEY` + `UNIQUE (order_number, order_line_number)`

**Justificación:**
- Simplicidad en la ingesta (auto-incremental)
- Previene duplicados mediante constraint
- Facilita queries posteriores

**Alternativa descartada:** `PRIMARY KEY (order_number, order_line_number)` - más compleja para un proyecto inicial

---

### 2. **Batch Insertion con `execute_batch`**

**Decisión:** Usar `execute_batch()` en lugar de insertar fila por fila

**Justificación:**
- **Eficiencia:** ~10x más rápido que inserts individuales
- **Manejo de red:** Reduce round-trips a la base de datos
- **Escalabilidad:** Preparado para datasets más grandes

```python
execute_batch(cursor, insert_query, data_tuples, page_size=100)
```

---

### 3. **Idempotencia del Pipeline**

**Decisión:** `ON CONFLICT (order_number, order_line_number) DO NOTHING`

**Justificación:**
- Permite re-ejecuciones sin errores
- Evita duplicados en caso de fallos parciales
- Simplifica testing y desarrollo

---

### 4. **Manejo de Fechas Inconsistentes**

**Decisión:** Parseo múltiple de formatos + fallback a parseo automático

**Problema detectado:** Dataset tiene fechas en formatos mixtos:
- `2/24/2003 0:00`
- `2/24/2003`
- Valores numéricos inválidos

**Solución implementada:**
```python
date_formats = ['%m/%d/%Y %H:%M', '%m/%d/%Y', '%Y-%m-%d']
# Try each format, fallback to auto-parsing
```

---

### 5. **Separación de Responsabilidades**

**Decisión:** Una función por responsabilidad

**Estructura del código:**
- `clean_sales_data()` → Solo limpieza
- `connect_to_database()` → Solo conexión
- `insert_data()` → Solo inserción
- `get_table_stats()` → Solo validación

**Beneficio:** Código testeable, mantenible y fácil de extender

---

### 6. **Índices Estratégicos**

**Decisión:** 4 índices en columnas críticas para queries analíticas

```sql
CREATE INDEX idx_raw_sales_order_date ON raw_sales(order_date);
CREATE INDEX idx_raw_sales_product_line ON raw_sales(product_line);
CREATE INDEX idx_raw_sales_country ON raw_sales(country);
CREATE INDEX idx_raw_sales_status ON raw_sales(status);
```

**Justificación:**
- `order_date` → Queries de rango temporal (Query 1, 5)
- `product_line` → Agregaciones por producto (Query 2, 7)
- `country` → Análisis geográfico (Query 4)
- `status` → Filtros por estado de orden

**Trade-off aceptado:** Ligero overhead en inserción a cambio de queries 5-10x más rápidas

---

### 7. **Credenciales en `.env`**

**Decisión:** Variables de entorno en lugar de hardcoded

**Justificación:**
- **Seguridad:** No expone credenciales en Git
- **Flexibilidad:** Fácil de cambiar sin modificar código
- **Buena práctica:** Estándar en la industria (12-factor app)

---

## Instrucciones de Ejecución

### **Prerequisitos**

- Python 3.8+
- PostgreSQL 17 (o superior)
- pip (gestor de paquetes Python)

---

### **Paso 1: Clonar el Repositorio**

```bash
git clone <tu-repositorio>
cd mini_pipeline_local
```

---

### **Paso 2: Instalar Dependencias**

```bash
pip install pandas psycopg2-binary python-dotenv
```

---

### **Paso 3: Configurar Base de Datos**

```bash
# Conectar a PostgreSQL
psql -U postgres

# Crear base de datos
CREATE DATABASE sales_pipeline;

# Salir
\q
```

---

### **Paso 4: Crear Tabla**

```bash
psql -U postgres -d sales_pipeline -f schema.sql
```

**Validación:**
```sql
\c sales_pipeline
\d raw_sales
-- Debes ver la tabla con 14 columnas
```

---

### **Paso 5: Configurar Credenciales**

Editar el archivo `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sales_pipeline
DB_USER=postgres
DB_PASSWORD=tu_password_aqui
```

---

### **Paso 6: Descargar Dataset**

1. Ir a: https://www.kaggle.com/datasets/kyanyoga/sample-sales-data
2. Descargar `sales_data_sample.csv`
3. Colocar en la raíz del proyecto

---

### **Paso 7: Ejecutar Pipeline de Ingesta**

```bash
python ingest.py
```

**Salida esperada:**
```
======================================================================
SALES DATA INGESTION PIPELINE
======================================================================
Start time: 2026-01-21 15:30:00

Found CSV file: sales_data_sample.csv

Reading CSV file...
   Loaded 2823 rows, 25 columns

Starting data cleaning...
   Selected 12 columns
   Renamed columns to snake_case
   Parsed dates successfully
   Handled missing values
   Converted and validated data types
   Removed duplicates

Cleaning Summary:
   Initial rows: 2823
   Final rows: 2823
   Rows removed: 0 (0.0%)

Connecting to PostgreSQL...
   Connected successfully

Inserting 2823 rows into database...
   Insertion completed successfully
   Total rows in table: 2823

Database Statistics:
   Total records: 2823
   Date range: 2003-01-06 to 2005-05-31
   Total sales: $10,032,628.85
   Unique customers: 92
   Product lines: 7

======================================================================
✅ PIPELINE COMPLETED SUCCESSFULLY
End time: 2026-01-21 15:30:15
======================================================================
```

---

### **Paso 8: Ejecutar Queries Analíticas**

```bash
# Ejecutar todas las queries
psql -U postgres -d sales_pipeline -f queries.sql

# O ejecutar queries individuales desde psql
psql -U postgres -d sales_pipeline
\i queries.sql
```

## 📊 Queries Disponibles

| Query | Descripción | Técnicas SQL |
|-------|-------------|--------------|
| **Query 1** | Monthly Sales Trend | `DATE_TRUNC`, `GROUP BY` |
| **Query 2** | Top 10 Best-Selling Products | `GROUP BY`, `ORDER BY`, `LIMIT` |
| **Query 3** | Top 10 Customers by Revenue | `COUNT DISTINCT`, métricas derivadas |
| **Query 4** | Sales by Country | Subconsultas, porcentajes |
| **Query 5** | Month-over-Month Growth | `CTE`, `LAG window function` |
| **Query 6** | Deal Size Analysis | Distribución porcentual |
| **Query 7** | Product Line Performance | `CTE`, `RANK window function` |

---

## Resultados y Métricas

### **Cobertura del Dataset**
- **Registros procesados:** 2,823 transacciones
- **Período analizado:** Enero 2003 - Mayo 2005 (29 meses)
- **Revenue total:** $10,032,628.85
- **Clientes únicos:** 92
- **Líneas de producto:** 7
- **Países:** 19

### **Calidad de Datos**
- **Duplicados eliminados:** 0 (dataset limpio)
- **Fechas inválidas:** 0 (parseo exitoso)
- **Valores nulos críticos:** 0 (validación pasada)

---


## Mejoras Futuras

### **Versión 2.0 - Cloud Migration**
- [ ] Migrar a AWS RDS (PostgreSQL)
- [ ] Almacenar CSV en S3
- [ ] Implementar Lambda para ingesta serverless
- [ ] Agregar CloudWatch para monitoreo

### **Versión 2.0 - Orchestration**
- [ ] Implementar Apache Airflow
- [ ] Crear DAGs para scheduling automático
- [ ] Agregar alertas de fallos
- [ ] Implementar retry logic

### **Versión 2.0 - Data Transformation**
- [ ] Integrar dbt para transformaciones
- [ ] Crear modelos staging → intermediate → marts
- [ ] Implementar data quality tests
- [ ] Versionado de transformaciones

### **Versión 2.0 - Analytics**
- [ ] Crear dashboard en Power BI / Tableau
- [ ] Implementar métricas de ML (churn prediction)
- [ ] Agregar análisis de cohorts
- [ ] Forecasting de ventas

### **Versión 2.0 - Data Quality**
- [ ] Agregar Great Expectations para validaciones
- [ ] Implementar data lineage tracking
- [ ] Crear data catalog
- [ ] Alertas de anomalías

### **Optimizaciones Inmediatas**
- [ ] Agregar logging estructurado (JSON)
- [ ] Implementar unit tests (pytest)
- [ ] Crear CI/CD pipeline (GitHub Actions)
- [ ] Agregar particionamiento por fecha en PostgreSQL
- [ ] Implementar incremental loads (solo nuevos datos)

---

## Contribuciones
Este es un proyecto educativo. Sugerencias y mejoras son bienvenidas vía Pull Requests.

## Autor - Byron Yaguar
 
**Analytics Engineer Portfolio Project**  
Proyecto desarrollado para demostrar capacidades fundamentales de ingeniería de datos y análisis.

-