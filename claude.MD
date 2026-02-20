# Agent Execution Platform

## Descripción del Proyecto

Plataforma self-hosted en Hetzner Linux para ejecutar y agendar agentes Python (LangGraph, etc.).
Los agentes se ejecutan según un schedule, recopilan métricas y las envían automáticamente
a una base de datos PostgreSQL compartida, de donde la aplicación web existente las lee
para mostrarlas a los clientes.

**Problema que resuelve:** Reemplaza un setup fallido con Coolify usando Docker Compose plano
para garantizar que la red interna entre servicios funcione correctamente.

---

## Arquitectura

```
Celery Beat (scheduler)
      |
      | [Redis - cola de tareas]
      v
Celery Worker (ejecuta agentes)
      |
      | [HTTP POST /metrics  →  http://collector:8000]
      v
FastAPI Collector (valida + guarda)
      |
      | [SQLAlchemy async]
      v
PostgreSQL (base de datos compartida)
      ^
      | [conexión read-only]
      |
Aplicación Web Existente (dashboard de clientes)
```

Todos los servicios corren en el mismo VPS Hetzner vía Docker Compose en la red `platform_net`.
Los servicios se descubren entre sí por nombre de servicio (e.g. `http://collector:8000`).

---

## Tech Stack

| Componente        | Tecnología            |
|-------------------|-----------------------|
| Scheduler         | Celery Beat + Redis   |
| Executor          | Celery Worker         |
| Metrics API       | FastAPI (async)       |
| Base de datos     | PostgreSQL 16         |
| ORM               | SQLAlchemy 2 (async)  |
| Migraciones       | Alembic               |
| Monitoreo jobs    | Flower                |
| Reverse proxy     | Nginx                 |
| Containers        | Docker Compose        |

---

## Estructura de Directorios

```
/opt/agent-platform/
├── docker-compose.yml
├── .env                        # Secrets — NO commitear
├── .env.example                # Template — SÍ commitear
├── CLAUDE.md
├── Makefile
│
├── collector/                  # FastAPI — recibe y guarda métricas
│   ├── Dockerfile
│   ├── main.py                 # Entrypoint FastAPI
│   ├── database.py             # Engine async + session factory
│   ├── requirements.txt
│   ├── routers/
│   │   ├── metrics.py          # POST /metrics  GET /metrics
│   │   └── health.py           # GET /health
│   └── models/
│       ├── metric.py           # Schemas Pydantic (request/response)
│       └── db.py               # Modelos ORM SQLAlchemy
│
├── scheduler/                  # Celery Beat + Worker
│   ├── Dockerfile
│   ├── celery_app.py           # App Celery + beat_schedule
│   ├── requirements.txt
│   └── tasks/
│       ├── __init__.py
│       └── runner.py           # Task genérico run_agent(agent_name)
│
├── agents/                     # Todos los agentes
│   ├── base_agent.py           # BaseAgent ABC — heredar siempre
│   ├── registry.py             # Mapeo: agent_name → clase
│   └── <nombre_agente>/
│       ├── __init__.py
│       ├── agent.py            # Implementa BaseAgent
│       └── requirements.txt    # Deps específicas del agente
│
├── migrations/                 # Alembic — cambios de schema
│   ├── env.py
│   ├── alembic.ini
│   └── versions/
│       └── 0001_initial_schema.py
│
└── nginx/
    └── nginx.conf
```

---

## Cómo Agregar un Agente Nuevo (3 Pasos)

### Paso 1 — Crear el agente

```python
# agents/mi_agente/agent.py
from agents.base_agent import BaseAgent
from typing import Any

class MiAgente(BaseAgent):
    name = "mi_agente"          # Debe ser único. Este string es el identificador.

    def run(self) -> dict[str, Any]:
        # ── Tu lógica aquí ──────────────────────────────
        resultado = hacer_algo()

        # Retorna un dict plano. Keys → metric_name, values → metric_value.
        return {
            "items_procesados": resultado.count,
            "errores": resultado.errors,
            "duracion_segundos": resultado.elapsed,
        }
```

### Paso 2 — Registrar el agente

```python
# agents/registry.py  — agregar la importación y la entrada
from agents.mi_agente.agent import MiAgente

AGENT_REGISTRY: dict[str, type] = {
    # ... agentes existentes ...
    "mi_agente": MiAgente,
}
```

### Paso 3 — Agendar la ejecución

```python
# scheduler/celery_app.py  — agregar una entrada a beat_schedule
app.conf.beat_schedule = {
    # ... entradas existentes ...
    "ejecutar-mi-agente-cada-hora": {
        "task": "tasks.runner.run_agent",
        "schedule": crontab(minute=0),      # Top de cada hora
        "args": ("mi_agente",),
    },
}
```

Luego reconstruir:
```bash
make restart-scheduler
```

---

## Contrato del API de Métricas

Los agentes NO necesitan llamar esto manualmente — `BaseAgent.execute()` lo hace automáticamente.
Para testing manual o integración externa:

```
POST http://collector:8000/metrics
Content-Type: application/json

{
    "agent_name":  "mi_agente",
    "metrics": {
        "items_procesados": 42,
        "errores": 0,
        "status": "ok"
    },
    "started_at":  "2026-02-19T10:00:00Z",
    "finished_at": "2026-02-19T10:00:45Z",
    "error":       null
}
```

Respuesta exitosa: `201 Created` con `{"run_id": <id>}`.

---

## Schema de Base de Datos

**`agent_runs`** — Un registro por ejecución de agente
| Columna        | Tipo          | Descripción                              |
|----------------|---------------|------------------------------------------|
| id             | BIGSERIAL PK  |                                          |
| agent_name     | VARCHAR(100)  | Nombre del agente                        |
| started_at     | TIMESTAMPTZ   | Inicio de ejecución                      |
| finished_at    | TIMESTAMPTZ   | Fin de ejecución                         |
| status         | VARCHAR(20)   | `running` / `success` / `failed`         |
| error_message  | TEXT          | Mensaje de error si falló                |

**`agent_metrics`** — Métricas individuales por ejecución (key/value)
| Columna        | Tipo          | Descripción                              |
|----------------|---------------|------------------------------------------|
| id             | BIGSERIAL PK  |                                          |
| run_id         | BIGINT FK     | → agent_runs(id)                         |
| agent_name     | VARCHAR(100)  | Denormalizado para queries rápidas       |
| metric_name    | VARCHAR(200)  | Nombre de la métrica                     |
| metric_value   | NUMERIC       | Valor numérico                           |
| metric_text    | TEXT          | Valor texto (para métricas no numéricas) |

**`agent_daily_summaries`** — Agregados diarios por agente (para performance del dashboard)

---

## Variables de Entorno (.env)

```bash
# PostgreSQL
POSTGRES_DB=agent_metrics_db
POSTGRES_USER=agent_platform
POSTGRES_PASSWORD=<contraseña_fuerte>

# Redis
REDIS_URL=redis://redis:6379/0

# Collector API
COLLECTOR_SECRET_KEY=<string_aleatorio_32_chars>

# Flower (UI de monitoreo de Celery)
FLOWER_USER=admin
FLOWER_PASSWORD=<contraseña_fuerte>

# Web app (usuario read-only de la DB)
WEBAPP_READONLY_PASSWORD=<contraseña_fuerte>

# Secrets de agentes (agregar según se necesiten)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

---

## Deploy en Hetzner (Primer Deploy)

```bash
# 1. SSH al VPS
ssh root@<hetzner-ip>

# 2. Instalar Docker
curl -fsSL https://get.docker.com | sh
apt-get install -y docker-compose-plugin

# 3. Clonar / transferir el proyecto
git clone <repo-url> /opt/agent-platform
cd /opt/agent-platform

# 4. Configurar variables de entorno
cp .env.example .env
nano .env   # Llenar todos los valores

# 5. Levantar infraestructura base primero
docker compose up -d postgres redis

# 6. Correr migraciones
docker compose run --rm collector alembic upgrade head

# 7. Crear usuario read-only para el web app
docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
  CREATE USER webapp_readonly WITH PASSWORD '${WEBAPP_READONLY_PASSWORD}';
  GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO webapp_readonly;
  GRANT USAGE ON SCHEMA public TO webapp_readonly;
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO webapp_readonly;
"

# 8. Levantar todos los servicios
docker compose up -d

# 9. Verificar que todo está healthy
docker compose ps
curl http://localhost:8000/health

# 10. Configurar firewall
ufw allow ssh
ufw allow 80
ufw allow 443
ufw allow from <IP_DEL_WEB_APP> to any port 5432
ufw enable
```

## Actualizar / Redesplegar

```bash
cd /opt/agent-platform
git pull
docker compose up -d --build
# Si hay cambios de schema:
docker compose run --rm collector alembic upgrade head
```

## Escalar Workers (30+ agentes)

```bash
# 5 workers paralelos × 4 concurrencia = 20 ejecuciones simultáneas
docker compose up -d --scale celery_worker=5
```

---

## Reglas Importantes

1. **Los agentes SIEMPRE heredan `BaseAgent`** — nunca escribir a la DB directamente desde código de agente.
2. **Solo el `collector` escribe a PostgreSQL** — todos los demás servicios son read-only o no acceden a la DB.
3. **El `name` del agente debe ser único** — coincide con la clave en `AGENT_REGISTRY` y en `beat_schedule`.
4. **Deps del agente van en `agents/<nombre>/requirements.txt`** — y deben agregarse al `Dockerfile` del scheduler.
5. **Cambios de schema → siempre via Alembic** — nunca `ALTER TABLE` manual.
6. **Todos los secrets en `.env`** — usar `os.getenv()` en el código, nunca hardcodear.
7. **Flower y el collector van detrás de Nginx** — nunca exponer en `0.0.0.0` sin autenticación.

---

## Comandos de Debugging

```bash
# Ver runs recientes y su estado
docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
    -c "SELECT agent_name, status, started_at, error_message FROM agent_runs ORDER BY started_at DESC LIMIT 20;"

# Ver métricas de un agente específico
docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
    -c "SELECT metric_name, metric_value, metric_text FROM agent_metrics WHERE agent_name='mi_agente' ORDER BY recorded_at DESC LIMIT 20;"

# Logs del worker en tiempo real
docker compose logs -f celery_worker

# Profundidad de la cola de Celery
docker compose exec redis redis-cli LLEN celery

# Push manual de métrica de prueba
curl -X POST http://localhost:8000/metrics \
    -H "Content-Type: application/json" \
    -d '{"agent_name":"test","metrics":{"x":1},"started_at":"2026-02-19T10:00:00Z","finished_at":"2026-02-19T10:00:01Z","error":null}'

# UI de monitoreo de Celery (jobs, schedules, workers)
# http://<vps-ip>:5555  (usuario/contraseña en .env)
```

---

## Makefile Reference

```
make deploy            → docker compose up -d --build (todos los servicios)
make logs              → docker compose logs -f
make restart-scheduler → rebuild + restart celery_beat y celery_worker
make shell-collector   → bash dentro del container del collector
make shell-db          → psql dentro de postgres
make migrate           → alembic upgrade head
make ps                → docker compose ps
```
