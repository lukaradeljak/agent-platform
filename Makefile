.PHONY: deploy logs restart-scheduler shell-collector shell-db migrate ps stop

# Levantar todos los servicios (rebuild si hay cambios)
deploy:
	docker compose up -d --build

# Ver logs en tiempo real de todos los servicios
logs:
	docker compose logs -f

# Ver logs de un servicio específico (uso: make logs-worker)
logs-%:
	docker compose logs -f $*

# Reconstruir y reiniciar solo el scheduler y los workers
restart-scheduler:
	docker compose up -d --build celery_beat celery_worker

# Abrir bash dentro del container del collector
shell-collector:
	docker compose exec collector bash

# Abrir psql dentro del container de postgres
shell-db:
	docker compose exec postgres psql -U $${POSTGRES_USER} -d $${POSTGRES_DB}

# Correr migraciones de Alembic
migrate:
	docker compose run --rm collector alembic upgrade head

# Ver el estado de todos los containers
ps:
	docker compose ps

# Detener todos los servicios
stop:
	docker compose down

# Detener y eliminar volúmenes (CUIDADO: borra la DB)
clean:
	docker compose down -v
