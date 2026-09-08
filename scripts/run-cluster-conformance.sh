#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

image="${MERIDIAN_POSTGRESQL_IMAGE:-postgis/postgis:16-3.4-alpine}"
primary_port="${MERIDIAN_POSTGRESQL_CLUSTER_PRIMARY_PORT:-}"
standby_one_port="${MERIDIAN_POSTGRESQL_CLUSTER_STANDBY_ONE_PORT:-}"
standby_two_port="${MERIDIAN_POSTGRESQL_CLUSTER_STANDBY_TWO_PORT:-}"
run_suffix="${GITHUB_RUN_ID:-local}-$$"
network="meridian-postgresql-cluster-${run_suffix}"
primary="meridian-postgresql-primary-${run_suffix}"
standby_one="meridian-postgresql-standby-one-${run_suffix}"
standby_two="meridian-postgresql-standby-two-${run_suffix}"
standby_one_volume="${standby_one}-data"
standby_two_volume="${standby_two}-data"

cleanup() {
  docker stop "${standby_two}" >/dev/null 2>&1 || true
  docker stop "${standby_one}" >/dev/null 2>&1 || true
  docker stop "${primary}" >/dev/null 2>&1 || true
  docker volume rm "${standby_two_volume}" >/dev/null 2>&1 || true
  docker volume rm "${standby_one_volume}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "${network}" >/dev/null
docker volume create "${standby_one_volume}" >/dev/null
docker volume create "${standby_two_volume}" >/dev/null
docker run --detach --rm \
  --name "${primary}" \
  --network "${network}" \
  --publish "127.0.0.1:${primary_port}:5432" \
  --env POSTGRES_USER=meridian \
  --env POSTGRES_PASSWORD=meridian \
  --env POSTGRES_DB=meridian \
  "${image}" \
  -c 'listen_addresses=*' \
  -c wal_level=replica \
  -c max_wal_senders=10 \
  -c max_replication_slots=10 \
  -c hot_standby=on >/dev/null

for _ in $(seq 1 60); do
  if docker exec "${primary}" pg_isready -h 127.0.0.1 -U meridian -d meridian >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${primary}" pg_isready -h 127.0.0.1 -U meridian -d meridian >/dev/null
primary_port="$(docker port "${primary}" 5432/tcp | head -n 1 | awk -F: '{print $NF}')"
docker exec "${primary}" sh -c \
  'echo "host replication meridian 0.0.0.0/0 scram-sha-256" >> "${PGDATA}/pg_hba.conf"'
docker exec "${primary}" psql -U meridian -d meridian -c "SELECT pg_reload_conf()" >/dev/null

bootstrap_standby() {
  local container="$1"
  local volume="$2"
  local slot="$3"
  local port="$4"
  docker run --rm \
    --user postgres \
    --network "${network}" \
    --env PGPASSWORD=meridian \
    --volume "${volume}:/var/lib/postgresql/data" \
    --entrypoint pg_basebackup \
    "${image}" \
    -h "${primary}" -U meridian -D /var/lib/postgresql/data \
    -Fp -Xs -R -C -S "${slot}" >/dev/null
  docker run --detach --rm \
    --name "${container}" \
    --network "${network}" \
    --publish "127.0.0.1:${port}:5432" \
    --env POSTGRES_USER=meridian \
    --env POSTGRES_PASSWORD=meridian \
    --env POSTGRES_DB=meridian \
    --volume "${volume}:/var/lib/postgresql/data" \
    "${image}" -c 'listen_addresses=*' -c hot_standby=on >/dev/null
}

bootstrap_standby "${standby_one}" "${standby_one_volume}" meridian_standby_one "${standby_one_port}"
bootstrap_standby "${standby_two}" "${standby_two_volume}" meridian_standby_two "${standby_two_port}"
standby_one_port="$(docker port "${standby_one}" 5432/tcp | head -n 1 | awk -F: '{print $NF}')"
standby_two_port="$(docker port "${standby_two}" 5432/tcp | head -n 1 | awk -F: '{print $NF}')"

for container in "${standby_one}" "${standby_two}"; do
  for _ in $(seq 1 60); do
    if docker exec "${container}" pg_isready -h 127.0.0.1 -U meridian -d meridian >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  docker exec "${container}" pg_isready -h 127.0.0.1 -U meridian -d meridian >/dev/null
done

for _ in $(seq 1 60); do
  replication_count="$({
    docker exec "${primary}" psql -U meridian -d meridian -Atc \
      "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'"
  } 2>/dev/null || true)"
  if [[ "${replication_count}" == "2" ]]; then
    break
  fi
  sleep 1
done
if [[ "${replication_count:-0}" != "2" ]]; then
  echo "expected two streaming PostgreSQL standbys, observed ${replication_count:-0}" >&2
  exit 1
fi

export MERIDIAN_POSTGRESQL_CLUSTER_DSN="postgresql://meridian:meridian@127.0.0.1:${primary_port}/meridian"
export MERIDIAN_POSTGRESQL_CLUSTER_STANDBY_DSNS="postgresql://meridian:meridian@127.0.0.1:${standby_one_port}/meridian,postgresql://meridian:meridian@127.0.0.1:${standby_two_port}/meridian"
# An explicit interpreter allows the exact installed release wheel to use the
# same cluster harness. Normal project CI retains its locked uv environment.
run_tests() {
  if [[ -n "${MERIDIAN_POSTGRESQL_PYTHON:-}" ]]; then
    "${MERIDIAN_POSTGRESQL_PYTHON}" -m pytest "$@"
  else
    uv run pytest "$@"
  fi
}
run_tests -m cluster --junitxml=cluster-tests.xml "$@"
export MERIDIAN_POSTGRESQL_TEST_DSN="${MERIDIAN_POSTGRESQL_CLUSTER_DSN}"
export MERIDIAN_POSTGRESQL_ENGINE_PROFILE=postgresql-postgis-cluster
run_tests -m integration --junitxml=cluster-integration.xml
if [[ -n "${MERIDIAN_POSTGRESQL_PYTHON:-}" ]]; then
  "${MERIDIAN_POSTGRESQL_PYTHON}" scripts/record_conformance.py --image "${image}" \
    --junit cluster-tests.xml --junit cluster-integration.xml --output cluster-provenance.json
else
  uv run python scripts/record_conformance.py --image "${image}" \
    --junit cluster-tests.xml --junit cluster-integration.xml --output cluster-provenance.json
fi
