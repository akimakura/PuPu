#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8001}"
TENANT="${TENANT:-tenant1}"
MODEL="${MODEL:-ror_dev_lt}"
REQUESTS="${REQUESTS:-100}"
CURL_MAX_TIME="${CURL_MAX_TIME:-180}"
AUTH_HEADER="${AUTH_HEADER:-}"
API_KEY_HEADER="${API_KEY_HEADER:-}"

COMMON_HEADERS=(-H "Accept: application/json")
if [[ -n "$AUTH_HEADER" ]]; then
  COMMON_HEADERS+=(-H "$AUTH_HEADER")
fi
if [[ -n "$API_KEY_HEADER" ]]; then
  COMMON_HEADERS+=(-H "$API_KEY_HEADER")
fi

request_url() {
  printf "%s/api/v0/tenants/%s/models/%s/dataStorages/" \
    "$BASE_URL" "$TENANT" "$MODEL"
}

check_base_url() {
  local probe_url="${BASE_URL%/}/openapi.json"
  local status

  status="$(
    curl -sS --connect-timeout 3 --max-time 5 \
      -o /dev/null \
      -w "%{http_code}" \
      "$probe_url" 2>/dev/null || true
  )"

  if [[ "$status" == "000" || -z "$status" ]]; then
    echo "Service is unreachable from this shell: $BASE_URL" >&2
    echo "If the app runs on another host or in Windows/WSL, pass BASE_URL explicitly." >&2
    echo "Example: BASE_URL='http://<host>:8001' bash scripts/test_singleflight.sh" >&2
    exit 1
  fi
}

run_request() {
  local index="$1"
  local url="$2"
  local out_dir="$3"
  local headers_file="$out_dir/$index.headers"
  local body_file="$out_dir/$index.body"
  local meta_file="$out_dir/$index.meta"
  local err_file="$out_dir/$index.err"
  local status

  if ! status="$(
    curl -sS --max-time "$CURL_MAX_TIME" \
      "${COMMON_HEADERS[@]}" \
      -D "$headers_file" \
      -o "$body_file" \
      -w "%{http_code}" \
      "$url" 2>"$err_file"
  )"; then
    printf "%s|CURL_ERROR|NONE\n" "$index" > "$meta_file"
    return 0
  fi

  local cache_status
  cache_status="$(
    awk -F': ' 'tolower($1)=="x-fastapi-cache"{gsub("\r","",$2); print $2}' "$headers_file" | tail -n 1
  )"
  cache_status="${cache_status:-NONE}"

  printf "%s|%s|%s\n" "$index" "$status" "$cache_status" > "$meta_file"
}

summarize_burst() {
  local name="$1"
  local out_dir="$2"
  shopt -s nullglob
  local meta_files=("$out_dir"/*.meta)
  shopt -u nullglob

  echo
  echo "=== $name ==="
  if [[ ${#meta_files[@]} -eq 0 ]]; then
    echo "No responses were collected."
    return 1
  fi
  echo "Statuses:"
  cut -d'|' -f2 "${meta_files[@]}" | sort | uniq -c | sort -nr
  echo "X-FastAPI-Cache:"
  cut -d'|' -f3 "${meta_files[@]}" | sort | uniq -c | sort -nr
  echo "Sample responses:"
  for index in 1 2 3; do
    if [[ -f "$out_dir/$index.meta" ]]; then
      echo "--- request $index ---"
      cat "$out_dir/$index.meta"
      cat "$out_dir/$index.body"
      echo
    fi
  done
}

run_burst() {
  local name="$1"
  local url="$2"
  local out_dir
  out_dir="$(mktemp -d)"

  echo
  echo "Running $name"
  echo "URL: $url"
  echo "Requests: $REQUESTS"

  for index in $(seq 1 "$REQUESTS"); do
    run_request "$index" "$url" "$out_dir" &
  done
  wait

  summarize_burst "$name" "$out_dir"
}

show_single_request() {
  local name="$1"
  local url="$2"
  local headers_file
  local body_file
  local status
  local cache_status
  headers_file="$(mktemp)"
  body_file="$(mktemp)"

  echo
  echo "=== $name ==="
  status="$(
    curl -sS --max-time "$CURL_MAX_TIME" \
      "${COMMON_HEADERS[@]}" \
      -D "$headers_file" \
      -o "$body_file" \
      -w "%{http_code}" \
      "$url"
  )"
  cache_status="$(
    awk -F': ' 'tolower($1)=="x-fastapi-cache"{gsub("\r","",$2); print $2}' "$headers_file" | tail -n 1
  )"
  cache_status="${cache_status:-NONE}"

  echo "status=$status"
  echo "x-fastapi-cache=$cache_status"
  cat "$body_file"
  echo

  rm -f "$headers_file" "$body_file"
}

echo "Assumption: приложение запущено в одном worker-процессе."
echo "Для takeover-сценария SINGLEFLIGHT_WAIT_TIMEOUT должен быть меньше sleep в get_data_storage_list_by_model_name."
check_base_url

takeover_url="$(request_url)"
run_burst "Scenario 1: timeout takeover + eventual success" "$takeover_url"
show_single_request "Scenario 1: post-warm cache check" "$takeover_url"
