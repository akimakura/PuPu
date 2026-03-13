#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8001}"
TENANT="${TENANT:-tenant1}"
MODEL="${MODEL:-ror_dev_lt}"
REQUESTS="${REQUESTS:-100}"
CURL_MAX_TIME="${CURL_MAX_TIME:-180}"
TAKEOVER_SLEEP_SECONDS="${TAKEOVER_SLEEP_SECONDS:-20}"
FAIL_FIRST_OWNERS="${FAIL_FIRST_OWNERS:-4}"
ERROR_TTL="${ERROR_TTL:-10}"
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
  local test_key="$1"
  local sleep_seconds="$2"
  local fail_first_owners="$3"
  printf "%s/api/v0/tenants/%s/models/%s/composites/singleflight-debug?testKey=%s&sleepSeconds=%s&failFirstOwners=%s" \
    "$BASE_URL" "$TENANT" "$MODEL" "$test_key" "$sleep_seconds" "$fail_first_owners"
}

run_request() {
  local index="$1"
  local url="$2"
  local out_dir="$3"
  local headers_file="$out_dir/$index.headers"
  local body_file="$out_dir/$index.body"
  local meta_file="$out_dir/$index.meta"
  local status

  status="$(
    curl -sS --max-time "$CURL_MAX_TIME" \
      "${COMMON_HEADERS[@]}" \
      -D "$headers_file" \
      -o "$body_file" \
      -w "%{http_code}" \
      "$url"
  )"

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

  echo
  echo "=== $name ==="
  echo "Statuses:"
  cut -d'|' -f2 "$out_dir"/*.meta | sort | uniq -c | sort -nr
  echo "X-FastAPI-Cache:"
  cut -d'|' -f3 "$out_dir"/*.meta | sort | uniq -c | sort -nr
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
echo "Для takeover-сценария SINGLEFLIGHT_WAIT_TIMEOUT должен быть меньше TAKEOVER_SLEEP_SECONDS."

takeover_key="takeover-$(date +%s)"
takeover_url="$(request_url "$takeover_key" "$TAKEOVER_SLEEP_SECONDS" "0")"
run_burst "Scenario 1: timeout takeover + eventual success" "$takeover_url"
show_single_request "Scenario 1: post-warm cache check" "$takeover_url"

error_key="error-open-$(date +%s)"
error_url="$(request_url "$error_key" "0" "$FAIL_FIRST_OWNERS")"
run_burst "Scenario 2: owner failures open error-state" "$error_url"
show_single_request "Scenario 2: immediate cached error check" "$error_url"

echo
echo "Sleeping for error TTL: $((ERROR_TTL + 1))s"
sleep "$((ERROR_TTL + 1))"

show_single_request "Scenario 2: recovery request after error TTL" "$error_url"
show_single_request "Scenario 2: cached success after recovery" "$error_url"
