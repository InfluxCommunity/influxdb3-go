#!/usr/bin/env bash
#
# The MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

DEFAULT_INFLUXDB_V3_VERSION="3-core"
INFLUXDB_V3_VERSION="${INFLUXDB_V3_VERSION:-$DEFAULT_INFLUXDB_V3_VERSION}"
INFLUXDB_V3_IMAGE=influxdb:${INFLUXDB_V3_VERSION}

DEFAULT_INFLUXDB_DATABASE=my-db
INFLUXDB_DATABASE="${INFLUXDB_DATABASE:-$DEFAULT_INFLUXDB_DATABASE}"

INFLUXDB_NETWORK_SUBNET=192.170.0.0/24
INFLUXDB_NETWORK_GW=192.170.0.1

#
# Parse command line arguments
#
EXPORT_URL_ENV_VAR=""
EXPORT_DB_ENV_VAR=""
EXPORT_TOKEN_ENV_VAR=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --export-url-as)
      EXPORT_URL_ENV_VAR="$2"
      shift 2
      ;;
    --export-db-as)
      EXPORT_DB_ENV_VAR="$2"
      shift 2
      ;;
    --export-token-as)
      EXPORT_TOKEN_ENV_VAR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

#
# Check prerequisites
#
for cmd in docker wget; do
  command -v ${cmd} &>/dev/null || { echo "'${cmd}' is not installed"; exit 1; }
done

echo
echo "Cleanup"
echo
docker kill influxdb_v3 2>/dev/null || true
docker rm influxdb_v3 2>/dev/null || true
docker network rm influx_network 2>/dev/null || true

echo
echo "Create network"
echo
docker network create -d bridge influx_network --subnet $INFLUXDB_NETWORK_SUBNET --gateway $INFLUXDB_NETWORK_GW

echo
echo "Restarting InfluxDB 3.0 [${INFLUXDB_V3_IMAGE}] ... "
echo
docker pull "${INFLUXDB_V3_IMAGE}" || true
docker run \
       --detach \
       --name influxdb_v3 \
       --network influx_network \
       --publish 8181:8181 \
       --env LOG_FILTER=debug \
       "${INFLUXDB_V3_IMAGE}" \
       serve -v \
        --node-id node01 \
        --object-store file \
        --data-dir /var/lib/influxdb3/data

echo
echo "Wait to start InfluxDB 3.0"
echo
for i in {1..30}; do
  if curl -s -o /dev/null -w "%{http_code}" http://localhost:8181/ping | grep -q "401"; then
    break
  fi
  echo "Attempt $i/30: Waiting for InfluxDB to respond with 401..."
  sleep 1
done
echo "Done"

echo
echo "Create admin token"
echo
ADMIN_TOKEN=$(docker exec influxdb_v3 influxdb3 create token --admin | grep "Token:" | awk '{print $2}')
echo "ADMIN_TOKEN=$ADMIN_TOKEN"

echo
echo "Test the token"
echo
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer ${ADMIN_TOKEN}" http://localhost:8181/ping)
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "Token test failed with HTTP $HTTP_CODE"
  exit 1
fi
echo "Done"

echo
echo "Create database"
echo
docker exec influxdb_v3 influxdb3 create database --token "$ADMIN_TOKEN" "$INFLUXDB_DATABASE"

#
# Export results
#

export_var() {
  local var_name="$1" var_value="$2"
  [[ -n "$var_name" ]] || return
  [[ -n "$BASH_ENV" ]] || { echo "\$BASH_ENV not available (not in CircleCI), cannot export variables."; exit 1; }
  echo "Exporting $var_name=$var_value"
  echo "export $var_name=$var_value" >> "$BASH_ENV"
}

echo
export_var "$EXPORT_URL_ENV_VAR" "http://localhost:8181"
export_var "$EXPORT_DB_ENV_VAR" "$INFLUXDB_DATABASE"
export_var "$EXPORT_TOKEN_ENV_VAR" "$ADMIN_TOKEN"
