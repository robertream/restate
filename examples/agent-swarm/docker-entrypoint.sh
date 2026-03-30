#!/bin/bash
set -e

echo "Starting Restate server..."
restate-server &
RESTATE_PID=$!

# Wait for Restate to be healthy
for i in $(seq 1 30); do
  if curl -s http://localhost:9070/health > /dev/null 2>&1; then
    echo "Restate ready"
    break
  fi
  sleep 1
done

echo "Starting agent services..."
npx tsx services/app.ts &
SERVICES_PID=$!
sleep 3

echo "Registering services..."
curl -s -X POST http://localhost:9070/deployments \
  -H 'content-type: application/json' \
  -d '{"uri": "http://localhost:9080"}' > /dev/null

echo "Starting frontend..."
npx vite --host 0.0.0.0 &
VITE_PID=$!

echo ""
echo "═══════════════════════════════════════════"
echo "  Agent Swarm Demo running!"
echo "  Frontend:    http://localhost:5173"
echo "  Restate API: http://localhost:8080"
echo "  Admin UI:    http://localhost:9070/ui/"
echo "═══════════════════════════════════════════"
echo ""

# Wait for any process to exit, then kill all
wait -n $RESTATE_PID $SERVICES_PID $VITE_PID
kill $RESTATE_PID $SERVICES_PID $VITE_PID 2>/dev/null
