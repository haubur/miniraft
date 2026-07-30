#!/usr/bin/env bash
# Integration test for miniraft's http module.
#
# NOTE: These integration tests are dirt and superficial and not fit to verify
# any of the Raft specifics for which maelstrom is a better fit.
#
# Tests PUT (write, update) and GET requests and response correlation.
# Runs the requests against the leader, the followers and across leader and followers.

set -u

FAILED=""
BODY=/tmp/miniraft_e2e_body

# On unexpected hold kill the cluster: https://tldp.org/LDP/Bash-Beginners-Guide/html/sect_12_02.html
trap 'pkill -f "target/debug/main-http" >/dev/null 2>&1' EXIT

# Build the http binary
cargo build --bin main-http >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "build failed"
  exit 1
fi

# kill any cluster from previous runs
pkill -f "target/debug/main-http" >/dev/null 2>&1
sleep 1
rm -rf node
NODES=5001,5002,5003 ./target/debug/main-http >/tmp/miniraft_e2e.log 2>&1 &

# The metrics server also tracks the current role of each node.
LEADER=""
i=0
while [ $i -lt 30 ]; do
  curl -s http://127.0.0.1:16001/metrics 2>/dev/null | grep -q 'role="leader"} 1' && LEADER=5001 && break
  curl -s http://127.0.0.1:16002/metrics 2>/dev/null | grep -q 'role="leader"} 1' && LEADER=5002 && break
  curl -s http://127.0.0.1:16003/metrics 2>/dev/null | grep -q 'role="leader"} 1' && LEADER=5003 && break
  sleep 1
  i=$((i + 1))
done

if [ -z "$LEADER" ]; then
  echo "no leader elected"
  exit 1
fi

# Pick any node that is not the leader to test against a follower.
FOLLOWER=5001
[ "$LEADER" = "5001" ] && FOLLOWER=5002

# PUT (write): create a key on the leader.
# Should return 201 Created and empty body.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT --data 'lw1' http://127.0.0.1:$LEADER/key/lw)
if [ "$CODE" = "201" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:write_key_lw_value_lw1_to_leader,"; fi

# GET on the leader returns the value
# Should return 200 OK and body "lw1".
CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$LEADER/key/lw)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "lw1" ]; then :; else FAILED="$FAILED TEST_FAILED:get_lw1_from_leader_after_write"; fi

# PUT (write): create a key on the follower.
# Should return 201 Created and empty body.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT --data 'fw1' http://127.0.0.1:$FOLLOWER/key/fw)
if [ "$CODE" = "201" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:put_fw1_on_follower"; fi

# GET on the follower.
# Should return 200 OK and body "fw1".
CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$FOLLOWER/key/fw)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "fw1" ]; then :; else FAILED="$FAILED TEST_FAILED:get_fw1_from_follower_after_write"; fi

# PUT (write) on the leader, GET on the follower.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT --data 'cross-lr' http://127.0.0.1:$LEADER/key/x1)
if [ "$CODE" = "201" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:put_x1_on_leader"; fi

CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$FOLLOWER/key/x1)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "cross-lr" ]; then :; else FAILED="$FAILED TEST_FAILED:read_x1_from_follower_after_write_to_leader"; fi

# PUT (write) on the follower, GET on the leader.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT --data 'cross-rl' http://127.0.0.1:$FOLLOWER/key/x2)
if [ "$CODE" = "201" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:put_x2_on_follower"; fi

CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$LEADER/key/x2)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "cross-rl" ]; then :; else FAILED="$FAILED TEST_FAILED:read_x2_from_leader_after_write_to_follower"; fi

# PUT (update, If-Match) on the leader.
# Should return 200 OK and empty body.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT -H 'If-Match: lw1' --data 'lw2' http://127.0.0.1:$LEADER/key/lw)
if [ "$CODE" = "200" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:cas_update_on_leader"; fi

CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$LEADER/key/lw)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "lw2" ]; then :; else FAILED="$FAILED TEST_FAILED:get_cas_update_from_leader_after_write_to_leader,"; fi

# PUT (update, If-Match) on the follower.
# Should return 200 OK and empty body.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT -H 'If-Match: fw1' --data 'fw2' http://127.0.0.1:$FOLLOWER/key/fw)
if [ "$CODE" = "200" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:cas_update_on_follower"; fi

CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$FOLLOWER/key/fw)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "fw2" ]; then :; else FAILED="$FAILED TEST_FAILED:get_cas_update_from_follower_after_write_to_follower"; fi

# PUT (update, NO If-Match) on the leader.
# Should return 201 and empty.
# NOTE: The effect on the server is the same regardless of whether If-Match is present in the header.
# Since miniraft implements a HashMap as the state machine insert() is still an update in this case.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT --data 'over' http://127.0.0.1:$LEADER/key/x1)
if [ "$CODE" = "201" ] && [ ! -s "$BODY" ]; then :; else FAILED="$FAILED TEST_FAILED:overwrite_x1_on_leader"; fi

CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$LEADER/key/x1)
if [ "$CODE" = "200" ] && [ "$(cat "$BODY")" = "over" ]; then :; else FAILED="$FAILED TEST_FAILED:get_new_x1_value_from_leader"; fi

# PUT (update, wrong If-Match).
# Should return 400 Bad Request.
CODE=$(curl -s -o "$BODY" -w '%{http_code}' -X PUT -H 'If-Match: WRONG' --data 'nope' http://127.0.0.1:$LEADER/key/x2)
if [ "$CODE" = "400" ] && grep -q 'values for key differ' "$BODY"; then :; else FAILED="$FAILED TEST_FAILED:wrong_if_match_in_put"; fi

# GET a missing key.
# Should return 400 Bad Request with body "no such key".
CODE=$(curl -s -o "$BODY" -w '%{http_code}' http://127.0.0.1:$LEADER/key/doesnotexist)
if [ "$CODE" = "400" ] && [ "$(cat "$BODY")" = "no such key" ]; then :; else FAILED="$FAILED TEST_FAILED:trying_non_exisiting_key"; fi

# report
if [ -z "$FAILED" ]; then
  echo "SUCCESS!"
else
  for t in $FAILED; do
    echo "$t"
  done
  exit 1
fi
