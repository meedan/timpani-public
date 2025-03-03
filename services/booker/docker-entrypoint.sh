#!/bin/bash


SCRIPT=$1

echo "docker entrypoint checking environment setup for booker..."
python3 -m timpani.util.aws_info
python3 -m timpani.raw_store.raw_store_setup

if [ "$SCRIPT" = "acquire" ]; then
  echo "calling scripts/acquire.py"
  python3 -m timpani.booker.acquire "${@:2}"  # pass all args except the first one which is the service name
elif [ "$SCRIPT" = "backfill" ]; then
  echo "calling scripts/backfill.py"
  python3 -m timpani.booker.backfill "${@:2}"  # pass all args except the first one which is the service name
elif [ "$SCRIPT" = "test" ]; then
  echo "running python unittests (additional args passed to unittest)"
  python3 -m unittest discover "${@:2}"
else
  echo "No script named: $SCRIPT container will be running for debuging"
  tail -f /dev/null
fi
