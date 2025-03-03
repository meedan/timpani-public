#!/bin/bash

SCRIPT=$1

echo "docker entrypoint checking raw_store environment setup for timpani conductor..."
# make sure that appropriate S3 buckets exist
python3 -m timpani.raw_store.raw_store_setup 
echo "checking content_store database setup..."
# create the database and admin users and (if not in live or qa) apply migrations
python3 -m timpani.content_store.content_store_manager setup

if [ "$SCRIPT" = "process" ]; then
  echo "running processing command"  
  python3 -m timpani.conductor.process "${@:2}"
elif [ "$SCRIPT" = "test" ]; then
  echo "running python unittests (additional args passed to unittest)"
  python3 -m unittest discover "${@:2}"
elif [ "$SCRIPT" = "content_store" ]; then
  echo "running content_store database migration scripts"
  python3 -m timpani.content_store.content_store_manager "${@:2}"
elif [ "$SCRIPT" = "benchmark" ]; then
  echo "running conductor benchmark script"
  python3 -m timpani.conductor.benchmark "${@:2}"
else
  echo "starting conductor web api"
  python3 -m timpani.conductor.app
fi
