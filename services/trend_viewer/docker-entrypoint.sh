#!/bin/bash

SCRIPT=$1

if [ "$SCRIPT" = "test" ]; then
  echo "running python unittests (additional args passed to unittest)"
  python3 -m unittest discover "${@:2}"
else
  echo "starting trend viewer web application"
  streamlit run /usr/src/app/timpani/trend_viewer/trend_viewer_app.py --browser.gatherUsageStats false
fi
