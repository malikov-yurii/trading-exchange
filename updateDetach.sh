#!/usr/bin/env bash

set -e

git pull origin dev

./build.sh

./startDetach.sh

