#!/usr/bin/env bash

sbt coverage test coverageReport

bash <(curl -s https://codecov.io/bash)

