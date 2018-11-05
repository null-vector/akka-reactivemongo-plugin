#!/usr/bin/env bash

sbt coverage test coverageReport coverageAggregate codacyCoverage
