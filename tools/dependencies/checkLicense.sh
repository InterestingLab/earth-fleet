#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

if [ -d "/tmp/seatunnel-dependencies" ]; then
  rm -rf /tmp/seatunnel-dependencies/*
fi

./mvnw clean -pl '!seatunnel-connectors-v2-dist,!seatunnel-dist,!seatunnel-connectors-v2/connector-fake'  --no-snapshot-updates dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/tmp/seatunnel-dependencies

# List all modules(jars) that belong to the SeaTunnel itself, these will be ignored when checking the dependency
ls /tmp/seatunnel-dependencies | sort > all-dependencies.txt

echo "start"

# licenses
echo '=== Self modules: ' && ./mvnw --batch-mode --quiet -Dexec.executable='echo' -Dexec.args='${project.artifactId}-${project.version}.jar' exec:exec | tee self-modules.txt

# Exclude all self modules(jars) to generate all third-party dependencies
echo '=== Third party dependencies: ' && grep -vf self-modules.txt all-dependencies.txt | sort | uniq | tee third-party-dependencies.txt

# 1. Compare the third-party dependencies with known dependencies, expect that all third-party dependencies are KNOWN
# and the exit code of the command is 0, otherwise we should add its license to LICENSE file and add the dependency to
# known-dependencies.txt. 2. Unify the `sort` behaviour: here we'll sort them again in case that the behaviour of `sort`
# command in target OS is different from what we used to sort the file `known-dependencies.txt`, i.e. "sort the two file
# using the same command (and default arguments)"

diff -w -B -U0 <(sort < tools/dependencies/known-dependencies.txt) <(sort < third-party-dependencies.txt)
