# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

for i in range(0,80):
    print(f"""apiVersion: v1
kind: Pod
metadata:
  name: scrape-cards-{i}
spec:
  restartPolicy: Always
  containers:
  - name: scraper
    image: <docker_image>
    resources:
      requests:
        cpu: "1000m"
        memory: "2Gi"
    env:
      - name: MODE
        value: SCRAPE
      - name: TASK_INDEX
        value: "{i}"
      - name: TASK_COUNT
        value: "80"
      - name: GCS_BUCKET
        value: <GCS_BUCKET_NAME>
      - name: SPANNER_INSTANCE_ID
        value: <SPANNER_INSTANCE_ID>
      - name: SPANNER_DATABASE_ID
        value: <SPANNER_DATABASE_ID>
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        add:
        - SYS_ADMIN
      readOnlyRootFilesystem: true
      runAsNonRoot: true
      runAsUser: 1000
    volumeMounts:
    - mountPath: /tmp
      name: tmp
    - mountPath: /.local
      name: local
  volumes:
  - emptyDir:
    name: tmp
  - emptyDir:
    name: local
---""")