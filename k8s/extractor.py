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

num_tasks = 20

for i in range(0, num_tasks):
  f = open(f'extract_cards1_{i}.yaml', 'w')
  f.write(f"""apiVersion: v1
kind: Pod
metadata:
  name: extract-cards1-{i}
spec:
  restartPolicy: Always
  containers:
  - name: extractor
    image: gcr.io/soil-health-card-india/extractor:11
    resources:
      requests:
        cpu: "250m"
        memory: "0.5Gi"
    env:
      - name: MODE
        value: EXTRACT
      - name: TASK_INDEX
        value: "{i}"
      - name: TASK_COUNT
        value: "{num_tasks}"
      - name: GCS_BUCKET
        value: anthrokrishi-shcs
      - name: SPANNER_INSTANCE_ID
        value: tfgen-spanid-20220525101635457
      - name: SPANNER_DATABASE_ID
        value: metadata
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
  - emptyDir: {{}}
    name: tmp
  - emptyDir: {{}}
    name: local
""")
  f.close()
