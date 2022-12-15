num_tasks = 109

for i in range(108, num_tasks):
  f = open(f'data_commons_{i}.yaml', 'w')
  f.write(f"""apiVersion: v1
kind: Pod
metadata:
  name: data-commons-{i}
spec:
  restartPolicy: Never
  containers:
  - name: data-commons
    image: gcr.io/soil-health-card-india/data_commons:4
    resources:
      requests:
        cpu: "250m"
        memory: "0.5Gi"
    env:
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
