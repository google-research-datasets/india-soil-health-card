for i in range(0,80):
    print(f"""apiVersion: batch/v1
kind: Job
metadata:
  name: scrape-cards-{i}
spec:
  backoffLimit: 100
  template:
    spec:
      restartPolicy: OnFailure
      nodeSelector:
        cloud.google.com/gke-spot: "true"
      serviceAccountName: scraper
      containers:
      - name: scraper
        image: gcr.io/grotz-pso-team/scraper:15
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
            value: "40"
          - name: GCS_BUCKET
            value: grotz-anthrokrishi-shcs
          - name: BIGQUERY_DATASET
            value: soilhealth
          - name: SPANNER_INSTANCE_ID
            value: tfgen-spanid-20220516132152938
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
      - emptyDir: 
        name: tmp
      - emptyDir: 
        name: local
---""")