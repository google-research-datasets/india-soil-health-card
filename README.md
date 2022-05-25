# AnthroKrishi SHC Scraper
This repository contains a simple Python based scraper that, will download metadata and Soil Health Cards from https://soilhealth.dac.gov.in and stores them in Google Cloud Storage (GCS).

In order to operate the scraper needs access to a GCS Bucket and a Google Cloud Spanner database.

# Getting started
You can create the needed infrastructure for this scraper using the Terraform scripts in the infrastructure folder. It will create a GCS Bucket, a Spanner Instance and Database and a GKE cluster.

You need to run `terraform init -backend-config="<bucket_name>"` with the name of the bucket where you want to store the terraform state. Afterwards you can deploy the infrastructure using `terraform apply`.

Afterwards you will need to build the docker container and push it to Google Cloud Container Registry. You can use `docker buildx build --push -t gcr.io/$GOOGLE_CLOUD_PROJECT/scraper:1 ./container` to build and push the container.

## Deploying the Scraper to Kubernetes
You can easily run the Scraper container on Kubernetes, we provided an example Kubernetes configuration for that. If you want to run multiple instances in parallel, please configure the `TASK_INDEX` and `TASK_COUNT` environment variables accordingly.
```
apiVersion: v1
kind: Pod
metadata:
  name: scrape-cards
spec:
  restartPolicy: Always
  containers:
  - name: scraper
    image: <image_url>
    resources:
      requests:
        cpu: "1000m"
        memory: "2Gi"
    env:
      - name: MODE
        value: <SCRAPE|CARDS|INGEST>
      - name: TASK_INDEX
        value: "0"
      - name: TASK_COUNT
        value: "1"
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
```

The k8s folder also contains helper scripts that allow you to easily generate Kubernetes deployment descriptors for multiple instances of the Pod. In our experience this is not necessary for the metadata scraping.

