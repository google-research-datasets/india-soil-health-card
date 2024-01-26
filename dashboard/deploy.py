import os

# os.system('docker buildx build --push -t gcr.io/soil-health-card-india/cron_job:2.2 ../dashboard')

os.system(f'kubectl apply -f cron_job.yaml')