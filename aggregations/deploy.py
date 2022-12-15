import os

# os.system('docker buildx build --push -t gcr.io/soil-health-card-india/data_commons:4 ./aggregate')

for i in range(108, 109):
  os.system(f'kubectl apply -f data_commons_{i}.yaml')