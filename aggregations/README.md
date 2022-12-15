## Aggregating test data
This python script can be used to calculate boostrapped averages aggregated over districts, villages and also aggregate on the basis of irrigation method and soil type of the farms.   
Since the aggregation task is highly parallelizable, the script can be run on a GKE cluster on multiple pods. Different task indexes perform calculations for different aggregations and parameters.


## Getting started
You will need to build the docker container and push it to Google Cloud Container Registry. You can use `docker buildx build --push -t gcr.io/$GOOGLE_CLOUD_PROJECT/data_commons:1 ./container` to build and push the container.


## Deploying the Scraper to Kubernetes
You can easily run the Scraper container on Kubernetes, we provided an example Kubernetes configuration for that. If you want to run multiple instances in parallel, please configure the `TASK_INDEX` and `TASK_COUNT` environment variables accordingly.   

The k8s_aggregate file also contains helper scripts that allow you to easily generate Kubernetes deployment descriptors for multiple instances of the Pod.


Once you have created the yaml files, these pods can then be easily deployed by running the following command:
```
kubectl apply -f <filename>.yaml
```