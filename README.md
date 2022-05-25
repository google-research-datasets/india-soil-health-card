# AnthroKrishi SHC Scraper

## Setup Workload Identity
kubectl create serviceaccount scraper \
    --namespace default

gcloud iam service-accounts add-iam-policy-binding anthro-run@grotz-pso-team.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:grotz-pso-team.svc.id.goog[default/scraper]"

kubectl annotate serviceaccount scraper \
    --namespace default \
    iam.gke.io/gcp-service-account=anthro-run@grotz-pso-team.iam.gserviceaccount.com

TODO
Somethings wrong with the permissions why did I have to assign roles/owner to the gsa

