#!/bin/bash
image=gcr.io/grotz-pso-team/scraper:15
docker buildx build --push -t ${image} .


