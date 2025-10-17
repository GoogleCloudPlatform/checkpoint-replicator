# High Scale Checkpointing Replicator

This repository contains the source code for High Scale Checkpointing Replicator for ML training jobs.

## Deployment

There is a helper shell script to build and publish Docker image:

`deploy\docker-build.sh <REGISTRY_PATH> [<IMAGE_NAME>]`

## Directory Organization

* `deploy/`: Docker building
* `src/replicator`: Replicator source code

## Documentation

Checkpoint Replicator is designed to be hosted in multiple runtime environments.

For using it on Google Kubernetes Engine (GKE) you don't have to build/deploy it yourself as fully-managed GKE addon is available. See the following docs (MTC stands for Multi-Tier Checkpointing):

* [MTC BLOG post](https://cloud.google.com/blog/products/ai-machine-learning/using-multi-tier-checkpointing-for-large-ai-training-jobs) 

* [MTC User Guide](https://cloud.google.com/kubernetes-engine/docs/how-to/machine-learning/training/multi-tier-checkpointing)

* [MaxText MTC documentation](https://maxtext.readthedocs.io/en/latest/guides/checkpointing_solutions/multi_tier_checkpointing.html) (for training on TPUs)

* [NeMo MTC recipe](https://github.com/AI-Hypercomputer/gpu-recipes/blob/main/training/a3ultra/llama3-1-405b/nemo-pretraining-gke-resiliency/goodput-guide.md#2-multi-tier-checkpointing-strategy-leveraging-gcs-with-fuse) (for training on GPUs)

Fully-managed [GKE hosting controller](https://github.com/GoogleCloudPlatform/high-scale-checkpointing-controller) is Open-Source as well.
