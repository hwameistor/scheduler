[DEPRECATED]

The code has been move to https://github.com/hwameistor/hwameistor.

# HwameiStor Scheduler

## Introduction

The Scheduler is one of important components of the HwameiStor. It is to automatically schedule the Pod to the correct node which has the associated HwameiStor volume. With the scheduler, the Pod doesn't have to has the NodeAffinity or NodeSelector field to select the node. A scheduler will work for both LVM and Disk volumes.

### Installation

The Scheduler should be deployed with the HA mode in the cluster, which is a best practice for the production.

### Deploy by Helm Chart

Scheduler must work with Local Storage and Local Disk Manager. It's suggested to install by [helm-charts](https://github.com/hwameistor/helm-charts/blob/main/README.md)

### Deploy by YAML (For Developing)

```shell
    # kubectl apply -f deploy/scheduler.yaml
```

## Feedbacks

Please submit any feedback and issue at: [Issues](https://github.com/hwameistor/scheduler/issues)
