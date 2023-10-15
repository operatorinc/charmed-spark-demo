# Operator Day 2023 - Charmed Spark

This repository contains the material of the Demo held at Operator Day 2023 for Charmed Spark

## Playscript

The demo is broken into 3 stages:
1. Setup
2. Develop
3. Scale & Observe 
4. Cleanup

##### Prerequisite

* Ubuntu 22.04
* AWS Account with permissions for:
  * read/write on S3 buckets
  * creation of EKS clusters
* AWS cli and `eksctl` correctly configured
* AWS Route53 domain
* *(Optional)* `yq` and `docker` installed

#### Setup

First, we suggest to create some environment variables for storing S3 credentials and endpoints, such that
you can use the scripts below as-is

```shell
export AWS_S3_ENDPOINT=<s3-endpoint>
export AWS_S3_BUCKET=<bucket-name>
export AWS_ACCESS_KEY=<your-access-key>
export AWS_SECRET_KEY=<your-secret-key>
export AWS_ROUTE53_DOMAIN=<your-domain> 
```

To carry out this demo, we will need a few components that needs to be installed.

###### MicroK8s

```shell
sudo snap install microk8s --channel 1.27/stable --classic
sudo microk8s enable hostpath-storage dns rbac storage
sudo snap alias microk8s.kubectl kubectl 
microk8s config > ~/.kube/config
```

MicroK8s will be used for deploying Spark workload locally.

###### Juju CLI 

```shell
sudo snap install juju --channel 3.1/stable
```

The Juju CLI will be used for interacting with the Juju controller
for managing services via charmed operators.

<!-- 
###### `yq` 

```shell
sudo snap install yq
```

`yq` will be used to parse and patch YAML files.

###### `docker` 

```shell
sudo snap install docker
sudo addgroup --system docker
sudo adduser $USER docker
sudo snap disable docker
sudo snap enable docker
```

`yq` will be used to parse and patch YAML files.

-->

###### `spark-client`

```shell
sudo snap install spark-client --channel 3.4/edge
```

The `spark-client` Snap provides a number of utilities to manage Spark service accounts as well 
starting Spark job on a K8s cluster. 

##### Resources

Once all the components are installed, we then need to set up a S3 bucket and copy the relevant 
data from this repository in, e.g.`data` and `script`, that will be used in this demo.

In order to do so, you can use the Python scripts bundled in this repository for creating and 
setting up (e.g. copying the files needed for the demo) the S3 bucket

```shell
python scripts/spark_bucket.py \
  --action create setup \
  --access-key $AWS_ACCESS_KEY \
  --secret-key $AWS_SECRET_KEY \
  --endpoint $AWS_S3_ENDPOINT \
  --bucket $AWS_S3_BUCKET 
```

You can now create the Spark service account on the K8s cluster that will be used to run the 
Spark workloads. The services will be created via the `spark-client.service-account-registry`
as `spark-client` will provide enhanced features to run your Spark jobs seamlessly integrated 
with the other parts of the Charmed Spark solution. 

For instance, `spark-client` allows you to bind your service account a hierarchical set of 
configurations that will be used when submitting Spark jobs. For instance, in this demo we will 
use S3 bucket to fetch and store data. Spark settings are specified in a 
[configuration file](./confs/s3.conf) and can be fed into the service account creation command
 (that also handles the parsing of environment variables specified in the configuration file), e.g. 

```shell
spark-client.service-account-registry create \
  --username spark --namespace spark \
  --properties-file ./confs/s3.conf
```

You can find more information about the hierarchical set of configurations 
[here](https://discourse.charmhub.io/t/spark-client-snap-explanation-hierarchical-configuration-handling/8956) 
and how to manage Spark service account via `spark-client` 
[here](https://discourse.charmhub.io/t/spark-client-snap-tutorial-manage-spark-service-accounts/8952)

That's it! You are now ready to use Spark!

#### Develop

It is always very convenient when you are either exploring some data or doing some first development
to use Jupyter notebook, assisted with a user-friendly and interactive environment where you can 
mix python (together with plots) and markdown code.

To start a Jupyter notebook server that provides a binding to Spark already integrated in 
your notebooks, you can run the Charmed Spark OCI image

```shell
docker run --name charmed-spark --rm \
  -v $HOME/.kube/config:/var/lib/spark/.kube/config \
  -v $(pwd):/var/lib/spark/notebook/repo \
  -p 8080:8888 \
  ghcr.io/canonical/charmed-spark:3.4-22.04_edge \
  \; start jupyter 
```

It is important for the image to have access to the Kubeconfig file (in order to fetch the 
Spark configuration via the `spark-client` CLI) as well as the local notebooks directory to access 
to the notebook already provided. 

When the image is up and running, you can navigate with your browser to

```shell
http://localhost:8080
```

You can now either start a new notebook or use the one provided in `./notebooks/Demo.ipynb`.
As you start a new notebook, you will already have a `SparkContext` and a `SparkSession` object 
defined by two variables, `sc` and `spark` respectively,

```python
> sc
SparkContext

Spark UI

Version           v3.4.1
Master            k8s://https://192.168.1.4:16443
AppName           PySparkShell
```

In fact, the notebook (running locally on Docker) acts as driver, and it spawns executor pods on 
Kubernetes. This can be confirmed by running

```shell
kubectl get pod -n spark
```

which should output something like

```shell
NAME                                                        READY   STATUS      RESTARTS   AGE
pysparkshell-79b4df8ad74ab7da-exec-1                        1/1     Running     0          5m31s
pysparkshell-79b4df8ad74ab7da-exec-2                        1/1     Running     0          5m29s
```

Beside running Jupyter notebooks, the `spark-client` SNAP also allow you to submit Python 
scripts/job. In this case, we recommend you to run both driver and executor in kubernetes. 
Therefore, the python program needs to be uploaded to a location that can be reached by the pods, 
such that it can be downloaded by the driver to be executed. 

The setup of the S3 bucket above should have already uploaded the data and the script to 
`data/data.csv.gz` and `scripts/stock_country_report.py` respectively.

Therefore, you should be able to run

```shell
spark-client.spark-submit \
  --username spark --namespace spark \
  --deploy-mode cluster \
  s3a://$AWS_S3_BUCKET/scripts/stock_country_report.py \
    --input  s3a://$AWS_S3_BUCKET/data/data.csv.gz \
    --output s3a://$AWS_S3_BUCKET/data/output_report_microk8s
```

### Scale & Observe

It is now time to scale our jobs from our local environment to a proper production cluster running 
on AWS EKS.

First we need to create the AWS EKS cluster using `eksctl`

```shell
eksctl create cluster -f eks/cluster.yaml
```

*(This should take around 20 minutes)*

When the EKS cluster is ready, test that the pods are correctly up and running using the command
```shell
kubectl get pod -A
```
or any similar command of your choice. Note that if you are using the `kubectl` provided by 
MicroK8s, you should also point to the `.kube/config` file where the information necessary to 
authenticate to EKS are stored. 

> :warning: `eksctl` should have created a Kubeconfig file that uses `aws cli` to continuously 
> retrieve the user token. Unfortunately, the `aws cli` commands are not available to the snap 
> confined environment. We therefore suggest you to manually request a token and insert that 
> in the Kubeconfig file. This process is automated also by the bash script 
> `/bin/update_kubeconfig_token`

You can now choose which cluster to use via the `context` argument of the `spark-client` CLI, 
allowing you to seamlessly switch from the local to the production kubernetes.

#### Spark Observability stack

An important requirement of a production-ready system is to provide a way for the admins to monitor
and actively observe the operation of the cluster. Charmed Spark comes with an observability 
stack that provides:
1. a deployment of a Spark History Server, that is a UI that most Spark users are accustomed to, to 
   analyze their jobs, at a more business level (e.g. jobs separated by the different steps and so 
   on) 
2. a deployment of a COS-lite bundle with Grafana Dashboards, that is more oriented to cluster 
   administrators, allowing to set up alerts and dashboarding based on resource utilization, also 
   providing the ability to set up an alerting system

Logs of driver and executors are by default stored on the pod local file system, and are therefore
lost once the jobs finishes. However, Spark allows us to store these logs into S3, such that 
they can be re-read and visualized by the Spark History Server, allowing to monitor and visualize
the information and metrics about the job execution. Moreover, Spark driver and executors real-time
metrics can also be sent by the job to a Prometheus Pushgateway, where they are temporarily stored
to allow Prometheus to scrape them subsequently, and exposing them via Grafana. All these 
components will be deployed and managed by Juju. 

To enable monitoring, we should therefore
* Deploy and configure the Charmed Spark Observability stack using Juju
* Configure the Spark service account to provide configuration for Spark jobs to store logs in 
   a S3 bucket and push metrics to the Prometheus Pushgateway endpoint

##### Setup Juju 

These deployments, configuration and operations are managed through juju. Once the EKS cluster is 
up and running, you need to register the EKS K8s cluster in Juju with

```shell
juju add-k8s spark-cluster
```

You then need to bootstrap a Juju controller, that is the component of the Juju ecosystem 
responsible for coordinating operations and managing your services

```shell
juju bootstrap spark-cluster
```

Finally, you need to add a new model/namespace where to deploy Spark components/workloads 

```shell
juju add-model spark
```

##### Deploy the bundle

You can now deploy all the charms required by the Monitoring stack using the provided bundle 
(where the relevant configurations need to be replaced using the environment variable)

```shell
YQ_CMD=".applications.s3.options.bucket=strenv(AWS_S3_BUCKET)"\
"| .applications.s3.options.endpoint=strenv(AWS_S3_ENDPOINT)"\
"| .applications.traefik.options.external_hostname=strenv(AWS_ROUTE53_DOMAIN)"
juju deploy --trust <( yq e "$YQ_CMD" ./confs/spark-bundle.yaml )
```

###### Configure components

The credentials of AWS needs to be provided separately, as these may also be changed and rotated
regularly and they are not part of the static configuration of the Juju deployment. 
Once the charms are deployed, you therefore need to configurations the `s3` charm that 
holds the credentials of the S3 bucket. 

```shell
juju run s3/leader sync-s3-credentials \
  access-key=$AWS_ACCESS_KEY secret-key=$AWS_SECRET_KEY &>/dev/null
```

Once all the charms have settled into an `active/idle` states,  `history-server` can then
be related to the `s3` to provide the S3 credentials to the Spark History server to 
be able to read the logs from the object storage. 

```shell
juju relate history-server s3
```

and the `cos-configuration` charm can be related to `grafana` to provide some defaults 
dashboards

```shell
juju relate cos-configuration grafana
```

###### Expose Services externally

In order to expose the different UI of the Charmed Spark Monitoring stack using the Route53 domain, 
fetch the internal load balancer address

```shell
juju status --format yaml | yq .applications.traefik.address
```

and use this information to create a record in your domain, e.g. `spark.<domain-name>`.

Once the charms settle down into `active/idle` states, you can then fetch the external Spark 
History Server URL (alongside other components of the bundle) using `traefik` via the action

```shell
juju run traefik/leader show-proxied-endpoints
```

In your browser, you should now be able to access the Spark History Server UI. The Spark History 
Server does not support authentication just yet, and you should already be able to explore it

Similarly, you can also assess to the grafana dashboard:

```shell
http://<your-domain>/spark-grafana
```

However, Grafana needs authentication. The admin user credentials can be fetched using a dedicated
action 

```shell
juju run grafana/leader get-admin-password
```

The Charmed Spark Monitoring stack is now ready to be used!

#### Run Observed Spark Jobs

In order to run a Spark workload on the new cluster, you should first create your production 
Spark service account as well, e.g. 

```shell
spark-client.service-account-registry create \
  --context <eks-context-name> \
  --username spark --namespace spark \
  --properties-file ./confs/s3.conf
```

> Note that you can use the `--context` argument to specify which K8s cluster you want the 
> `spark-client` tool to use. If not specified, the `spark-client` will use the default one 
> provided in the Kubeconfig file. You can change the default context in the Kubeconfig file using 
> 
> `kubectl --kubeconfig path/to/kubeconfig config use-context <context-name>`
>

To enable Spark Jobs to be monitored, we need to further configure the Spark service account 
by enabling
* to store logs into a S3 bucket
* to push metrics the Prometheus Pushgateway endpoint

We need to fetch and store the address of the Prometheus Pushgateway endpoint in an 
environment variable
```shell
export PROMETHEUS_GATEWAY=$(juju status --format=yaml | yq ".applications.prometheus-pushgateway.address") 
export PROMETHEUS_PORT=9091
```

We can now add these configuration to the Spark service account with

```shell
spark-client.service-account-registry add-config \
  --username spark --namespace spark \
  --properties-file ./confs/spark-observability.conf
```

Once this is done, you are now all setup to run your job at scale

```shell
spark-client.spark-submit \
  --username spark --namespace spark \
  --conf spark.executor.instances=4 \
  --deploy-mode cluster \
  s3a://$AWS_S3_BUCKET/scripts/stock_country_report.py \
    --input  s3a://$AWS_S3_BUCKET/data/data.csv.gz \
    --output s3a://$AWS_S3_BUCKET/data/output_report_eks
```

Where we have also explicitly imposed to now run with 4 executors. Once this is done (or even 
during the execution) navigate to the Grafana and Spark History server dashboards to see, explore
and analyze the logs of the job. 

### Cleanup

First destroy the Juju model and controller

```shell
juju destroy-controller --force --no-wait \
  --destroy-all-models \
  --destroy-storage spark-cluster
```

You can then destroy the EKS cluster as well

```shell
eksctl delete cluster -f eks/cluster.yaml
```

Finally, you can also remove the S3-bucket that was used during the demo via the provided Python
script

```shell
python scripts/spark_bucket.py \
  --action delete \
  --access-key $AWS_ACCESS_KEY \
  --secret-key $AWS_SECRET_KEY \
  --endpoint $AWS_S3_ENDPOINT \
  --bucket $AWS_S3_BUCKET 
```