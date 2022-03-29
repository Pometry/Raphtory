# Pulsar

## Script options
- You can replace /usr/local/bin with the install path you would like.
- You can replace 2.9.0 with another pulsar version if you require another version.
- If you are missing dependencies, the script will report what is missing
- If you wish to know more about the script options run `./bin/pulsar-local`

<br>

## Running Pulsar locally using java process
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Install binaries | `./bin/pulsar-local -d machine -a install -v 2.9.0 -p /usr/local/bin`|
|Install pulsar connectors | `./bin/pulsar-local -d machine -a install-connectors -v 2.9.0 -p /usr/local/bin`|
|Start pulsar | `./bin/pulsar-local -d machine -a start -v 2.9.0 -p /usr/local/bin`|
|Stop pulsar | `./bin/pulsar-local -d machine -a stop -v 2.9.0`|

<br>

## Running Pulsar locally using docker
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Start pulsar docker container |`./bin/pulsar-local -d docker -a start -v 2.9.0`|
|Stop pulsar docker container |`./bin/pulsar-local -d docker -a stop -v 2.9.0`|
|Get pulsar docker container logs | `./bin/pulsar-local -d docker -a logs -v 2.9.0`|

<br>

## Running Pulsar locally using minikube (must have existing minikube cluster)
|   |   |
|---|---|
|Checkout the Raphtory code base | `git clone URL && cd Raphtory`|
|Get kube context of your existing cluster | `kubectl config get-contexts -o name`|
|To start pulsar on minikube | `./bin/pulsar-local -d minikube -a start -v 2.9.0 -m <minikube_context_name>`|
|To stop running pulsar deployment on minikube | `./bin/pulsar-local -d minikube -a start -v 2.9.0 -m <minikube_context_name>`|

<br>