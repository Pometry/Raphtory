# Docker/Scala Experiments

>**Project Note:**
>This project relies on some very recent upgrades to sbt-native-packager.  This functionality has not been published to a repo yet, so this project will download and build the master branch of sbt-native-packager until such time as the 1.1 release is published.

### Akka Cluster Application
This is the main event!  The goal is to be able to run Akka servers inside Docker containers and then connect them with Akka clustering.

The basic problem is that Docker is designed to encapsulate the network.  See diagram below:

![Akka Picture](akkadoc.jpg)

Referencing the image, Akka's IP addresses/ports inside a Docker container are internal-only, and useless outside the container.  That's a big problem since Akka's protocol depends on a free flow of communication between nodes.

Akka 2.4+ supports a bind-hostname/bind-port, meaning Akka can now bind to a second set of IP/port address.  In our case we'll use that new facility to bind to both the internal Docker IP (which we basically don't care about unless we're running >1 node inside the container) and the external host's IP.

### Basic Setup

For our cluster application we'll use 4 nodes:

- REST node (1)
- Service/logic nodes (2 - load balanced)
- Seed node (1)

A REST service request comes in over HTTP to the rest node, which will then in turn communicate with a logic node to do "work", then reply to the request.

Each node will be in a separate Docker container.

> **Note:** 
> In production it is envisioned you'd run more than one seed node, but this is just an example application.

### Running Locally (no AWS)

Docker-ized Akka nodes will need two pieces of information from the host machine running the container: the host's IP and port.   This is information that Docker is actually tring to abstract/hide from you so you need to pass it in when you run the container.

Here's how you'd start each of the nodes.  Note the argument passed to determine node type (seed, logic, or rest) and the fact we're passing in the seed node's IP.

>**Seed Node**
>docker run -p 9101:2551 --rm -e "HOST_IP=172.25.40.108" -e "HOST_PORT=9101" dockerexp/cluster seed

>**REST Node**
>docker run -p 9102:2551 -p 8080:8080 --rm -e "HOST_IP=172.25.40.108" -e "HOST_PORT=9102" dockerexp/cluster rest 172.25.40.108:9101

>**Logic Nodes**

>docker run -p 9103:2551 --rm -e "HOST_IP=172.25.40.108" -e "HOST_PORT=9103" dockerexp/cluster logic 172.25.40.108:9101

>docker run -p 9104:2551 --rm -e "HOST_IP=172.25.40.108" -e "HOST_PORT=9104" dockerexp/cluster logic 172.25.40.108:9101

In this example 172.25.401.108 is the IP of the host running the Docker containers (all on 1 box in my example, but it doesn't have to be).  Akka's default port 2551 is mapped to some port on the host with the -p Docker argument.  Both the host's IP and the port is passed into the Docker container as environment variables (-e argument).  Finally the seed IP:port is passed as an argument to the server code (172.25.40.108:9101).

>**Note:**
>On a Mac if you're running VirtualBox with Boot2Docker or similar you're going to have to make sure you've opened all the ports you're using in the example above or nothing will happen.  Open VirtualBox and go to Settings > Network > Port Forwarding.  If you're running natively on Linux you can ignore this step. 

###Running on AWS

In an AWS setting we want to make running the Docker container w/Akka as seamless as possible, so we need a way to get the host's IP and port accessible inside the docker, preferably without passing this information in (although this will work).

Fortunately AWS has a magic URL, that when called will provide the host's IP, even from inside a running Docker image.  Greatness. 

>http://169.254.169.254/latest/meta-data/local-ipv4

So what about the port?  This is trickier.  We'd like to support container runners like Amazon's ECS, which means we don't assign the ports--the runner does.  We may even have multiples of the same image running on the same host (on different ports).  It's up to the runner.

Docker must know the port mappings but its trying to hide this information.  Fortunately they do expose this knowledge on a Unix socket.  Using a simple modification you can map the socket to a RESTful HTTP endpoint:

>Add the following to /etc/sysconfig/docker
>OPTIONS="-H 0.0.0.0:5555 -H unix:///var/run/docker.sock"

Now all kinds of very useful Docker-related information is available on port 5555 inside your container.

>**Note:**
>It might be advisable to create an AMI with the previous modification to /etc/sysconfig/docker and save it so you can easily spin up AWS instances pre-configured to handle this technique.

The IpAndPort class in this project is designed to see if you pass in the IP/port and 1) use that info if so, or 2) inspect AWS+Docker if not.

Start the servers on AWS with the socket mapped to port 5555 as shown above.  Note the less cumbersome footprint.

>**Seed Node**
>docker run -p 9101:2551 --rm dockerexp/cluster seed

>**REST Node**
>docker run -p 9102:2551 -p 8080:8080 --rm dockerexp/cluster rest 172.25.40.108:9101

>**Logic Nodes**

>docker run -p 9103:2551 --rm dockerexp/cluster logic 172.25.40.108:9101

>docker run -p 9104:2551 --rm dockerexp/cluster logic 172.25.40.108:9101

This will start containers manually.  In a real-world example we would want to hand-start seed node(s) on known instances (IP:port).  You could then start REST + Logic nodes using ECS.  In ECS you can't assign the external ports (like 9103 above).  That will be inspected with the Docker ":5555" socket we exposed.  You'd just tell ECS you need to map internal port 2551 to something and it does the rest.

###Using

After all the setup to get here, using the service is a little anti-climatic!  Three APIs are exposed (of course use your REST node's IP):

```
curl 172.25.40.108:8080/ping
curl 172.25.40.108:8080/nodes
curl 172.25.40.108:8080/svc
```

The first is a simple response to see that the node is up and responding.  The second should show you a list of node IPs that were discovered via Akka's cluster facility.  (There should be 4 entries in this list per this example.)  The final call goes through the REST server and hits a clustered logic node then replies with the UUID of the server that did "work".  Hit it a few times and this should pop back 'n forth between two UUIDs, showing both logic nodes in the cluster are being hit.
