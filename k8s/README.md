# Kubernetes (K8s) Guide for Bluesky Pipeline

Welcome to the Kubernetes deployment! If you are a junior infrastructure engineer or someone new to K8s, think of it as a massive, self-healing orchestration engine that replaces `docker-compose`. Instead of declaring containers in one big massive YAML file and tying them all to a single machine, K8s uses specific decoupled "resource objects" that describe the *desired state* of your infrastructure.

## Key Terminology and Concepts

1. **Cluster & Control Plane (Master Node)**:
   A K8s cluster is a set of worker machines (nodes) running containerized applications. The Control Plane acts as the brains of the cluster, deciding where to put containers, when to restart them, and how they communicate.

2. **Pod**:
   The smallest deployable unit in K8s. A pod is a wrapper around one or more tightly coupled containers (like Docker containers). If your container crashes, the Pod dies and K8s replaces it. You rarely deploy a naked Pod; you use higher-level controllers (like Deployments or StatefulSets) to manage them.

3. **Namespace (`00-namespace.yaml`)**:
   Think of this as a virtual fence inside the cluster. We created a namespace called `bluesky`. All our databases, networking, and containers live inside this fence so they don't accidentally collide with other applications running on the same hardware.

4. **ConfigMap (`01-configmaps.yaml`)**:
   In `docker-compose`, we used a `.env` file to pass strings into our code. In K8s, we store those non-secret environment variables in a `ConfigMap`. Any container in our namespace can safely read from it without hardcoding IP addresses into our code.

5. **Deployments (`03-nessie.yaml`, `07-firehose-consumer.yaml`, Flink JobManager)**:
   A Deployment manages **stateless** applications (apps that don't need to save data to a permanent hard drive). If your python firehose consumer crashes, the Deployment immediately notices the Pod is gone and automatically spins up a new pod to maintain your desired `replicas: 1` count.

6. **StatefulSets (`02-minio.yaml`, `04-redpanda.yaml`, `05-clickhouse.yaml`)**:
   These are similar to Deployments, but for **databases**. If a ClickHouse pod restarts, it shouldn't come back completely erased as a brand-new pristine container. A StatefulSet guarantees stable network IDs (e.g., `clickhouse-0`), predictable deployment ordering, and permanently attaches a Persistent Volume (virtual hard drive) to the pod so your data survives Pod restarts.

7. **PersistentVolumeClaim (PVC) (Found in StatefulSets)**:
   A request for storage. When our StatefulSet asks for `10Gi` of space via a PVC, K8s goes to the physical hardware and carves out a 10 Gigabyte chunk of disk (Persistent Volume) and permanently glues it to that specific database pod.

8. **Services (Found at the bottom of most manifests)**:
   In K8s, Pod internal IP addresses change randomly every time a container crashes and restarts. A `Service` solves this by giving your app a stable, static internal DNS name (like `http://redpanda:9092` or `http://minio:9000`) and a static IP. Our consumer app calls the Service URL, and the Service acts as an invisible traffic cop, routing the traffic to the actual living Pods.

9. **InitContainers (Found in `06-flink.yaml`)**:
   Special temporary containers that run *before* the main app starts up. We use them in Flink to download necessary Java JAR files (like the Iceberg connector) into a shared folder before the main Flink process is allowed to boot.

## Using `kind` (Kubernetes IN Docker) for Local Dev
`kind` is an incredible tool that runs a full Kubernetes control-plane *inside* Docker containers on your laptop. It is lightweight and perfect for safely testing K8s manifests before moving to real hardware (like K3s on your NAS).
- We defined `extraPortMappings` in `kind-config.yaml` so that the `kind` cluster exposes its internal K8s NodePorts to your Mac/PC's `localhost`. This lets you open `localhost:9000` in your web browser exactly like you did back in the Docker Compose days!

## How to Boot / Test
To spin everything up, simply execute the bootstrap script in the root directory:
```bash
./bootstrap.sh
```

**Here is exactly what that script automatically does for you:**
1. Spins up the `kind` cluster on your machine.
2. Runs `docker build` on the python consumer locally.
3. Automatically injects that built `firehose-consumer:latest` image directly into the `kind` cluster's cache (so K8s doesn't try pulling from the public internet).
4. Creates a `flink-config` ConfigMap out of the raw `.conf` text files inside `compose/config/flink/`.
5. Submits all the `k8s/*.yaml` blueprints to the Control Plane via `kubectl apply`.

As a beginner to infrastructure, this completely replicates the local behavior of `docker compose up -d`, but structurally equips you with a 1:1 replica of a real-world edge/production orchestration topology!
