# Deploying DataFlow Studio

DataFlow Studio ships as a small set of framework-dependent .NET 10 container images — the always-on
**Api** plus the pipeline **jobs** (seed, curation, warehouse-sink) and the **migrations** (OltpDb,
StarRocks, ClickHouse). This directory packages them three ways: a single parameterized `Dockerfile`,
a `docker-compose.yml` for a laptop, and `k8s/` manifests for a cluster.

> The data platform it talks to — SQL Server AG, Kafka, Schema Registry, StarRocks, ClickHouse, the OTLP
> collector, Marquez — is **not** packaged here. It is the NexusPlatform lab (or your own cluster). Point
> the `DFS_*` variables at it (see [`.env.example`](.env.example)).

## Images

All images build from one [`Dockerfile`](Dockerfile), selected by build args (`PROJECT`, `ENTRY_DLL`):
a multi-stage build (SDK → publish → aspnet runtime), **non-root**, framework-dependent. The `Nexus.*`
packages restore from GitHub Packages, so the build takes a `read:packages` token as a **BuildKit secret**
(never baked into a layer).

```bash
# One image, by hand:
DOCKER_BUILDKIT=1 GITHUB_PACKAGES_TOKEN=$(gh auth token) docker build \
  --secret id=github_packages_token,env=GITHUB_PACKAGES_TOKEN \
  --build-arg PROJECT=src/DataFlowStudio.Api/DataFlowStudio.Api.csproj \
  --build-arg ENTRY_DLL=DataFlowStudio.Api.dll \
  -f deploy/Dockerfile -t dfs-api:latest .
```

## docker-compose (laptop)

```bash
cp deploy/.env.example deploy/.env            # then fill in the DFS_* values
mkdir -p deploy/secrets                        # drop the Vault-issued Kafka mTLS PEMs here (gitignored)

# Build every image:
DOCKER_BUILDKIT=1 GITHUB_PACKAGES_TOKEN=$(gh auth token) \
  docker compose -f deploy/docker-compose.yml build

# Run the Api (default profile):
docker compose -f deploy/docker-compose.yml up dfs-api      # -> http://localhost:8080/health

# Run a pipeline job on demand (mirrors the Aspire explicit-start ordering):
docker compose -f deploy/docker-compose.yml --profile jobs run --rm dfs-seed
docker compose -f deploy/docker-compose.yml --profile jobs run --rm dfs-curation
docker compose -f deploy/docker-compose.yml --profile jobs run --rm dfs-warehouse-sink

# Migrate a sink:
docker compose -f deploy/docker-compose.yml --profile migrate run --rm migrate-starrocks
```

## Kubernetes

```bash
# Build + push the images to your registry first (retag dfs-*:latest to <registry>/dfs-*:tag).
# Create the real Secret out-of-band (Vault Secrets Operator / CSI in the lab) — do NOT commit it:
kubectl create namespace dataflow-studio
kubectl -n dataflow-studio create secret generic dfs-credentials \
  --from-literal=DFS_SQL_CONN='...' \
  --from-literal=DFS_STARROCKS_CONNECTION='...' \
  --from-literal=DFS_CLICKHOUSE_CONNECTION='...' \
  --from-file=kafka-ca.pem=./deploy/secrets/kafka-ca.pem \
  --from-file=kafka-cert.pem=./deploy/secrets/kafka-cert.pem \
  --from-file=kafka-key.pem=./deploy/secrets/kafka-key.pem

# Apply the always-on tier (namespace, config, Api Deployment + Service):
kubectl apply -k deploy/k8s

# Run the jobs on demand:
kubectl apply -f deploy/k8s/job-migrate-starrocks.yaml
kubectl apply -f deploy/k8s/job-curation.yaml
```

`k8s/` layout:

| File | What |
|---|---|
| `namespace.yaml` | the `dataflow-studio` namespace |
| `configmap.yaml` | non-secret endpoints (Kafka bootstrap, SR/OTLP/Marquez URLs, PEM paths) |
| `secret.example.yaml` | the **shape** of `dfs-credentials` (connection strings + Kafka mTLS PEMs) — example only |
| `api-deployment.yaml` | the Api `Deployment` (non-root, read-only rootfs, `/health` probes) + `Service` |
| `job-curation.yaml` | a one-shot curation drain `Job` |
| `job-migrate-starrocks.yaml` | a one-shot StarRocks migration `Job` |
| `kustomization.yaml` | `kubectl apply -k` for the always-on tier |

Every workload runs non-root with `allowPrivilegeEscalation: false`, a read-only root filesystem, and all
capabilities dropped; the Api has liveness + readiness probes on `/health`.
