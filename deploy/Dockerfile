# syntax=docker/dockerfile:1
#
# One parameterized multi-stage build for every runnable DataFlow Studio host. Each image is selected by
# two build args — PROJECT (the .csproj to publish) and ENTRY_DLL (its published entry assembly) — so the
# Api and the pipeline consoles all share one reviewed, non-root, framework-dependent image recipe.
# docker-compose.yml passes the args per service.
#
# The nexus-shared (Nexus.*) packages restore from GitHub Packages, so the build needs a read:packages
# token. It is passed as a BuildKit secret (never baked into a layer):
#   DOCKER_BUILDKIT=1 docker build --secret id=github_packages_token,env=GITHUB_PACKAGES_TOKEN \
#     --build-arg PROJECT=src/DataFlowStudio.Api/DataFlowStudio.Api.csproj \
#     --build-arg ENTRY_DLL=DataFlowStudio.Api.dll -f deploy/Dockerfile -t dfs-api .

ARG DOTNET_VERSION=10.0

# ---- build ----
FROM mcr.microsoft.com/dotnet/sdk:${DOTNET_VERSION} AS build
ARG PROJECT
ARG CONFIGURATION=Release
WORKDIR /src

# Copy the manifests first so the restore layer caches until a dependency actually changes.
# .editorconfig carries the analyzer configuration (e.g. CA1716 severity) — without it a newer base-image
# SDK would apply stricter defaults and fail the warnings-as-errors build.
COPY Directory.Build.props Directory.Packages.props nuget.config global.json .editorconfig ./
COPY src/ src/

# The GitHub Packages feed credential is consumed only for this RUN and never persisted in a layer.
RUN --mount=type=secret,id=github_packages_token \
    GITHUB_PACKAGES_TOKEN="$(cat /run/secrets/github_packages_token 2>/dev/null || true)" \
    dotnet publish "${PROJECT}" -c "${CONFIGURATION}" -o /app /p:UseAppHost=false

# ---- runtime ----
# aspnet carries the ASP.NET Core shared framework (a superset of the base runtime), so the Api and the
# plain-console workers both run on one image.
FROM mcr.microsoft.com/dotnet/aspnet:${DOTNET_VERSION} AS runtime
ARG ENTRY_DLL
WORKDIR /app
COPY --from=build /app ./

# The .NET runtime image ships a non-root user 'app' (its numeric uid is $APP_UID); run as it so K8s
# runAsNonRoot is satisfiable. The published files are world-readable, so no chown is needed.
USER $APP_UID

ENV DFS_ENTRY_DLL=${ENTRY_DLL} \
    DOTNET_EnableDiagnostics=0
# The Api listens on 8080 (compose/K8s map it); consoles ignore the port.
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080

ENTRYPOINT ["sh", "-c", "exec dotnet \"$DFS_ENTRY_DLL\""]
