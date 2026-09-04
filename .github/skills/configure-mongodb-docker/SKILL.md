---
name: configure-mongodb-docker
description: 'Configure MongoDB in Docker or Docker Compose. Use when adding a MongoDB container, creating or updating docker-compose.yml, configuring credentials, persistent volumes, healthchecks, application connection strings, backups, or troubleshooting MongoDB connectivity.'
argument-hint: 'Describe the application, environment, database name, and whether Docker Compose already exists.'
user-invocable: true
---

# Configure MongoDB in Docker

Configure a repeatable, persistent MongoDB development or deployment setup and prove that the database is reachable by the application.

## When to Use

Use this skill when the user asks to:

- Run MongoDB in Docker or Docker Compose.
- Add MongoDB to an existing application stack.
- Configure MongoDB credentials, database names, ports, volumes, or networks.
- Diagnose connection failures between an application and MongoDB in containers.
- Add a MongoDB healthcheck, initialization script, backup command, or local connection instructions.

## Required Inputs

Identify these values before editing files. Ask only for values that cannot be inferred from the repository:

- Application runtime and how it is started.
- Whether a Compose file already exists and which services use it.
- MongoDB image/version policy. Prefer the repository's existing version; otherwise use a current stable major version explicitly pinned, not `latest`.
- Database name, application username, and database authentication requirements.
- Whether the application runs inside Docker or directly on the host.
- Development-only or production-like requirements.

Do not request or write plaintext production passwords. Use environment variables or a secret manager and document the variable names.

## Procedure

### 1. Inspect the repository

1. Search for `docker-compose.yml`, `compose.yml`, Dockerfiles, `.env*`, MongoDB drivers, connection strings, and existing service names.
2. Read the application's configuration and startup path to determine the expected connection variable.
3. Check `.gitignore` before creating `.env`; ensure local credentials are ignored.
4. Preserve existing Compose services, networks, volumes, naming conventions, and version pins.

Decision points:

- If Compose exists, add MongoDB to the existing file and reuse its network where appropriate.
- If no Compose file exists, create a minimal `compose.yml` at the repository root.
- If the application runs on the host, expose a loopback port such as `127.0.0.1:27017:27017`.
- If the application runs in Compose, do not use `localhost` in its connection string; use the MongoDB service name, normally `mongodb`.

### 2. Define configuration

Create or update a local `.env` from a non-secret example such as `.env.example`:

```dotenv
MONGO_ROOT_USERNAME=admin
MONGO_ROOT_PASSWORD=change-me-locally
MONGO_DATABASE=app
MONGO_PORT=27017
MONGO_URI=mongodb://admin:change-me-locally@localhost:27017/app?authSource=admin
```

Use a URL-encoded password when constructing a URI if it contains reserved characters such as `@`, `:`, `/`, `?`, `#`, or `%`. Do not duplicate credentials in Compose, application code, or documentation when an environment variable can be used.

For an application container, the URI should use the service name:

```text
mongodb://admin:${MONGO_ROOT_PASSWORD}@mongodb:27017/${MONGO_DATABASE}?authSource=admin
```

### 3. Add the MongoDB service

Use a pinned image, a named volume, a restart policy appropriate to the environment, and a healthcheck. A minimal Compose service is:

```yaml
services:
  mongodb:
    image: mongo:8
    restart: unless-stopped
    environment:
      MONGO_INITDB_ROOT_USERNAME: ${MONGO_ROOT_USERNAME}
      MONGO_INITDB_ROOT_PASSWORD: ${MONGO_ROOT_PASSWORD}
      MONGO_INITDB_DATABASE: ${MONGO_DATABASE}
    ports:
      - "127.0.0.1:${MONGO_PORT:-27017}:27017"
    volumes:
      - mongodb_data:/data/db
    healthcheck:
      test: ["CMD-SHELL", "mongosh --quiet --eval 'db.adminCommand({ ping: 1 }).ok' localhost:27017/test --username \"$${MONGO_INITDB_ROOT_USERNAME}\" --password \"$${MONGO_INITDB_ROOT_PASSWORD}\" --authenticationDatabase admin | grep 1"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s

volumes:
  mongodb_data:
```

Adapt the image major version to the repository's compatibility requirements. Do not expose MongoDB publicly by binding to `0.0.0.0` unless the user explicitly requires it and network access is protected.

If an application service depends on MongoDB, add a health-gated dependency only when the Compose implementation supports it and it matches the existing style:

```yaml
depends_on:
  mongodb:
    condition: service_healthy
```

### 4. Add initialization only when needed

The root user created by `MONGO_INITDB_ROOT_USERNAME` and `MONGO_INITDB_ROOT_PASSWORD` is for administration. If the application needs a least-privilege user, add an idempotent initialization script under a mounted `/docker-entrypoint-initdb.d/` directory.

Remember that MongoDB initialization scripts run only when the data directory is empty. State this clearly before asking the user to remove a volume. Never delete a volume without explicit approval because that destroys database data.

### 5. Start and validate

Run these checks from the directory containing the Compose file:

```powershell
docker compose config
docker compose up -d mongodb
docker compose ps
docker compose logs --no-color mongodb
```

The validation must confirm:

1. Compose configuration renders successfully.
2. The MongoDB container is running and becomes `healthy`.
3. The named volume exists and is mounted.
4. An authenticated ping succeeds.
5. The application can connect using the correct hostname for its execution context.

For a host-based connection, use a short `mongosh` check when available:

```powershell
mongosh "$env:MONGO_URI" --eval "db.adminCommand({ ping: 1 })"
```

When `mongosh` is not installed on the host, run it inside the container:

```powershell
docker compose exec mongodb mongosh --quiet --eval "db.adminCommand({ ping: 1 })" --username "$env:MONGO_ROOT_USERNAME" --password "$env:MONGO_ROOT_PASSWORD" --authenticationDatabase admin
```

Use the repository's tests or a minimal application health check to validate the actual driver connection. Do not treat `docker compose ps` alone as proof that the application can authenticate.

### 6. Document operation and recovery

Document the following near the Compose file or in the repository's setup documentation:

- Start, stop, status, and log commands.
- Host and container connection strings.
- Credential variable names, without real secret values.
- Volume name and the fact that `down -v` deletes data.
- How to create a backup before destructive maintenance.

Provide a logical backup example:

```powershell
New-Item -ItemType Directory -Force backups | Out-Null
docker compose exec mongodb mongodump --username "$env:MONGO_ROOT_USERNAME" --password "$env:MONGO_ROOT_PASSWORD" --authenticationDatabase admin --out /tmp/backup
docker compose cp mongodb:/tmp/backup ./backups/mongodb
```

For production, require an external backup policy, restricted network access, secret management, monitoring, and a tested restore procedure. A local Compose volume is not a production backup strategy.

## Completion Criteria

The task is complete only when:

- The Compose file passes `docker compose config`.
- MongoDB uses an explicit image version and a persistent named volume.
- Credentials are supplied through environment variables or secrets and are not committed.
- The healthcheck uses authenticated MongoDB access and reports healthy.
- The connection string matches whether the application runs on the host or in Docker.
- The application or a driver-level check successfully authenticates.
- Documentation includes operational commands and warns about destructive volume removal.
- Existing services and unrelated repository changes remain untouched.

## Troubleshooting

- `ECONNREFUSED`: check that the container is running, the port mapping matches the URI, and the application is not using `localhost` from inside another container.
- Authentication failure: verify `authSource=admin`, credentials, and whether an old volume was initialized with different credentials.
- Healthcheck unhealthy: inspect `docker compose logs mongodb`; test the same `mongosh` command manually inside the container.
- Initialization changes have no effect: the named volume already contains data. Back up first, then remove the volume only with explicit approval.
- Port already in use: choose another host port and update only host-side connection strings; container-to-container traffic should continue using `mongodb:27017`.
