# SERVER-ACCESS.md — the Hetzner box (where this service runs)

This service does **NOT** run on Railway. It runs as a Docker container on the shared MikeOS
self-hosted box, behind Caddy. This file is how you reach and deploy it.

## The box
| | |
|---|---|
| Provider | **Hetzner** dedicated (Falkenstein / FSN1), hostname `mikeos-osm` |
| CPU / RAM | AMD Ryzen 9 5950X (16c/32t) · 128 GB ECC · **no GPU** (software ffmpeg) |
| Disk | 2 × 3.84 TB NVMe **RAID1** → 3.5 TB usable at `/`; app data on **`/data`** (~2.4 TB free) |
| IPv4 / IPv6 | `144.76.45.114` / `2a01:4f8:191:51aa::2` |

## SSH (key auth, root)
```bash
ssh -i ~/.ssh/mikeos_osm -o UserKnownHostsFile=~/.ssh/known_hosts_osm \
    -o StrictHostKeyChecking=accept-new root@144.76.45.114
```
- Deploy key: `~/.ssh/mikeos_osm`. Host key (ED25519): `SHA256:UXn2a/vuBi3NZKkhWsoVLnO+FSgpCfHjQZ6PRT1vwjE`.
- **Secrets are NOT in git.** They live in the gitignored `mikeos-osm/.deploy-credentials`
  (server + host key + `OSM_TOKEN` + `NOMINATIM_PASSWORD`) and `mikeos-osm/.keys`
  (`CLOUDFLARE_API_TOKEN` for `osmike.com`). Live service env on the box is `/root/<service>/.env`.

## What's already running (don't disturb it)
Docker Compose in **`/root/mikeos-osm`**: `mikeos-{caddy,nominatim,overpass,osrm,basemap}`. Caddy owns
:80/:443 and terminates TLS (Let's Encrypt). Heavy OSM imports may be running in tmux `osm` — leave them.

## Where THIS service lives
Clone the repo to **`/root/<repo-name>`** on the box; run it with its own `docker-compose.yml`.
Persistent data goes under **`/data/mikevideo/…`** (the big RAID). Never write bulk data to `/`.

## Networking → Caddy must reach this container
Caddy runs in the `mikeos-osm` compose network. Make this container reachable one of two ways
(pick what works, document which you used):
1. **Shared external docker network** — create `docker network create mikeos-net` (once), attach
   both this service (in its compose) and Caddy to it, and `reverse_proxy <container-name>:<port>`.
2. **Host port + gateway** — publish `127.0.0.1:<port>` and `reverse_proxy` to the docker bridge
   gateway (`172.17.0.1:<port>`) from Caddy. Simple, no touching the OSM stack.

## Deploy (the standard flow)
```bash
# on the box
cd /root && git clone git@github.com:milkosten/<repo>.git   # or: cd /root/<repo> && git pull
cd /root/<repo> && cp .env.example .env && $EDITOR .env      # fill secrets (never commit .env)
docker compose up -d --build
docker compose logs -f --tail=50                            # confirm healthy
# add the Caddy site block (below) to /root/mikeos-osm/Caddyfile, then:
docker restart mikeos-caddy                                 # mints the Let's Encrypt cert
```

### Caddy site block (add to `/root/mikeos-osm/Caddyfile`)
```
<host>.osmike.com {
    request_body { max_size 8GB }     # ingest host only; keep the API host small
    reverse_proxy <container-or-172.17.0.1>:<port>
}
```

## DNS (Cloudflare API) — grey-cloud so Caddy owns TLS
`CLOUDFLARE_API_TOKEN` from `mikeos-osm/.keys`; zone `osmike.com` = `cd63b925b8dd92c77b69f0660781d559`.
```bash
set -a; . mikeos-osm/.keys; set +a
curl -s -X POST "https://api.cloudflare.com/client/v4/zones/cd63b925b8dd92c77b69f0660781d559/dns_records" \
  -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json" \
  -d '{"type":"A","name":"<host>","content":"144.76.45.114","ttl":300,"proxied":false}'
# + the AAAA to 2a01:4f8:191:51aa::2
```
Keep records **`proxied:false`** (grey-cloud). See `mikeos-architecture/docs/domain-management.md`.
