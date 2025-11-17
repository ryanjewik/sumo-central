Production deployment notes (EC2 + Cloudflare)

1) Place certificates on the EC2 host (do NOT commit them to git)
   - Recommended path: /etc/ssl/sumopedia/
   - Files expected by docker-compose: /etc/ssl/sumopedia/origin-cert.pem and /etc/ssl/sumopedia/origin-key.pem

2) Secrets
   - Do NOT store secrets in repository. Use AWS Secrets Manager or SSM Parameter Store.
   - Use `.env.prod.example` files in `backend/` and `airflow/` as templates for what values are required.

3) Cloudflare
   - Create an A record for `sumopedia.net` pointing to EC2 public IP (52.38.158.114) and enable the orange cloud (proxied).
   - Set SSL/TLS mode to "Full (strict)" in Cloudflare.
   - Upload the Cloudflare origin certificate (the one in `sumo_origin_certificate_and_private_key.txt`) to the EC2 host at `/etc/ssl/sumopedia/` and reference it in the proxy service.

4) Run on EC2 (example)
   - Ensure Docker and Docker Compose are installed.
   - Place certs in `/etc/ssl/sumopedia/` and secure permissions (root only).
   - Export any required prod env variables or use a secrets provider. Example (PowerShell):

```powershell
# On Linux EC2 use sh/bash; below is an example for bash.
sudo mkdir -p /etc/ssl/sumopedia
sudo chown root:root /etc/ssl/sumopedia
sudo chmod 700 /etc/ssl/sumopedia
# scp or echo the files into place (owner root, mode 600)

# Then bring up the stack (from repo root):
docker compose -f docker-compose.full.yml up -d --build
```

5) Smoke tests
   - HTTP -> should redirect to HTTPS
   - curl -I https://sumopedia.net  (expect 200)
   - curl -I https://sumopedia.net/api/health  (if you have a health endpoint)

6) Notes on `env.prod.example` files
   - They are a safe template that documents required variables and prevents accidental commits of secrets.
   - In production you should set real values via a secure store (AWS Secrets Manager) or environment config on EC2.
