import os
import secrets
import datetime as dt

import requests
from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from sqlalchemy.orm import Session
from urllib.parse import urlencode

from app.db.session import SessionLocal
from app.db.models import Company
from app.db.dashboard_models import DashClient

router = APIRouter(tags=["oauth"])


def _env_or_fail(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise HTTPException(status_code=500, detail=f"{name} não configurado")
    return v


@router.get("/api/contaazul/start")
def contaazul_start(
    company_id: int | None = None,
    context: str | None = None,       # "dashboard" | None
    dash_client_id: int | None = None,
):
    """
    Inicia o fluxo OAuth do Conta Azul.

    Módulo de vendas: ?company_id=X
    Módulo dashboard: ?context=dashboard&dash_client_id=X
    """
    ca_client_id = _env_or_fail("CA_CLIENT_ID")
    redirect_uri = _env_or_fail("CA_REDIRECT_URI")

    scope = "openid profile aws.cognito.signin.user.admin"
    nonce = secrets.token_urlsafe(16)

    # Encode contexto no state para recuperar no callback
    if context == "dashboard":
        if not dash_client_id:
            raise HTTPException(status_code=400, detail="dash_client_id obrigatório para context=dashboard")
        state = f"dashboard:{dash_client_id}:{nonce}"
    else:
        if not company_id:
            raise HTTPException(status_code=400, detail="company_id obrigatório")
        state = f"{company_id}:{nonce}"

    params = {
        "response_type": "code",
        "client_id": ca_client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": scope,
    }

    auth_url = "https://auth.contaazul.com/login?" + urlencode(params)
    return RedirectResponse(url=auth_url, status_code=302)


@router.get("/api/contaazul/callback")
def contaazul_callback(code: str, state: str):
    """
    Callback OAuth — troca code por tokens e salva na Company ou DashClient.
    """
    ca_client_id = _env_or_fail("CA_CLIENT_ID")
    ca_client_secret = _env_or_fail("CA_CLIENT_SECRET")
    redirect_uri = _env_or_fail("CA_REDIRECT_URI")

    # Detecta contexto pelo state
    parts = state.split(":")
    is_dashboard = parts[0] == "dashboard"

    try:
        if is_dashboard:
            dash_client_id = int(parts[1])
        else:
            company_id = int(parts[0])
    except Exception:
        raise HTTPException(status_code=400, detail="state inválido")

    # Troca code por tokens
    r = requests.post(
        "https://auth.contaazul.com/oauth2/token",
        auth=(ca_client_id, ca_client_secret),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
        timeout=30,
    )

    if r.status_code >= 400:
        if is_dashboard:
            return _popup_error(f"Erro ao trocar code: {r.status_code}")
        raise HTTPException(status_code=400, detail=f"token_exchange_failed: {r.status_code} {r.text}")

    data = r.json()
    access_token = data.get("access_token")
    refresh_token = data.get("refresh_token")
    expires_in = int(data.get("expires_in", 3600))

    if not access_token or not refresh_token:
        if is_dashboard:
            return _popup_error(f"Retorno sem tokens: {data}")
        raise HTTPException(status_code=400, detail=f"Retorno sem tokens: {data}")

    expires_at = dt.datetime.utcnow() + dt.timedelta(seconds=expires_in)

    # ── Salva tokens ─────────────────────────────────────────────────
    db: Session = SessionLocal()
    try:
        if is_dashboard:
            # Salva em dash_clients
            client = db.query(DashClient).filter(DashClient.id == dash_client_id).first()
            if not client:
                return _popup_error(f"DashClient {dash_client_id} não encontrado")
            client.ca_access_token = access_token
            client.ca_refresh_token = refresh_token
            client.ca_token_expires_at = expires_at
            db.add(client)
            db.commit()
            client_name = client.name
        else:
            # Salva em companies (módulo de vendas — comportamento original)
            c = db.query(Company).filter(Company.id == company_id).first()
            if not c:
                raise HTTPException(status_code=404, detail="Company não encontrada")
            c.access_token = access_token
            c.refresh_token = refresh_token
            c.token_expires_at = expires_at
            db.add(c)
            db.commit()
    finally:
        db.close()

    # ── Resposta ─────────────────────────────────────────────────────
    if is_dashboard:
        # Fecha o popup e avisa o onboarding via postMessage
        return _popup_success(client_name)
    else:
        # Comportamento original — página de confirmação
        html = f"""
        <html>
          <body style="font-family: Arial; padding: 24px;">
            <h2>&#x2705; Conta Azul conectado com sucesso</h2>
            <p>Company ID: <b>{company_id}</b></p>
            <p>Você já pode voltar para a plataforma e enviar as vendas.</p>
          </body>
        </html>
        """
        return HTMLResponse(content=html, status_code=200)


# ── Helpers para popup do onboarding Dashboard ───────────────────────

def _popup_success(client_name: str) -> HTMLResponse:
    """Fecha o popup OAuth e envia postMessage de sucesso para o onboarding."""
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><title>Conectado</title></head>
<body style="font-family:'DM Sans',Arial,sans-serif;display:flex;align-items:center;
             justify-content:center;min-height:100vh;background:#F5EDD6;margin:0">
  <div style="background:white;border-radius:16px;padding:40px;text-align:center;
              box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:360px">
    <div style="font-size:48px;margin-bottom:16px">&#x2705;</div>
    <div style="font-family:'Syne',Arial,sans-serif;font-size:20px;font-weight:800;
                margin-bottom:8px">Conta Azul conectado!</div>
    <div style="font-size:14px;color:#6B6B6B;margin-bottom:24px">
      <b>{client_name}</b> autorizado com sucesso.<br>
      Esta janela vai fechar automaticamente.
    </div>
  </div>
  <script>
    try {{
      window.opener.postMessage({{ type: 'oauth_success' }}, '*');
    }} catch(e) {{}}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)


def _popup_error(message: str) -> HTMLResponse:
    """Fecha o popup OAuth e envia postMessage de erro para o onboarding."""
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><title>Erro</title></head>
<body style="font-family:'DM Sans',Arial,sans-serif;display:flex;align-items:center;
             justify-content:center;min-height:100vh;background:#F5EDD6;margin:0">
  <div style="background:white;border-radius:16px;padding:40px;text-align:center;
              box-shadow:0 4px 24px rgba(0,0,0,.08);max-width:360px">
    <div style="font-size:48px;margin-bottom:16px">&#x274C;</div>
    <div style="font-family:'Syne',Arial,sans-serif;font-size:20px;font-weight:800;
                margin-bottom:8px;color:#E74C3C">Erro na autorização</div>
    <div style="font-size:13px;color:#6B6B6B;margin-bottom:24px">{message}</div>
  </div>
  <script>
    try {{
      window.opener.postMessage({{ type: 'oauth_error', message: '{message}' }}, '*');
    }} catch(e) {{}}
    setTimeout(() => window.close(), 3000);
  </script>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)
