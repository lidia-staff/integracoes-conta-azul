"""
Dashboard API — todos os endpoints /dashboard/*
"""

from __future__ import annotations
import json
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.db.session import SessionLocal
from app.db.dashboard_models import DashPartner, DashClient, DashUser, DashSnapshot
from app.services.dashboard_auth import (
    authenticate_user,
    create_token,
    hash_password,
    get_current_user,
    require_master,
    require_master_or_partner,
)
from app.services.conta_azul_client import DashboardCAClient
from app.services.dashboard_snapshot_job import run_snapshot, run_snapshot_last_n_months
from app.services.dashboard_service import build_category_map

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


# ── Bootstrap — primeiro usuário Master ──────────────────────────────
# Protegido por BOOTSTRAP_SECRET (variável de ambiente).
# Bloqueado automaticamente após o primeiro Master ser criado.

class BootstrapRequest(BaseModel):
    secret: str
    email: str
    password: str


@router.post("/bootstrap", summary="Cria o primeiro usuário Master (uso único)")
def bootstrap(req: BootstrapRequest):
    """
    Cria o primeiro usuário Master do sistema.
    - Só funciona se a variável BOOTSTRAP_SECRET estiver definida no Railway.
    - Bloqueado automaticamente após o primeiro Master existir.
    - Remova BOOTSTRAP_SECRET do Railway após usar para desativar.
    """
    expected = os.getenv("BOOTSTRAP_SECRET", "")
    if not expected:
        raise HTTPException(status_code=403, detail="Bootstrap desativado (BOOTSTRAP_SECRET não configurado)")

    if req.secret != expected:
        raise HTTPException(status_code=403, detail="Secret inválido")

    db = SessionLocal()
    try:
        # Bloqueia se já existe algum Master
        existing_master = db.query(DashUser).filter(DashUser.role == "master").first()
        if existing_master:
            raise HTTPException(
                status_code=409,
                detail=f"Já existe um usuário Master ({existing_master.email}). Bootstrap bloqueado."
            )

        # Verifica se email já está em uso
        if db.query(DashUser).filter(DashUser.email == req.email.strip().lower()).first():
            raise HTTPException(status_code=409, detail="Email já cadastrado")

        user = DashUser(
            email=req.email.strip().lower(),
            password_hash=hash_password(req.password),
            role="master",
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        token = create_token(user.id, "master", None, None)
        return {
            "ok": True,
            "message": "Usuário Master criado com sucesso. Remova BOOTSTRAP_SECRET do Railway agora.",
            "email": user.email,
            "token": token,
        }
    finally:
        db.close()


# ── Schemas ─────────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    email: str
    password: str


class CreatePartnerRequest(BaseModel):
    name: str
    slug: str | None = None
    logo_url: str | None = None
    primary_color: str = "#F26522"


class CreateClientRequest(BaseModel):
    name: str
    segment: str = "servico"
    logo_url: str | None = None
    primary_color: str = "#F26522"
    # partner_id preenchido automaticamente pelo JWT do Parceiro; Master informa explicitamente
    partner_id: int | None = None


class UpdateClientRequest(BaseModel):
    name: str | None = None
    segment: str | None = None
    logo_url: str | None = None
    primary_color: str | None = None
    bg_color: str | None = None
    ignored_accounts: list[str] | None = None
    ignored_categories: list[str] | None = None
    benchmarks: dict | None = None
    active: bool | None = None


class CreateUserRequest(BaseModel):
    email: str
    password: str
    role: str = "client"
    partner_id: int | None = None
    client_id: int | None = None


class SaveOAuthTokensRequest(BaseModel):
    """Usado pelo callback OAuth do onboarding para salvar tokens no dash_client."""
    dash_client_id: int
    access_token: str
    refresh_token: str
    expires_at: str | None = None  # ISO datetime


# ── Auth ─────────────────────────────────────────────────────────────


@router.post("/auth/login")
def login(req: LoginRequest):
    user = authenticate_user(req.email, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Email ou senha inválidos")
    token = create_token(user.id, user.role, user.partner_id, user.client_id)
    return {
        "token": token,
        "role": user.role,
        "partner_id": user.partner_id,
        "client_id": user.client_id,
    }


@router.post("/auth/create-user")
def create_user(req: CreateUserRequest, _: dict = Depends(require_master)):
    """Cria usuário — apenas Master."""
    db = SessionLocal()
    try:
        existing = db.query(DashUser).filter(DashUser.email == req.email.strip().lower()).first()
        if existing:
            raise HTTPException(status_code=409, detail="Email já cadastrado")
        user = DashUser(
            email=req.email.strip().lower(),
            password_hash=hash_password(req.password),
            role=req.role,
            partner_id=req.partner_id,
            client_id=req.client_id,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return {"id": user.id, "email": user.email, "role": user.role}
    finally:
        db.close()


# ── Partners ─────────────────────────────────────────────────────────


@router.get("/partners")
def list_partners(_: dict = Depends(require_master)):
    db = SessionLocal()
    try:
        partners = db.query(DashPartner).order_by(DashPartner.name).all()
        return [
            {
                "id": p.id,
                "name": p.name,
                "slug": p.slug,
                "logo_url": p.logo_url,
                "primary_color": p.primary_color,
                "total_clients": len(p.clients),
            }
            for p in partners
        ]
    finally:
        db.close()


@router.post("/partners")
def create_partner(req: CreatePartnerRequest, _: dict = Depends(require_master)):
    db = SessionLocal()
    try:
        partner = DashPartner(
            name=req.name,
            slug=req.slug,
            logo_url=req.logo_url,
            primary_color=req.primary_color,
        )
        db.add(partner)
        db.commit()
        db.refresh(partner)
        return {"id": partner.id, "name": partner.name}
    finally:
        db.close()


# ── Clients ───────────────────────────────────────────────────────────


@router.get("/clients")
def list_clients(user: dict = Depends(get_current_user)):
    db = SessionLocal()
    try:
        q = db.query(DashClient)
        if user["role"] == "partner":
            q = q.filter(DashClient.partner_id == user["partner_id"])
        elif user["role"] == "client":
            q = q.filter(DashClient.id == user["client_id"])
        # master vê todos

        clients = q.order_by(DashClient.name).all()
        result = []
        for c in clients:
            # Último snapshot
            last_snap = (
                db.query(DashSnapshot)
                .filter(DashSnapshot.client_id == c.id)
                .order_by(DashSnapshot.snapshot_month.desc())
                .first()
            )
            partner = db.query(DashPartner).filter(DashPartner.id == c.partner_id).first() if c.partner_id else None
            result.append({
                "id": c.id,
                "partner_id": c.partner_id,
                "partner_name": partner.name if partner else "—",
                "name": c.name,
                "segment": c.segment,
                "logo_url": c.logo_url,
                "primary_color": c.primary_color,
                "bg_color": c.bg_color or "#0f0f0f",
                "active": c.active,
                "last_snapshot": last_snap.snapshot_month if last_snap else None,
                "last_updated": last_snap.updated_at.isoformat() if last_snap else None,
                "ignored_accounts":   json.loads(c.ignored_accounts or "[]"),
                "ignored_categories": json.loads(c.ignored_categories or "[]"),
            })
        return result
    finally:
        db.close()


@router.post("/clients")
def create_client(req: CreateClientRequest, user: dict = Depends(require_master_or_partner)):
    partner_id = req.partner_id
    if user["role"] == "partner":
        partner_id = user["partner_id"]
    # Parceiro DEVE ter partner_id; Master pode criar sem parceiro (partner_id=None)
    if not partner_id and user["role"] == "partner":
        raise HTTPException(status_code=400, detail="partner_id obrigatório para parceiro")

    db = SessionLocal()
    try:
        if partner_id:
            partner = db.query(DashPartner).filter(DashPartner.id == partner_id).first()
            if not partner:
                raise HTTPException(status_code=404, detail="Parceiro não encontrado")

        client = DashClient(
            partner_id=partner_id,
            name=req.name,
            segment=req.segment,
            logo_url=req.logo_url,
            primary_color=req.primary_color,
        )
        db.add(client)
        db.commit()
        db.refresh(client)
        return {"id": client.id, "name": client.name}
    finally:
        db.close()


@router.put("/clients/{client_id}")
def update_client(
    client_id: int,
    req: UpdateClientRequest,
    user: dict = Depends(require_master_or_partner),
):
    db = SessionLocal()
    try:
        client = db.query(DashClient).filter(DashClient.id == client_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Cliente não encontrado")

        # Parceiro só pode editar próprios clientes
        if user["role"] == "partner" and client.partner_id != user["partner_id"]:
            raise HTTPException(status_code=403, detail="Acesso negado")

        if req.name is not None:
            client.name = req.name
        if req.segment is not None:
            client.segment = req.segment
        if req.logo_url is not None:
            client.logo_url = req.logo_url
        if req.primary_color is not None:
            client.primary_color = req.primary_color
        if req.bg_color is not None:
            client.bg_color = req.bg_color
        if req.ignored_accounts is not None:
            client.ignored_accounts = json.dumps(req.ignored_accounts)
        if req.ignored_categories is not None:
            client.ignored_categories = json.dumps(req.ignored_categories)
        if req.benchmarks is not None:
            client.benchmarks = json.dumps(req.benchmarks)
        if req.active is not None:
            client.active = req.active

        db.commit()
        return {"ok": True}
    finally:
        db.close()


@router.delete("/clients/{client_id}")
def delete_client(client_id: int, _: dict = Depends(require_master)):
    """Remove cliente e todos os snapshots associados. Apenas Master."""
    db = SessionLocal()
    try:
        client = db.query(DashClient).filter(DashClient.id == client_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Cliente não encontrado")
        # Remove snapshots primeiro (FK)
        db.query(DashSnapshot).filter(DashSnapshot.client_id == client_id).delete()
        db.delete(client)
        db.commit()
        return {"ok": True, "deleted": client_id}
    finally:
        db.close()


@router.delete("/snapshot/{client_id}/{mes}")
def delete_snapshot(client_id: int, mes: str, _: dict = Depends(require_master_or_partner)):
    """Remove snapshot de um mês específico. Formato: YYYY-MM."""
    db = SessionLocal()
    try:
        deleted = (
            db.query(DashSnapshot)
            .filter(DashSnapshot.client_id == client_id, DashSnapshot.snapshot_month == mes)
            .delete()
        )
        db.commit()
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Snapshot {mes} não encontrado")
        return {"ok": True, "deleted_month": mes, "client_id": client_id}
    finally:
        db.close()


# ── OAuth callback para Dashboard ────────────────────────────────────


@router.post("/clients/{client_id}/oauth-tokens")
def save_oauth_tokens(
    client_id: int,
    req: SaveOAuthTokensRequest,
    user: dict = Depends(require_master_or_partner),
):
    """Salva tokens CA no dash_client após o fluxo OAuth do onboarding."""
    db = SessionLocal()
    try:
        client = db.query(DashClient).filter(DashClient.id == client_id).first()
        if not client:
            raise HTTPException(status_code=404, detail="Cliente não encontrado")
        if user["role"] == "partner" and client.partner_id != user["partner_id"]:
            raise HTTPException(status_code=403, detail="Acesso negado")

        client.ca_access_token = req.access_token
        client.ca_refresh_token = req.refresh_token
        if req.expires_at:
            client.ca_token_expires_at = datetime.fromisoformat(req.expires_at)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# ── CA data helpers (onboarding) ─────────────────────────────────────


@router.get("/ca/contas/{client_id}")
def list_ca_accounts(client_id: int, user: dict = Depends(require_master_or_partner)):
    """Lista contas financeiras do CA — usado no onboarding etapa 3."""
    _check_client_access(client_id, user)
    try:
        ca = DashboardCAClient(client_id)
        accounts = ca.list_financial_accounts()
        return {"contas": accounts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ca/categorias/{client_id}")
def list_ca_categories(client_id: int, user: dict = Depends(require_master_or_partner)):
    """Lista categorias financeiras do CA com entrada_dre — usado no onboarding etapa 3."""
    _check_client_access(client_id, user)
    try:
        ca = DashboardCAClient(client_id)
        cats = ca.list_categories_dre()
        return {"categorias": cats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ca/debug-snapshot/{client_id}")
def debug_snapshot(client_id: int, user: dict = Depends(require_master)):
    """Debug: retorna JSON bruto do snapshot salvo no banco."""
    db = SessionLocal()
    try:
        snaps = (
            db.query(DashSnapshot)
            .filter(DashSnapshot.client_id == client_id)
            .order_by(DashSnapshot.snapshot_month.desc())
            .all()
        )
        if not snaps:
            return {"error": "Nenhum snapshot encontrado", "client_id": client_id}
        return {
            "total_snapshots": len(snaps),
            "snapshots": [
                {
                    "month": s.snapshot_month,
                    "data": json.loads(s.data_json) if s.data_json else {},
                }
                for s in snaps
            ],
        }
    finally:
        db.close()


@router.get("/ca/debug-raw/{client_id}")
def debug_raw_transactions(
    client_id: int,
    mes: str = "2026-04",
    user: dict = Depends(require_master),
):
    """
    Debug: chama a API CA diretamente e retorna as primeiras 5 transações brutas.
    Parâmetro ?mes=YYYY-MM (default: 2026-04)
    """
    import calendar as _cal
    year, month = int(mes[:4]), int(mes[5:7])
    last_day = _cal.monthrange(year, month)[1]
    date_from = f"{year:04d}-{month:02d}-01"
    date_to = f"{year:04d}-{month:02d}-{last_day:02d}"

    try:
        ca = DashboardCAClient(client_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao inicializar DashboardCAClient: {e}")

    # Faz requisição bruta sem normalização para ver estrutura real
    raw_receita = []
    raw_despesa = []
    errors = {}

    year_from = year - 5
    year_to = year + 2

    # Receitas: status RECEBIDO funciona na CA v2
    try:
        params_rec = {
            "pagina": 1, "tamanho_pagina": 3,
            "status": "RECEBIDO",
            "data_vencimento_de": f"{year_from}-01-01",
            "data_vencimento_ate": f"{year_to}-12-31",
            "data_pagamento_de": date_from,
            "data_pagamento_ate": date_to,
        }
        raw_receita = ca._request("GET", "/v1/financeiro/eventos-financeiros/contas-a-receber/buscar", params=params_rec, timeout=30)
    except Exception as e:
        errors["receita"] = str(e)

    # Despesas: testa múltiplos valores de status para descobrir o correto
    status_despesa_tentativas = ["LIQUIDADO", "PAGO", "QUITADO", "BAIXADO"]
    despesa_status_ok = None
    for st in status_despesa_tentativas:
        try:
            params_desp = {
                "pagina": 1, "tamanho_pagina": 3,
                "status": st,
                "data_vencimento_de": f"{year_from}-01-01",
                "data_vencimento_ate": f"{year_to}-12-31",
                "data_pagamento_de": date_from,
                "data_pagamento_ate": date_to,
            }
            raw_despesa = ca._request("GET", "/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar", params=params_desp, timeout=30)
            despesa_status_ok = st
            break
        except Exception as e:
            errors[f"despesa_status_{st}"] = str(e)

    # Extrai os primeiros itens independentemente da estrutura da resposta
    def extract_items(resp):
        if isinstance(resp, list):
            return resp[:3]
        if isinstance(resp, dict):
            items = resp.get("itens", resp.get("items", resp.get("data", [])))
            return items[:3]
        return []

    receita_items = extract_items(raw_receita)
    despesa_items = extract_items(raw_despesa)

    return {
        "mes": mes,
        "periodo": {"de": date_from, "ate": date_to},
        "errors": errors,
        "receitas_total": raw_receita.get("itens_totais") if isinstance(raw_receita, dict) else len(raw_receita) if isinstance(raw_receita, list) else 0,
        "despesas_total": raw_despesa.get("itens_totais") if isinstance(raw_despesa, dict) else len(raw_despesa) if isinstance(raw_despesa, list) else 0,
        "despesa_status_que_funcionou": despesa_status_ok,
        # Estrutura COMPLETA dos primeiros itens para inspecionar campos reais
        "receitas_raw": receita_items,
        "despesas_raw": despesa_items,
    }


# ── DRE ──────────────────────────────────────────────────────────────


@router.get("/dre/{client_id}")
def get_dre(
    client_id: int,
    meses: str | None = None,  # "2025-03,2025-04" ou "2025-03"
    user: dict = Depends(get_current_user),
):
    """
    Retorna snapshot(s) DRE do cliente.
    meses: lista separada por vírgula no formato YYYY-MM.
           Se omitido, retorna todos os snapshots disponíveis.
    """
    # Verificação de acesso
    if user["role"] == "client" and user["client_id"] != client_id:
        raise HTTPException(status_code=403, detail="Acesso negado")
    if user["role"] == "partner":
        _check_client_access(client_id, user)

    db = SessionLocal()
    try:
        q = db.query(DashSnapshot).filter(DashSnapshot.client_id == client_id)
        if meses:
            month_list = [m.strip() for m in meses.split(",") if m.strip()]
            q = q.filter(DashSnapshot.snapshot_month.in_(month_list))
        snapshots = q.order_by(DashSnapshot.snapshot_month).all()

        months_data = []
        for snap in snapshots:
            try:
                data = json.loads(snap.data_json)
                months_data.append(data)
            except json.JSONDecodeError:
                pass

        # Metadados do cliente (nome, logo, cor, benchmarks)
        client = db.query(DashClient).filter(DashClient.id == client_id).first()
        available_months = [
            s.snapshot_month
            for s in db.query(DashSnapshot.snapshot_month)
            .filter(DashSnapshot.client_id == client_id)
            .order_by(DashSnapshot.snapshot_month)
            .all()
        ]

        return {
            "client": {
                "id": client.id if client else client_id,
                "name": client.name if client else "",
                "logo_url": client.logo_url if client else None,
                "primary_color": client.primary_color if client else "#F26522",
                "bg_color": (client.bg_color if client else None) or "#0f0f0f",
                "segment": client.segment if client else "servico",
                "benchmarks": json.loads(client.benchmarks or "{}") if client else {},
            },
            "available_months": available_months,
            "meses": months_data,
        }
    finally:
        db.close()


# ── Snapshot manual ───────────────────────────────────────────────────


@router.post("/snapshot/run/{client_id}")
def run_snapshot_manual(
    client_id: int,
    mes: str | None = None,      # "YYYY-MM" — mês específico (novo parâmetro principal)
    ate_hoje: bool = False,       # True = do mês informado até o mês atual
    target_month: str | None = None,  # legado
    n_months: int = 1,            # legado
    user: dict = Depends(require_master_or_partner),
):
    """
    Executa snapshot manual.

    Modo novo (preferencial):
      ?mes=2025-03            → atualiza APENAS março (não toca outros meses)
      ?mes=2025-03&ate_hoje=true → atualiza de março até o mês atual

    Modo legado:
      ?n_months=12            → últimos 12 meses a partir de hoje
    """
    _check_client_access(client_id, user)
    from datetime import date
    today = date.today()

    # Modo novo: mês específico
    if mes:
        if ate_hoje:
            # Gera lista de meses do mês informado até o atual
            y0, m0 = int(mes[:4]), int(mes[5:7])
            months_to_run = []
            y, m = y0, m0
            while (y, m) <= (today.year, today.month):
                months_to_run.append(f"{y:04d}-{m:02d}")
                m += 1
                if m > 12:
                    m = 1
                    y += 1
            results = [run_snapshot(client_id, t) for t in months_to_run]
            return {"results": results}
        else:
            # Apenas o mês informado
            result = run_snapshot(client_id, mes)
            if not result.get("ok"):
                raise HTTPException(status_code=500, detail=result.get("error", "Erro no snapshot"))
            return result

    # Modo legado
    if not target_month:
        target_month = f"{today.year:04d}-{today.month:02d}"
    if n_months > 1:
        results = run_snapshot_last_n_months(client_id, min(n_months, 24))
        return {"results": results}
    else:
        result = run_snapshot(client_id, target_month)
        if not result.get("ok"):
            raise HTTPException(status_code=500, detail=result.get("error", "Erro no snapshot"))
        return result


# ── Helpers ───────────────────────────────────────────────────────────


def _check_client_access(client_id: int, user: dict):
    """Verifica que um Parceiro tem acesso ao cliente."""
    if user["role"] == "master":
        return
    if user["role"] == "partner":
        db = SessionLocal()
        try:
            client = db.query(DashClient).filter(
                DashClient.id == client_id,
                DashClient.partner_id == user["partner_id"],
            ).first()
            if not client:
                raise HTTPException(status_code=403, detail="Acesso negado ao cliente")
        finally:
            db.close()
