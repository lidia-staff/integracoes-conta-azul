from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import text
import os

from app.api.routes_upload import router as upload_router
from app.api.routes_sales import router as sales_router
from app.api.routes_companies import router as companies_router
from app.api.routes_oauth import router as oauth_router
from app.api.routes_dashboard import router as dashboard_router
from app.api.routes_asaas import router as asaas_router
from app.api.routes_asaas_webhook import router as asaas_webhook_router

from app.db.session import Base, engine
from app.db import models  # noqa: F401
from app.db import dashboard_models  # noqa: F401

app = FastAPI(title="Integração Conta Azul — StaffConnect")

app.include_router(upload_router, prefix="/v1")
app.include_router(sales_router, prefix="/v1")
app.include_router(companies_router, prefix="/v1")
app.include_router(oauth_router)
app.include_router(dashboard_router)
app.include_router(asaas_router, prefix="/v1")
app.include_router(asaas_webhook_router)


@app.get("/debug/env")
def debug_env():
    return {
        "CA_CLIENT_ID": "OK" if os.getenv("CA_CLIENT_ID") else "MISSING",
        "CA_CLIENT_SECRET": "OK" if os.getenv("CA_CLIENT_SECRET") else "MISSING",
        "CA_REDIRECT_URI": os.getenv("CA_REDIRECT_URI"),
        "CA_API_BASE_URL": os.getenv("CA_API_BASE_URL"),
        "DATABASE_URL": "OK" if os.getenv("DATABASE_URL") else "MISSING",
    }


def _load_html():
    path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/painel", response_class=HTMLResponse)
def painel():
    return HTMLResponse(content=_load_html())


@app.get("/painel/{slug}", response_class=HTMLResponse)
def painel_slug(slug: str):
    return HTMLResponse(content=_load_html())


@app.get("/onboarding", response_class=HTMLResponse)
def onboarding():
    path = os.path.join(os.path.dirname(__file__), "static", "onboarding.html")
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


def _load_dashboard_html(filename: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "static", "dashboard", filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_login():
    return HTMLResponse(content=_load_dashboard_html("index.html"))


@app.get("/dashboard/app", response_class=HTMLResponse)
def dashboard_app():
    return HTMLResponse(content=_load_dashboard_html("index.html"))


@app.get("/dashboard/onboarding", response_class=HTMLResponse)
def dashboard_onboarding():
    return HTMLResponse(content=_load_dashboard_html("onboarding.html"))


@app.get("/")
def root():
    return {"ok": True, "service": "ca-sales-api", "painel": "/painel", "dashboard": "/dashboard"}


@app.get("/health")
def health():
    return {"ok": True}


def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from app.services.dashboard_snapshot_job import run_all_snapshots_job

        scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
        scheduler.add_job(run_all_snapshots_job, "cron", day=5, hour=6, minute=0)
        scheduler.start()
        print("[SCHEDULER] APScheduler iniciado — snapshot automático todo dia 5 às 6h")
    except Exception as e:
        print(f"[SCHEDULER] AVISO: APScheduler não iniciado: {e}")


def run_schema_migrations():
    stmts = [
        # Companies
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS default_item_id VARCHAR;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS ca_financial_account_id VARCHAR;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS access_token TEXT;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS refresh_token TEXT;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS token_expires_at TIMESTAMP;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS review_mode BOOLEAN DEFAULT TRUE;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS slug VARCHAR(100);",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS access_pin VARCHAR(64);",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS group_mode VARCHAR(20) DEFAULT 'grouped';",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS ca_sale_status VARCHAR(30) DEFAULT 'EM_ANDAMENTO';",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS item_type VARCHAR(20) DEFAULT 'servico';",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS asaas_enabled BOOLEAN DEFAULT FALSE;",
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS upload_enabled BOOLEAN DEFAULT TRUE;",
        # Sales
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS sale_number VARCHAR(50);",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS discount_amount NUMERIC(12,2);",
        "ALTER TABLE sales ADD COLUMN IF NOT EXISTS cost_center_id VARCHAR(80);",
        # Índices
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_companies_slug ON companies(slug) WHERE slug IS NOT NULL;",
        # Tabelas auxiliares
        """CREATE TABLE IF NOT EXISTS company_payment_accounts (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            payment_method_key VARCHAR(50) NOT NULL,
            ca_financial_account_id VARCHAR(80) NOT NULL,
            label VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_company_payment_method UNIQUE (company_id, payment_method_key)
        );""",
        """CREATE TABLE IF NOT EXISTS company_products (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            product_key VARCHAR(250) NOT NULL,
            product_name VARCHAR(200),
            ca_product_id VARCHAR(80) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_company_product_key UNIQUE (company_id, product_key)
        );""",
        "CREATE INDEX IF NOT EXISTS ix_company_products_company_key ON company_products(company_id, product_key);",
        """CREATE TABLE IF NOT EXISTS company_categories (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            name_key VARCHAR(250) NOT NULL,
            label VARCHAR(200),
            ca_category_id VARCHAR(80) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_company_category_key UNIQUE (company_id, name_key)
        );""",
        """CREATE TABLE IF NOT EXISTS company_cost_centers (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            name_key VARCHAR(250) NOT NULL,
            label VARCHAR(200),
            ca_cost_center_id VARCHAR(80) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_company_cost_center_key UNIQUE (company_id, name_key)
        );""",
        # ── Asaas integration tables ─────────────────────────────────
        """CREATE TABLE IF NOT EXISTS asaas_credentials (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            api_key TEXT NOT NULL,
            environment VARCHAR(20) DEFAULT 'production',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_asaas_credential_company UNIQUE (company_id)
        );""",
        """CREATE TABLE IF NOT EXISTS asaas_processed_events (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            asaas_payment_id VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL,
            error_detail TEXT,
            processed_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_asaas_processed UNIQUE (company_id, asaas_payment_id)
        );""",
        """CREATE TABLE IF NOT EXISTS asaas_execution_logs (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            asaas_payment_id VARCHAR(100),
            status VARCHAR(20) NOT NULL,
            ca_customer_id VARCHAR(100),
            ca_receivable_id VARCHAR(100),
            error_detail TEXT,
            payload_summary TEXT,
            duration_ms INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        );""",
        "CREATE INDEX IF NOT EXISTS ix_asaas_exec_logs_company ON asaas_execution_logs(company_id, created_at DESC);",
        # ── Dashboard tables ─────────────────────────────────────────
        """CREATE TABLE IF NOT EXISTS dash_partners (
            id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            slug VARCHAR(100),
            logo_url TEXT,
            primary_color VARCHAR(20) DEFAULT '#F26522',
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_dash_partners_slug UNIQUE (slug)
        );""",
        """CREATE TABLE IF NOT EXISTS dash_clients (
            id SERIAL PRIMARY KEY,
            partner_id INTEGER NOT NULL REFERENCES dash_partners(id) ON DELETE CASCADE,
            name VARCHAR(200) NOT NULL,
            segment VARCHAR(50) DEFAULT 'servico',
            logo_url TEXT,
            primary_color VARCHAR(20) DEFAULT '#F26522',
            ca_access_token TEXT,
            ca_refresh_token TEXT,
            ca_token_expires_at TIMESTAMP,
            ignored_accounts TEXT DEFAULT '[]',
            ignored_categories TEXT DEFAULT '[]',
            benchmarks TEXT DEFAULT '{}',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        );""",
        """CREATE TABLE IF NOT EXISTS dash_users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(200) NOT NULL,
            password_hash VARCHAR(200) NOT NULL,
            role VARCHAR(20) NOT NULL DEFAULT 'client',
            partner_id INTEGER REFERENCES dash_partners(id) ON DELETE SET NULL,
            client_id INTEGER REFERENCES dash_clients(id) ON DELETE SET NULL,
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_dash_users_email UNIQUE (email)
        );""",
        """CREATE TABLE IF NOT EXISTS dash_snapshots (
            id SERIAL PRIMARY KEY,
            client_id INTEGER NOT NULL REFERENCES dash_clients(id) ON DELETE CASCADE,
            snapshot_month VARCHAR(7) NOT NULL,
            data_json TEXT NOT NULL DEFAULT '{}',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_dash_snapshot_client_month UNIQUE (client_id, snapshot_month)
        );""",
    ]
    # Colunas adicionadas em versões posteriores (ALTER TABLE idempotente)
    column_migrations = [
        "ALTER TABLE dash_clients ADD COLUMN IF NOT EXISTS bg_color VARCHAR(20) DEFAULT '#0f0f0f'",
    ]
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))
        for s in column_migrations:
            conn.execute(text(s))


Base.metadata.create_all(bind=engine)
run_schema_migrations()
_start_scheduler()
