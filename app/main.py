from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import text
import os

from app.api.routes_upload import router as upload_router
from app.api.routes_sales import router as sales_router
from app.api.routes_companies import router as companies_router
from app.api.routes_oauth import router as oauth_router

from app.db.session import Base, engine
from app.db import models  # noqa: F401

app = FastAPI(title="Automatizar Input Vendas - Conta Azul")

app.include_router(upload_router, prefix="/v1")
app.include_router(sales_router, prefix="/v1")
app.include_router(companies_router, prefix="/v1")
app.include_router(oauth_router)


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


@app.get("/")
def root():
    return {"ok": True, "service": "ca-sales-api", "painel": "/painel", "onboarding": "/onboarding"}


@app.get("/health")
def health():
    return {"ok": True}


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
        """CREATE TABLE IF NOT EXISTS company_cost_centers (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            name_key VARCHAR(250) NOT NULL,
            label VARCHAR(200),
            ca_cost_center_id VARCHAR(80) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uq_company_cost_center_key UNIQUE (company_id, name_key)
        );""",
    ]
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))


Base.metadata.create_all(bind=engine)
run_schema_migrations()
