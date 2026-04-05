from datetime import datetime, timedelta
import traceback
import re
import hashlib
from typing import Optional

from fastapi import APIRouter, Body, HTTPException
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db.models import Company, CompanyPaymentAccount, CompanyCostCenter, CompanyCategory
from app.services.conta_azul_client import ContaAzulClient

router = APIRouter(tags=["companies"])

VALID_PAYMENT_KEYS = ["PIX", "CARTAO_CREDITO", "CARTAO_DEBITO", "BOLETO", "TRANSFERENCIA", "DINHEIRO", "OUTRO"]


def _slugify(text: str) -> str:
    import unicodedata
    text = unicodedata.normalize("NFD", text).encode("ASCII", "ignore").decode("ASCII")
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text


def _hash_pin(pin: str) -> str:
    """Hash simples do PIN com SHA256."""
    return hashlib.sha256(pin.strip().encode()).hexdigest()


@router.post("/companies")
def create_company(name: str = Body(..., embed=True), slug: Optional[str] = Body(None, embed=True)):
    db: Session = SessionLocal()
    try:
        existing = db.query(Company).filter(Company.name == name).first()
        if existing:
            return {"id": existing.id, "name": existing.name, "slug": existing.slug, "message": "Já existente"}
        final_slug = slug.strip() if slug else _slugify(name)
        base = final_slug
        counter = 1
        while db.query(Company).filter(Company.slug == final_slug).first():
            final_slug = f"{base}-{counter}"
            counter += 1
        company = Company(name=name, slug=final_slug)
        db.add(company)
        db.commit()
        db.refresh(company)
        return {"id": company.id, "name": company.name, "slug": company.slug}
    finally:
        db.close()


@router.get("/companies")
def list_companies():
    db: Session = SessionLocal()
    try:
        rows = db.query(Company).order_by(Company.id.asc()).all()
        return [{"id": c.id, "name": c.name, "slug": c.slug,
                 "has_token": bool(c.refresh_token), "token_expires_at": c.token_expires_at,
                 "ca_financial_account_id": c.ca_financial_account_id,
                 "default_item_id": getattr(c, "default_item_id", None),
                 "has_pin": bool(getattr(c, "access_pin", None)),
                 "review_mode": c.review_mode} for c in rows]
    finally:
        db.close()


@router.get("/companies/by-slug/{slug}")
def get_company_by_slug(slug: str):
    """Busca empresa pelo slug. Usado pelo painel para carregar por URL."""
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.slug == slug).first()
        if not c:
            raise HTTPException(status_code=404, detail=f"Empresa com slug '{slug}' não encontrada")
        return {
            "id": c.id, "name": c.name, "slug": c.slug,
            "has_token": bool(c.refresh_token), "token_expires_at": c.token_expires_at,
            "ca_financial_account_id": c.ca_financial_account_id,
            "default_item_id": getattr(c, "default_item_id", None),
            "has_pin": bool(getattr(c, "access_pin", None)),
            "review_mode": c.review_mode
            # ⚠️ access_pin NUNCA é retornado aqui
        }
    finally:
        db.close()


@router.post("/companies/by-slug/{slug}/verify-pin")
def verify_pin(slug: str, pin: str = Body(..., embed=True)):
    """Verifica o PIN de acesso ao painel. Retorna apenas ok: true/false."""
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.slug == slug).first()
        if not c:
            raise HTTPException(status_code=404, detail="Empresa não encontrada")

        stored_pin = getattr(c, "access_pin", None)

        # Se não tem PIN configurado, acesso liberado
        if not stored_pin:
            return {"ok": True}

        # Compara hash do PIN informado com o hash armazenado
        if _hash_pin(pin) == stored_pin:
            return {"ok": True}

        return {"ok": False}
    finally:
        db.close()


@router.get("/companies/{company_id}")
def get_company(company_id: int):
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
        return {
            "id": c.id, "name": c.name, "slug": c.slug,
            "has_token": bool(c.refresh_token), "token_expires_at": c.token_expires_at,
            "ca_financial_account_id": c.ca_financial_account_id,
            "default_item_id": getattr(c, "default_item_id", None),
            "has_pin": bool(getattr(c, "access_pin", None)),
            "review_mode": c.review_mode,
            "group_mode": getattr(c, "group_mode", "grouped") or "grouped",
            "ca_sale_status": getattr(c, "ca_sale_status", "EM_ANDAMENTO") or "EM_ANDAMENTO",
            "item_type": getattr(c, "item_type", "servico") or "servico",
            "asaas_enabled": bool(getattr(c, "asaas_enabled", False)),
            "upload_enabled": bool(getattr(c, "upload_enabled", True)),
        }
    finally:
        db.close()


@router.patch("/companies/{company_id}")
def update_company(
    company_id: int,
    name: Optional[str] = Body(None, embed=True),
    slug: Optional[str] = Body(None, embed=True),
    review_mode: Optional[bool] = Body(None, embed=True),
    default_item_id: Optional[str] = Body(None, embed=True),
    ca_financial_account_id: Optional[str] = Body(None, embed=True),
    access_pin: Optional[str] = Body(None, embed=True),
    group_mode: Optional[str] = Body(None, embed=True),
    ca_sale_status: Optional[str] = Body(None, embed=True),
    item_type: Optional[str] = Body(None, embed=True),
    asaas_enabled: Optional[bool] = Body(None, embed=True),
    upload_enabled: Optional[bool] = Body(None, embed=True),
):
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
        if name:
            c.name = name
        if slug:
            c.slug = slug.strip()
        if review_mode is not None:
            c.review_mode = review_mode
        if default_item_id is not None:
            c.default_item_id = default_item_id
        if ca_financial_account_id is not None:
            c.ca_financial_account_id = ca_financial_account_id
        if access_pin is not None:
            # Salva o hash do PIN (nunca o PIN em texto claro)
            c.access_pin = _hash_pin(access_pin) if access_pin.strip() else None
        if group_mode is not None:
            valid_modes = ["grouped", "individual", "by_sale_number"]
            if group_mode not in valid_modes:
                raise HTTPException(status_code=400, detail=f"group_mode inválido. Válidos: {valid_modes}")
            c.group_mode = group_mode
        if ca_sale_status is not None:
            valid_statuses = ["EM_ANDAMENTO", "APROVADO", "CONCLUIDO"]
            if ca_sale_status not in valid_statuses:
                raise HTTPException(status_code=400, detail=f"ca_sale_status inválido. Válidos: {valid_statuses}")
            c.ca_sale_status = ca_sale_status
        if item_type is not None:
            valid_types = ["servico", "produto"]
            if item_type not in valid_types:
                raise HTTPException(status_code=400, detail=f"item_type inválido. Válidos: {valid_types}")
            c.item_type = item_type
        if asaas_enabled is not None:
            c.asaas_enabled = asaas_enabled
        if upload_enabled is not None:
            c.upload_enabled = upload_enabled
        db.add(c)
        db.commit()
        db.refresh(c)
        return {"id": c.id, "name": c.name, "slug": c.slug, "has_pin": bool(c.access_pin), "group_mode": c.group_mode, "ca_sale_status": c.ca_sale_status, "item_type": c.item_type}
    finally:
        db.close()


@router.post("/companies/{company_id}/tokens")
def set_company_tokens(company_id: int, access_token: str = Body(..., embed=True),
                       refresh_token: str = Body(..., embed=True), expires_in: int = Body(3600, embed=True)):
    db: Session = SessionLocal()
    try:
        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company não encontrada")
        company.access_token = access_token
        company.refresh_token = refresh_token
        company.token_expires_at = datetime.utcnow() + timedelta(seconds=int(expires_in))
        db.add(company)
        db.commit()
        return {"ok": True, "company_id": company_id, "token_expires_at": company.token_expires_at}
    finally:
        db.close()


@router.get("/companies/{company_id}/ca/financial-accounts")
def ca_list_financial_accounts(company_id: int):
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
    finally:
        db.close()
    try:
        client = ContaAzulClient(company_id=company_id)
        return client.list_financial_accounts()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@router.post("/companies/{company_id}/ca/financial-account")
def ca_set_financial_account(company_id: int, ca_financial_account_id: str = Body(..., embed=True)):
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
        c.ca_financial_account_id = ca_financial_account_id
        db.add(c)
        db.commit()
        return {"ok": True, "ca_financial_account_id": ca_financial_account_id}
    finally:
        db.close()


@router.get("/companies/{company_id}/ca/products")
def ca_list_products(company_id: int):
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
    finally:
        db.close()
    try:
        client = ContaAzulClient(company_id=company_id)
        return client.list_products()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")


@router.get("/companies/{company_id}/payment-accounts")
def list_payment_accounts(company_id: int):
    db: Session = SessionLocal()
    try:
        accounts = db.query(CompanyPaymentAccount).filter(
            CompanyPaymentAccount.company_id == company_id).all()
        return [{"payment_method_key": a.payment_method_key,
                 "ca_financial_account_id": a.ca_financial_account_id,
                 "label": a.label} for a in accounts]
    finally:
        db.close()


@router.post("/companies/{company_id}/payment-accounts")
def set_payment_account(company_id: int,
                        payment_method_key: str = Body(..., embed=True),
                        ca_financial_account_id: str = Body(..., embed=True),
                        label: Optional[str] = Body(None, embed=True)):
    key = payment_method_key.strip().upper()
    if key not in VALID_PAYMENT_KEYS:
        raise HTTPException(status_code=400, detail=f"Chave inválida. Válidas: {VALID_PAYMENT_KEYS}")
    db: Session = SessionLocal()
    try:
        c = db.query(Company).filter(Company.id == company_id).first()
        if not c:
            raise HTTPException(status_code=404, detail="Company não encontrada")
        existing = db.query(CompanyPaymentAccount).filter(
            CompanyPaymentAccount.company_id == company_id,
            CompanyPaymentAccount.payment_method_key == key).first()
        if existing:
            existing.ca_financial_account_id = ca_financial_account_id
            existing.label = label
            db.add(existing)
        else:
            db.add(CompanyPaymentAccount(company_id=company_id, payment_method_key=key,
                                         ca_financial_account_id=ca_financial_account_id, label=label))
        db.commit()
        return {"ok": True, "company_id": company_id, "payment_method_key": key,
                "ca_financial_account_id": ca_financial_account_id, "label": label}
    finally:
        db.close()


@router.delete("/companies/{company_id}/payment-accounts/{payment_method_key}")
def delete_payment_account(company_id: int, payment_method_key: str):
    key = payment_method_key.strip().upper()
    db: Session = SessionLocal()
    try:
        mapping = db.query(CompanyPaymentAccount).filter(
            CompanyPaymentAccount.company_id == company_id,
            CompanyPaymentAccount.payment_method_key == key).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapeamento não encontrado")
        db.delete(mapping)
        db.commit()
        return {"ok": True, "deleted": key}
    finally:
        db.close()


# ── CENTRO DE CUSTO ─────────────────────────────────────────────────────────

@router.get("/companies/{company_id}/cost-centers")
def list_cost_centers(company_id: int):
    db: Session = SessionLocal()
    try:
        rows = db.query(CompanyCostCenter).filter(
            CompanyCostCenter.company_id == company_id).all()
        return [{"name_key": r.name_key, "label": r.label,
                 "ca_cost_center_id": r.ca_cost_center_id} for r in rows]
    finally:
        db.close()


@router.post("/companies/{company_id}/cost-centers")
def set_cost_center(
    company_id: int,
    name_key: str = Body(..., embed=True),
    ca_cost_center_id: str = Body(..., embed=True),
    label: Optional[str] = Body(None, embed=True),
):
    key = name_key.strip().upper()
    if not key:
        raise HTTPException(status_code=400, detail="name_key não pode ser vazio")
    if len(ca_cost_center_id.strip()) != 36:
        raise HTTPException(status_code=400, detail="ca_cost_center_id deve ser UUID com 36 caracteres")

    db: Session = SessionLocal()
    try:
        existing = db.query(CompanyCostCenter).filter(
            CompanyCostCenter.company_id == company_id,
            CompanyCostCenter.name_key == key).first()
        if existing:
            existing.ca_cost_center_id = ca_cost_center_id.strip()
            existing.label = label or key
            db.add(existing)
        else:
            db.add(CompanyCostCenter(
                company_id=company_id,
                name_key=key,
                label=label or key,
                ca_cost_center_id=ca_cost_center_id.strip(),
            ))
        db.commit()
        return {"ok": True, "name_key": key, "ca_cost_center_id": ca_cost_center_id.strip()}
    finally:
        db.close()


@router.delete("/companies/{company_id}/cost-centers/{name_key}")
def delete_cost_center(company_id: int, name_key: str):
    key = name_key.strip().upper()
    db: Session = SessionLocal()
    try:
        row = db.query(CompanyCostCenter).filter(
            CompanyCostCenter.company_id == company_id,
            CompanyCostCenter.name_key == key).first()
        if not row:
            raise HTTPException(status_code=404, detail="Mapeamento não encontrado")
        db.delete(row)
        db.commit()
        return {"ok": True, "deleted": key}
    finally:
        db.close()


# ── CATEGORIA FINANCEIRA ─────────────────────────────────────────────────────

@router.get("/companies/{company_id}/categories")
def list_categories(company_id: int):
    db: Session = SessionLocal()
    try:
        rows = db.query(CompanyCategory).filter(
            CompanyCategory.company_id == company_id).all()
        return [{"name_key": r.name_key, "label": r.label,
                 "ca_category_id": r.ca_category_id} for r in rows]
    finally:
        db.close()


@router.post("/companies/{company_id}/categories")
def set_category(
    company_id: int,
    name_key: str = Body(..., embed=True),
    ca_category_id: str = Body(..., embed=True),
    label: Optional[str] = Body(None, embed=True),
):
    key = name_key.strip().upper()
    if not key:
        raise HTTPException(status_code=400, detail="name_key não pode ser vazio")
    if len(ca_category_id.strip()) != 36:
        raise HTTPException(status_code=400, detail="ca_category_id deve ser UUID com 36 caracteres")

    db: Session = SessionLocal()
    try:
        existing = db.query(CompanyCategory).filter(
            CompanyCategory.company_id == company_id,
            CompanyCategory.name_key == key).first()
        if existing:
            existing.ca_category_id = ca_category_id.strip()
            existing.label = label or key
            db.add(existing)
        else:
            db.add(CompanyCategory(
                company_id=company_id,
                name_key=key,
                label=label or key,
                ca_category_id=ca_category_id.strip(),
            ))
        db.commit()
        return {"ok": True, "name_key": key, "ca_category_id": ca_category_id.strip()}
    finally:
        db.close()


@router.delete("/companies/{company_id}/categories/{name_key}")
def delete_category(company_id: int, name_key: str):
    key = name_key.strip().upper()
    db: Session = SessionLocal()
    try:
        row = db.query(CompanyCategory).filter(
            CompanyCategory.company_id == company_id,
            CompanyCategory.name_key == key).first()
        if not row:
            raise HTTPException(status_code=404, detail="Mapeamento não encontrado")
        db.delete(row)
        db.commit()
        return {"ok": True, "deleted": key}
    finally:
        db.close()
