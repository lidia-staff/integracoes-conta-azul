from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db.models import Sale, SaleItem, Company, CompanyPaymentAccount, CompanyCostCenter, CompanyCategory
from app.services.conta_azul_client import ContaAzulClient
from app.services.contaazul_people import get_or_create_customer_uuid_cached
from app.services.contaazul_products import get_or_create_product_uuid_cached
from app.services.ca_sale_builder import build_ca_sale_payload
from app.services.ca_payload_builder import _normalize_payment_method

router = APIRouter(tags=["sales"])



def _get_sale_category(db, sale_id: int) -> str | None:
    """Busca a categoria do primeiro item da venda."""
    item = db.query(SaleItem).filter(SaleItem.sale_id == sale_id).first()
    if item and item.category:
        return item.category
    return None


def _get_financial_account_id(db: Session, company: Company, payment_method: str) -> str:
    """
    Resolve conta financeira por forma de pagamento.
    Prioridade: mapeamento específico → conta padrão → erro
    """
    tipo = _normalize_payment_method(payment_method)
    key_map = {
        "PIX_PAGAMENTO_INSTANTANEO": "PIX",
        "BOLETO_BANCARIO": "BOLETO",
        "CARTAO_CREDITO": "CARTAO_CREDITO",
        "CARTAO_DEBITO": "CARTAO_DEBITO",
        "TRANSFERENCIA_BANCARIA": "TRANSFERENCIA",
        "DINHEIRO": "DINHEIRO",
        "OUTRO": "OUTRO",
    }
    key = key_map.get(tipo, "OUTRO")

    mapping = (
        db.query(CompanyPaymentAccount)
        .filter(
            CompanyPaymentAccount.company_id == company.id,
            CompanyPaymentAccount.payment_method_key == key,
        )
        .first()
    )
    if mapping:
        return mapping.ca_financial_account_id

    if company.ca_financial_account_id:
        return company.ca_financial_account_id

    raise RuntimeError(
        f"Nenhuma conta financeira configurada para '{key}'. "
        f"Configure em POST /v1/companies/{company.id}/payment-accounts"
    )


def _resolve_product_uuids(
    db: Session,
    client: ContaAzulClient,
    company_id: int,
    items: list,
    item_type: str = "servico",
) -> dict:
    """
    Para cada item, resolve o UUID do produto/serviço no CA.
    Retorna dict {product_service_name → ca_product_uuid}.
    """
    uuid_map = {}
    for item in items:
        name = (item.product_service or "").strip()
        if name and name not in uuid_map:
            try:
                uuid_map[name] = get_or_create_product_uuid_cached(
                    db=db, client=client, company_id=company_id, product_name=name, item_type=item_type
                )
            except Exception as e:
                print(f"[ROUTES_SALES] Erro ao resolver produto '{name}': {e}")
    return uuid_map


def _resolve_cost_center_id(
    db,
    company_id: int,
    cost_center_raw: str | None,
) -> str | None:
    """
    Resolve nome do centro de custo (planilha) → UUID do CA via tabela de mapeamento.
    Retorna None se não informado ou mapeamento não encontrado (não bloqueia envio).
    """
    if not cost_center_raw:
        return None
    key = cost_center_raw.strip().upper()
    row = db.query(CompanyCostCenter).filter(
        CompanyCostCenter.company_id == company_id,
        CompanyCostCenter.name_key == key,
    ).first()
    if row:
        return row.ca_cost_center_id
    print(f"[SALES] Centro de custo '{key}' sem mapeamento configurado — omitindo do payload")
    return None


def _resolve_category_id(db, company_id: int, category_raw: str | None) -> str | None:
    """
    Resolve nome da categoria (planilha) → UUID do CA via tabela de mapeamento.
    Retorna None se não informado ou mapeamento não encontrado.
    """
    if not category_raw or str(category_raw).strip() in ("", "nan", "None"):
        return None
    key = category_raw.strip().upper()
    row = db.query(CompanyCategory).filter(
        CompanyCategory.company_id == company_id,
        CompanyCategory.name_key == key,
    ).first()
    if row:
        return row.ca_category_id
    print(f"[SALES] Categoria '{key}' sem mapeamento configurado — omitindo do payload")
    return None


def _get_numero(sale, client) -> int:
    """
    Prioridade: número da planilha (sale.sale_number) → próximo número do CA.
    """
    numero = getattr(sale, "sale_number", None)
    if numero:
        try:
            return int(numero)
        except Exception:
            print(f"[SALES] sale_number '{numero}' não é inteiro válido — usando próximo do CA")
    return client.get_next_sale_number()


@router.get("/sales")
def list_sales(company_id: int | None = None, batch_id: int | None = None, status: str | None = None):
    db: Session = SessionLocal()
    try:
        q = db.query(Sale)
        if company_id is not None:
            q = q.filter(Sale.company_id == company_id)
        if batch_id is not None:
            q = q.filter(Sale.batch_id == batch_id)
        if status is not None:
            q = q.filter(Sale.status == status)
        return q.order_by(Sale.id.asc()).all()
    finally:
        db.close()


@router.get("/sales/{sale_id}")
def get_sale(sale_id: int):
    db: Session = SessionLocal()
    try:
        s = db.query(Sale).filter(Sale.id == sale_id).first()
        if not s:
            raise HTTPException(status_code=404, detail="Sale não encontrada")
        items = db.query(SaleItem).filter(SaleItem.sale_id == sale_id).all()
        return {"sale": s, "items": items}
    finally:
        db.close()


@router.post("/sales/{sale_id}/send_to_ca")
def send_to_ca(sale_id: int):
    db: Session = SessionLocal()
    try:
        sale = db.query(Sale).filter(Sale.id == sale_id).first()
        if not sale:
            raise HTTPException(status_code=404, detail="Sale não encontrada")

        company = db.query(Company).filter(Company.id == sale.company_id).first()
        if not company:
            raise HTTPException(status_code=400, detail="Company não encontrada")

        items = db.query(SaleItem).filter(SaleItem.sale_id == sale.id).all()
        if not items:
            raise HTTPException(status_code=400, detail="Sale sem itens")

        client = ContaAzulClient(company_id=company.id)

        customer_uuid = get_or_create_customer_uuid_cached(
            db=db, client=client, company_id=company.id, customer_name=sale.customer_name
        )

        # Resolve UUIDs dos produtos
        item_type = getattr(company, "item_type", None) or "servico"
        product_uuid_map = _resolve_product_uuids(db, client, company.id, items, item_type=item_type)

        # Número da venda: prioriza o da planilha, senão usa próximo do CA
        numero = _get_numero(sale, client)

        financial_account_id = _get_financial_account_id(db, company, sale.payment_method)

        # Resolve centro de custo: texto → UUID
        raw_cc = getattr(sale, "cost_center_id", None)
        sale.cost_center_id = _resolve_cost_center_id(db, company.id, raw_cc)

        # Resolve categoria financeira: texto → UUID
        raw_cat = getattr(sale, "_category_raw", None) or _get_sale_category(db, sale.id)
        sale._ca_category_id = _resolve_category_id(db, company.id, raw_cat)

        # Corrige NaN do pandas no desconto
        discount = getattr(sale, "discount_amount", None)
        if discount is not None:
            try:
                import math
                if math.isnan(float(discount)):
                    sale.discount_amount = None
            except Exception:
                pass

        # Injeta status configurável na venda antes de montar payload
        sale._ca_sale_status = getattr(company, "ca_sale_status", None) or "EM_ANDAMENTO"

        payload = build_ca_sale_payload(
            id_cliente=customer_uuid,
            numero=numero,
            sale=sale,
            items=items,
            id_conta_financeira=financial_account_id,
            product_uuid_map=product_uuid_map,
        )

        resp = client.create_sale(payload)

        sale.ca_sale_id = resp.get("id") or sale.ca_sale_id
        sale.status = "ENVIADA_CA"
        sale.error_summary = None
        db.add(sale)
        db.commit()
        db.refresh(sale)

        return {"ok": True, "sale_id": sale.id, "ca_response": resp}

    except HTTPException:
        raise
    except Exception as e:
        try:
            s2 = db.query(Sale).filter(Sale.id == sale_id).first()
            if s2:
                s2.status = "ERRO_ENVIO_CA"
                s2.error_summary = str(e)[:1000]
                db.add(s2)
                db.commit()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()


@router.post("/batches/{batch_id}/send_to_ca")
def send_batch_to_ca(batch_id: int):
    """Envia todas as vendas PRONTAS do batch para o Conta Azul."""
    db: Session = SessionLocal()
    try:
        sales = (
            db.query(Sale)
            .filter(Sale.batch_id == batch_id)
            .filter(Sale.status.in_(["PRONTA", "PRONTA_PARA_ENVIO"]))
            .all()
        )

        if not sales:
            return {
                "batch_id": batch_id, "total_sales": 0, "sent": 0,
                "errors": 0, "skipped": 0,
                "message": "Nenhuma venda PRONTA encontrada", "results": [],
            }

        company_ids = list(set([s.company_id for s in sales]))
        if len(company_ids) > 1:
            raise HTTPException(status_code=400, detail=f"Batch com múltiplas companies: {company_ids}")

        company_id = company_ids[0]
        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company não encontrada")

        client = ContaAzulClient(company_id=company_id)
        ca_sale_status = getattr(company, "ca_sale_status", None) or "EM_ANDAMENTO"
        item_type = getattr(company, "item_type", None) or "servico"

        sent = errors = 0
        results = []

        for sale in sales:
            result = {
                "sale_id": sale.id,
                "customer_name": sale.customer_name,
                "total_amount": float(sale.total_amount),
                "status": None,
                "error": None,
                "ca_sale_id": None,
            }
            try:
                items = db.query(SaleItem).filter(SaleItem.sale_id == sale.id).all()
                if not items:
                    raise RuntimeError("Venda sem itens")

                customer_uuid = get_or_create_customer_uuid_cached(
                    db=db, client=client, company_id=company_id, customer_name=sale.customer_name
                )

                # Resolve UUIDs dos produtos para esta venda
                product_uuid_map = _resolve_product_uuids(db, client, company_id, items, item_type=item_type)

                # Número da venda: prioriza o da planilha, senão usa próximo do CA
                numero = _get_numero(sale, client)

                financial_account_id = _get_financial_account_id(db, company, sale.payment_method)

                # Resolve centro de custo: texto → UUID
                raw_cc = getattr(sale, "cost_center_id", None)
                sale.cost_center_id = _resolve_cost_center_id(db, company_id, raw_cc)

                # Resolve categoria financeira: texto → UUID
                raw_cat = _get_sale_category(db, sale.id)
                sale._ca_category_id = _resolve_category_id(db, company_id, raw_cat)

                # Corrige NaN do pandas no desconto
                discount = getattr(sale, "discount_amount", None)
                if discount is not None:
                    try:
                        import math
                        if math.isnan(float(discount)):
                            sale.discount_amount = None
                    except Exception:
                        pass

                # Injeta status configurável
                sale._ca_sale_status = ca_sale_status

                payload = build_ca_sale_payload(
                    id_cliente=customer_uuid,
                    numero=numero,
                    sale=sale,
                    items=items,
                    id_conta_financeira=financial_account_id,
                    product_uuid_map=product_uuid_map,
                )

                resp = client.create_sale(payload)

                sale.ca_sale_id = resp.get("id") or sale.ca_sale_id
                sale.status = "ENVIADA_CA"
                sale.error_summary = None
                db.add(sale)
                db.commit()

                result["status"] = "success"
                result["ca_sale_id"] = sale.ca_sale_id
                sent += 1

            except Exception as e:
                error_msg = str(e)[:1000]
                sale.status = "ERRO_ENVIO_CA"
                sale.error_summary = error_msg
                db.add(sale)
                db.commit()
                result["status"] = "error"
                result["error"] = error_msg
                errors += 1

            results.append(result)

        return {
            "batch_id": batch_id, "company_id": company_id,
            "total_sales": len(sales), "sent": sent,
            "errors": errors, "skipped": 0, "results": results,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no envio em lote: {str(e)}")
    finally:
        db.close()


@router.delete("/batches/{batch_id}")
def delete_batch(batch_id: int):
    """Exclui lote e vendas. Bloqueado se houver vendas já enviadas ao CA."""
    db: Session = SessionLocal()
    try:
        from app.db.models import UploadBatch
        batch = db.query(UploadBatch).filter(UploadBatch.id == batch_id).first()
        if not batch:
            raise HTTPException(status_code=404, detail="Lote não encontrado")

        sales = db.query(Sale).filter(Sale.batch_id == batch_id).all()
        enviadas = [s for s in sales if s.status == "ENVIADA_CA"]
        if enviadas:
            raise HTTPException(
                status_code=400,
                detail=f"Lote contém {len(enviadas)} venda(s) já enviada(s) ao Conta Azul e não pode ser excluído."
            )

        total = len(sales)
        for sale in sales:
            db.query(SaleItem).filter(SaleItem.sale_id == sale.id).delete()
            db.delete(sale)

        db.delete(batch)
        db.commit()

        return {"ok": True, "batch_id": batch_id, "sales_deleted": total}
    finally:
        db.close()


@router.post("/sales/{sale_id}/approve")
def approve_sale(sale_id: int):
    db: Session = SessionLocal()
    try:
        sale = db.query(Sale).filter(Sale.id == sale_id).first()
        if not sale:
            raise HTTPException(status_code=404, detail="Sale não encontrada")
        if sale.status != "AGUARDANDO_APROVACAO":
            raise HTTPException(status_code=400, detail=f"Sale não aguarda aprovação (status: {sale.status})")
        sale.status = "PRONTA"
        db.add(sale)
        db.commit()
        db.refresh(sale)
        return {"ok": True, "sale_id": sale.id, "new_status": sale.status}
    finally:
        db.close()


@router.post("/batches/{batch_id}/approve")
def approve_batch(batch_id: int):
    db: Session = SessionLocal()
    try:
        sales = (
            db.query(Sale)
            .filter(Sale.batch_id == batch_id)
            .filter(Sale.status == "AGUARDANDO_APROVACAO")
            .all()
        )
        for sale in sales:
            sale.status = "PRONTA"
            db.add(sale)
        db.commit()
        return {"ok": True, "batch_id": batch_id, "approved": len(sales)}
    finally:
        db.close()
