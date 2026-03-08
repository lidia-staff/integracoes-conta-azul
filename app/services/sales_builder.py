import hashlib
from decimal import Decimal
from typing import List, Dict, Tuple

from app.db.models import Sale, SaleItem, Company
from app.services.validate import validate_item


def _to_decimal(v) -> Decimal:
    if v is None or v == "":
        return Decimal("0")
    return Decimal(str(v).replace(",", "."))


def _to_decimal_or_none(v):
    """Retorna Decimal se valor informado, None se vazio."""
    if v is None or str(v).strip() == "":
        return None
    try:
        return Decimal(str(v).replace(",", "."))
    except Exception:
        return None


def _to_str_or_none(v):
    """Retorna string limpa se informada, None se vazia."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _build_group_key(row: dict) -> str:
    """Chave de agrupamento padrão: agrupa vendas do mesmo cliente/data/pagamento/conta."""
    d = row["DATA ATENDIMENTO"].isoformat()
    venc = row["VENCIMENTO"].isoformat()
    cliente = str(row["CLIENTE / PACIENTE"]).strip()
    forma = str(row["FORMA DE PAGAMENTO"]).strip()
    cond = str(row["CONDICAO DE PAGAMENTO"]).strip()
    conta = str(row["CONTA DE RECEBIMENTO"]).strip()
    return f"{d}|{cliente}|{forma}|{cond}|{conta}|{venc}"


def _build_individual_key(row: dict, index: int) -> str:
    """Chave única por linha: garante que cada linha vira uma venda separada."""
    d = row["DATA ATENDIMENTO"].isoformat()
    cliente = str(row["CLIENTE / PACIENTE"]).strip()
    produto = str(row.get("PRODUTOS/SERVIÇOS") or "").strip()
    valor = str(row.get("VALOR UNITARIO") or "").strip()
    return f"individual|{index}|{d}|{cliente}|{produto}|{valor}"


def _hash_unique(group_key: str, items_signature: str) -> str:
    raw = f"{group_key}::{items_signature}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def create_sales_from_records(
    *,
    db,
    company_id: int,
    batch_id: int,
    records: List[Dict],
) -> Tuple[int, int, int, int, int]:
    """
    Cria Sales + SaleItems a partir dos records importados da planilha.

    Campos opcionais lidos da planilha (se coluna existir e estiver preenchida):
      - NUMERO_VENDA  → sale.sale_number   (senão usa get_next_sale_number() no envio ao CA)
      - DESCONTO      → sale.discount_amount em R$ (soma de todas as linhas do grupo)
      - CENTRO_CUSTO  → sale.cost_center_id UUID do CA (senão não envia centro de custo)

    group_mode da empresa:
      - "grouped"    → agrupa por cliente + data + pagamento + conta (padrão)
      - "individual" → cada linha = uma venda separada

    Retorna: (created, ready, awaiting, with_error, items_with_error)
    """
    company = db.query(Company).filter(Company.id == company_id).first()
    if not company:
        raise ValueError("company_not_found")

    mode = getattr(company, "group_mode", None) or "grouped"

    # ── AGRUPAMENTO ───────────────────────────────────────────────
    grouped: dict[str, list[dict]] = {}
    item_errors_count = 0

    for idx, row in enumerate(records):
        errs = validate_item(row)
        if errs:
            item_errors_count += 1

        if mode == "individual":
            gk = _build_individual_key(row, idx)
        else:
            gk = _build_group_key(row)

        grouped.setdefault(gk, []).append(row)

    # ── CRIAÇÃO DAS VENDAS ─────────────────────────────────────────
    created = ready = awaiting = with_error = 0

    for group_key, rows in grouped.items():
        sig_parts = []
        total = Decimal("0")
        has_error = False
        error_msgs = []

        for r in rows:
            errs = validate_item(r)
            if errs:
                has_error = True
                error_msgs.extend(errs)

            qty = _to_decimal(r.get("QUANTIDADE"))
            unit = _to_decimal(r.get("VALOR UNITARIO"))
            line_total = (qty * unit).quantize(Decimal("0.01"))
            total += line_total
            sig_parts.append(f"{r.get('PRODUTOS/SERVIÇOS')}|{qty}|{unit}")

        items_signature = "||".join(sig_parts)
        hash_unique = _hash_unique(group_key, items_signature)

        # evita duplicar se já existe igual no batch
        exists = (
            db.query(Sale)
            .filter(Sale.company_id == company_id, Sale.batch_id == batch_id, Sale.hash_unique == hash_unique)
            .first()
        )
        if exists:
            continue

        # Campos base (primeira linha do grupo)
        first = rows[0]
        sale_date = first["DATA ATENDIMENTO"]
        due_date = first["VENCIMENTO"]
        customer_name = first["CLIENTE / PACIENTE"]
        payment_method = first["FORMA DE PAGAMENTO"]
        payment_terms = first["CONDICAO DE PAGAMENTO"]
        receiving_account = first["CONTA DE RECEBIMENTO"]

        # Campos opcionais
        sale_number = _to_str_or_none(first.get("NUMERO_VENDA"))
        cost_center_id = _to_str_or_none(first.get("CENTRO_CUSTO"))

        # Desconto: soma de todas as linhas do grupo
        discount_total = Decimal("0")
        for r in rows:
            d = _to_decimal_or_none(r.get("DESCONTO"))
            if d is not None:
                discount_total += d
        discount_amount = discount_total if discount_total > 0 else None

        if has_error:
            status = "ERRO"
            error_summary = "; ".join(sorted(set(error_msgs)))[:1000]
            with_error += 1
        else:
            if company.review_mode:
                status = "AGUARDANDO_APROVACAO"
                awaiting += 1
            else:
                status = "PRONTA"
                ready += 1
            error_summary = None

        sale = Sale(
            company_id=company_id,
            batch_id=batch_id,
            group_key=group_key,
            hash_unique=hash_unique,
            sale_date=sale_date,
            customer_name=customer_name,
            payment_method=payment_method,
            payment_terms=payment_terms,
            receiving_account=receiving_account,
            due_date=due_date,
            total_amount=total,
            status=status,
            error_summary=error_summary,
            sale_number=sale_number,
            discount_amount=discount_amount,
            cost_center_id=cost_center_id,
        )
        db.add(sale)
        db.commit()
        db.refresh(sale)

        for r in rows:
            qty = _to_decimal(r.get("QUANTIDADE"))
            unit = _to_decimal(r.get("VALOR UNITARIO"))
            line_total = (qty * unit).quantize(Decimal("0.01"))
            item = SaleItem(
                sale_id=sale.id,
                category=(r.get("CATEGORIA") or None),
                product_service=str(r.get("PRODUTOS/SERVIÇOS") or "-"),
                details=(r.get("DETALHES DO ITEM") or None),
                qty=qty,
                unit_price=unit,
                line_total=line_total,
            )
            db.add(item)

        db.commit()
        created += 1

    return created, ready, awaiting, with_error, item_errors_count
