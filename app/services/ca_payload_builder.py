from datetime import date
from typing import Dict


def _normalize_payment_method(raw: str) -> str:
    s = (raw or "").strip().upper()
    if "PIX" in s:
        return "PIX_PAGAMENTO_INSTANTANEO"
    if "BOLETO" in s:
        return "BOLETO_BANCARIO"
    if "CRÉDITO" in s or "CREDITO" in s:
        return "CARTAO_CREDITO"
    if "DÉBITO" in s or "DEBITO" in s:
        return "CARTAO_DEBITO"
    if "TRANSFER" in s:
        return "TRANSFERENCIA_BANCARIA"
    if "DINHEIRO" in s:
        return "DINHEIRO"
    return "OUTRO"


def _parcelas_qtd(payment_terms: str) -> int:
    t = (payment_terms or "").strip().upper()
    if "À VISTA" in t or "A VISTA" in t:
        return 1
    digits = "".join([c for c in t if c.isdigit()])
    if digits:
        try:
            return max(1, int(digits))
        except Exception:
            return 1
    return 1


def _build_parcelas(total: float, due_date: date, parcelas: int = 1):
    valor_parcela = round(total / parcelas, 2)
    return [{"data_vencimento": str(due_date), "valor": valor_parcela} for _ in range(parcelas)]


def _build_itens(sale, product_uuid_map: dict | None = None) -> list:
    """
    Monta lista de itens para o payload do CA.

    product_uuid_map: dict {product_service_name → ca_product_uuid}
    Se fornecido, injeta o id do produto no item.
    O campo 'descricao' recebe o valor de item.details (DETALHES DO ITEM da planilha).
    """
    itens = []
    for i in sale.items:
        item = {
            "descricao": (i.details or i.product_service),  # DETALHES DO ITEM ou fallback
            "quantidade": float(i.qty),
            "valor": float(i.unit_price),
        }
        # Injeta UUID do produto se disponível
        if product_uuid_map:
            ca_id = product_uuid_map.get(i.product_service)
            if ca_id:
                item["id"] = ca_id
        itens.append(item)
    return itens


def build_ca_payload(sale, product_uuid_map: dict | None = None) -> Dict:
    tipo_pagamento = _normalize_payment_method(sale.payment_method)
    n_parcelas = _parcelas_qtd(sale.payment_terms or "")

    # Status da venda: usa o da empresa se disponível, senão EM_ANDAMENTO
    situacao = getattr(sale, "_ca_sale_status", None) or "EM_ANDAMENTO"

    payload = {
        "situacao": situacao,
        "data_venda": str(sale.sale_date),
        "observacoes": "Venda importada automaticamente.",
        "itens": _build_itens(sale, product_uuid_map),
        "condicao_pagamento": {
            "tipo_pagamento": tipo_pagamento,
            "opcao_condicao_pagamento": "À vista" if n_parcelas == 1 else f"{n_parcelas}x",
            "parcelas": _build_parcelas(
                total=float(sale.total_amount),
                due_date=sale.due_date,
                parcelas=n_parcelas,
            ),
        },
    }

    # Categoria financeira — só inclui se UUID resolvido
    category_id = getattr(sale, "_ca_category_id", None)
    if category_id:
        payload["id_categoria"] = category_id

    # Desconto em R$ — só inclui se informado e maior que zero
    import math
    from decimal import Decimal as _Decimal
    discount = getattr(sale, "discount_amount", None)
    print(f"[PAYLOAD] discount_amount raw: {discount!r} type={type(discount).__name__}")
    if discount is not None:
        try:
            dval = float(str(discount))  # str() garante conversão correta de Decimal ORM
            print(f"[PAYLOAD] discount float: {dval}")
            if not math.isnan(dval) and dval > 0:
                payload["composicao_de_valor"] = {
                    "desconto": {
                        "tipo": "VALOR",
                        "valor": dval
                    }
                }
                print(f"[PAYLOAD] desconto incluído no payload (composicao_de_valor): {dval}")
        except Exception as e:
            print(f"[PAYLOAD] erro ao converter desconto: {e}")

    # Centro de custo — só inclui se informado
    cost_center = getattr(sale, "cost_center_id", None)
    if cost_center:
        payload["id_centro_custo"] = cost_center

    print(f"[PAYLOAD] payload completo enviado ao CA: {payload}")
    return payload
