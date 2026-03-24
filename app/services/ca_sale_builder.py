from app.services.ca_payload_builder import build_ca_payload


def build_ca_sale_payload(
    id_cliente: str,
    numero: str | int,
    sale,
    items: list,
    id_conta_financeira: str | None = None,
) -> dict:
    """
    Monta payload final para criar venda no Conta Azul.
    - build_ca_payload(sale) monta o corpo base (itens, datas, condição etc.)
    - injeta id_cliente e numero
    - injeta id_conta_financeira (UUID 36 chars) dentro de condicao_pagamento
    """
    try:
        sale.items = items
    except Exception:
        pass

    payload = build_ca_payload(sale)

    payload["id_cliente"] = id_cliente
    payload["numero"] = int(numero)  # CA exige int64, não string

    if id_conta_financeira:
        payload.setdefault("condicao_pagamento", {})
        if not isinstance(payload["condicao_pagamento"], dict):
            payload["condicao_pagamento"] = {}
        payload["condicao_pagamento"]["id_conta_financeira"] = id_conta_financeira

    return payload
