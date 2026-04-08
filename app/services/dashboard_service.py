"""
Dashboard Service — lógica de negócio do DRE.

Responsabilidades:
- Mapeamento entrada_dre (CA) → coluna DRE
- Cálculo de campos derivados (Lucro Bruto, EBITDA, etc.)
- Cálculo de KPIs (Markup, % margens)
- Formatação do snapshot_month para exibição (ex: "2025-03" → "MAR25")
"""

from __future__ import annotations
import json
import calendar
from datetime import date

# ── Mapeamento entrada_dre → coluna DRE ─────────────────────────────
# O campo entrada_dre vem de cada categoria financeira no CA.
# Múltiplos valores de entrada_dre podem mapear para a mesma coluna DRE.

ENTRADA_DRE_MAP: dict[str, str] = {
    # Receitas
    "RECEITAS_OPERACIONAIS":       "FATURAMENTO_BRUTO",
    "RECEITA_OPERACIONAL":         "FATURAMENTO_BRUTO",
    "FATURAMENTO_BRUTO":           "FATURAMENTO_BRUTO",
    "VENDAS":                      "FATURAMENTO_BRUTO",
    "SERVICOS_PRESTADOS":          "FATURAMENTO_BRUTO",
    # Impostos
    "IMPOSTOS":                    "IMPOSTOS",
    "TRIBUTOS":                    "IMPOSTOS",
    "IMPOSTOS_SOBRE_VENDAS":       "IMPOSTOS",
    # Custos
    "CUSTO_SERVICO_PRESTADO":      "CUSTOS_TOTAIS",
    "CUSTO_MERCADORIA_VENDIDA":    "CUSTOS_TOTAIS",
    "CUSTOS_VARIAVEIS":            "CUSTOS_TOTAIS",
    "CUSTOS_DIRETOS":              "CUSTOS_TOTAIS",
    "CUSTOS_SERVICOS":             "CUSTOS_TOTAIS",
    # Pessoal
    "DESPESAS_PESSOAL":            "DEPTO_PESSOAL",
    "FOLHA_PAGAMENTO":             "DEPTO_PESSOAL",
    "SALARIOS":                    "DEPTO_PESSOAL",
    "PRO_LABORE":                  "DEPTO_PESSOAL",
    # Administrativas
    "DESPESAS_ADMINISTRATIVAS":    "ADMINISTRATIVAS",
    "DESPESAS_GERAIS":             "ADMINISTRATIVAS",
    "ADMINISTRATIVAS":             "ADMINISTRATIVAS",
    # Comerciais
    "DESPESAS_COMERCIAIS":         "COMERCIAIS",
    "VENDAS_COMISSOES":            "COMERCIAIS",
    "COMERCIAIS":                  "COMERCIAIS",
    # Marketing
    "DESPESAS_MARKETING":          "MARKETING",
    "MARKETING":                   "MARKETING",
    "PUBLICIDADE":                 "MARKETING",
    # Imóvel / aluguel
    "DESPESAS_IMOVEL":             "IMOVEL",
    "ALUGUEIS":                    "IMOVEL",
    "IMOVEL":                      "IMOVEL",
    "ALUGUEL":                     "IMOVEL",
    # Resultado financeiro
    "RECEITAS_FINANCEIRAS":        "RECEITAS_FINANCEIRAS",
    "JUROS_RECEBIDOS":             "RECEITAS_FINANCEIRAS",
    "DESPESAS_FINANCEIRAS":        "DESPESAS_FINANCEIRAS",
    "JUROS_PAGOS":                 "DESPESAS_FINANCEIRAS",
    "ENCARGOS_FINANCEIROS":        "DESPESAS_FINANCEIRAS",
    # Outras
    "OUTRAS_RECEITAS":             "OUTRAS_RECEITAS",
    "OUTRAS_DESPESAS":             "OUTRAS_DESPESAS",
    # Investimentos
    "INVESTIMENTOS":               "INVESTIMENTOS",
    "CAPEX":                       "INVESTIMENTOS",
    # Retirada de sócio
    "DISTRIBUICAO_LUCROS":         "RETIRADA_SOCIO",
    "RETIRADA_SOCIO":              "RETIRADA_SOCIO",
    "RETIRADA_SOCIOS":             "RETIRADA_SOCIO",
    "PRO_LABORE_SOCIO":            "RETIRADA_SOCIO",
}

# Campos DRE calculados (derivados, não acumulados diretamente)
CALCULATED_FIELDS = {
    "FATURAMENTO_LIQUIDO",
    "LUCRO_BRUTO",
    "LUCRO_OPERACIONAL_EBITDA",
    "LUCRO_LIQUIDO",
    "LUCRO_REMANESCENTE",
}

# Todos os campos DRE na ordem de exibição
DRE_FIELDS = [
    "FATURAMENTO_BRUTO",
    "IMPOSTOS",
    "FATURAMENTO_LIQUIDO",      # calculado
    "CUSTOS_TOTAIS",
    "LUCRO_BRUTO",              # calculado
    "DEPTO_PESSOAL",
    "ADMINISTRATIVAS",
    "COMERCIAIS",
    "MARKETING",
    "IMOVEL",
    "LUCRO_OPERACIONAL_EBITDA", # calculado
    "RECEITAS_FINANCEIRAS",
    "OUTRAS_RECEITAS",
    "DESPESAS_FINANCEIRAS",
    "OUTRAS_DESPESAS",
    "INVESTIMENTOS",
    "LUCRO_LIQUIDO",            # calculado
    "RETIRADA_SOCIO",
    "LUCRO_REMANESCENTE",       # calculado
]

_MONTH_LABELS = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN",
                 "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]


def snapshot_month_label(snapshot_month: str) -> str:
    """Converte "2025-03" → "MAR25"."""
    year, month = snapshot_month.split("-")
    return _MONTH_LABELS[int(month) - 1] + year[2:]


def month_date_range(snapshot_month: str) -> tuple[str, str]:
    """Retorna (primeiro_dia, ultimo_dia) no formato YYYY-MM-DD."""
    year, month = int(snapshot_month[:4]), int(snapshot_month[5:7])
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def _empty_dre() -> dict:
    return {f: 0.0 for f in DRE_FIELDS if f not in CALCULATED_FIELDS}


def _nome_to_dre_field(nome: str, tipo_tx: str) -> str | None:
    """
    Fallback: mapeia nome da categoria para campo DRE por palavras-chave.
    Usado quando entrada_dre_raw está vazio (CA API v2 não tem esse campo).
    """
    n = (nome or "").lower()

    if tipo_tx == "Receita":
        # Impostos sobre receita ficam em IMPOSTOS
        if any(k in n for k in ["imposto", "tributo", "iss ", "das ", "simples", "cofins", "pis/", "irpj", "csll"]):
            return "IMPOSTOS"
        if any(k in n for k in ["juros recebido", "rendimento", "aplicação", "financeiro"]):
            return "RECEITAS_FINANCEIRAS"
        # Toda receita operacional → FATURAMENTO_BRUTO
        return "FATURAMENTO_BRUTO"

    # Despesas
    if any(k in n for k in ["salário", "salario", "folha", "funcionário", "funcionario",
                              "férias", "ferias", "13º", "fgts", "inss", "clt", "pessoal",
                              "benefício", "beneficio", "vale", "pró-labore", "pro-labore",
                              "pro labore"]):
        return "DEPTO_PESSOAL"
    if any(k in n for k in ["retirada", "distribuição de lucro", "distribuicao de lucro",
                              "distribuição sócio", "socio", "sócio"]):
        return "RETIRADA_SOCIO"
    if any(k in n for k in ["aluguel", "imóvel", "imovel", "locação", "locacao",
                              "condomínio", "condominio", "iptu", "arrendamento"]):
        return "IMOVEL"
    if any(k in n for k in ["marketing", "publicidade", "propaganda", "mídia", "midia",
                              "anúncio", "anuncio", "google ads", "facebook", "instagram ads",
                              "influencer", "panfleto", "impulsionamento"]):
        return "MARKETING"
    if any(k in n for k in ["imposto", "tributo", "iss", "das ", "simples nacional",
                              "cofins", "pis", "irpj", "csll", "iof", "icms", "darf"]):
        return "IMPOSTOS"
    if any(k in n for k in ["juros", "tarifa bancária", "tarifa bancaria", "iof",
                              "multa financeira", "encargo", "banco ", "cartão banco",
                              "taxa bancária", "taxa bancaria", "anuidade"]):
        return "DESPESAS_FINANCEIRAS"
    if any(k in n for k in ["investimento", "equipamento", "máquina", "maquina",
                              "imobilizado", "capex", "compra de ativo"]):
        return "INVESTIMENTOS"
    if any(k in n for k in ["custo produto", "mercadoria", "estoque", "matéria-prima",
                              "materia-prima", "insumo", "produto revendido"]):
        return "CUSTOS_TOTAIS"
    if any(k in n for k in ["comissão", "comissao", "vendedor", "representante"]):
        return "COMERCIAIS"
    if any(k in n for k in ["administrativ", "escritório", "escritorio", "material",
                              "papelaria", "telefone", "internet", "água", "agua",
                              "energia elétrica", "energia eletrica", "luz ", "contador",
                              "contabilidade", "software", "assinatura", "serviço",
                              "servico", "manutenção", "manutencao", "limpeza",
                              "segurança", "seguranca", "seguro", "plano"]):
        return "ADMINISTRATIVAS"
    # Despesas sem classificação clara → OUTRAS_DESPESAS
    return "OUTRAS_DESPESAS"


def aggregate_transactions(
    transactions: list[dict],
    category_map: dict[str, str],   # ca_category_id → entrada_dre
    ignored_accounts: list[str],
    ignored_categories: list[str],
) -> tuple[dict, dict[str, list]]:
    """
    Agrega transações em colunas DRE.

    Retorna:
      - totais: {campo_DRE: valor_total}
      - subcats: {campo_DRE: [{categoria, valor}]}
    """
    totals: dict[str, float] = _empty_dre()
    subcats: dict[str, list] = {f: [] for f in totals}

    for tx in transactions:
        # Filtra contas ignoradas
        account_id = str(tx.get("conta_financeira_id") or tx.get("conta_id") or "")
        if account_id in ignored_accounts:
            continue

        # Resolve categoria
        cat_id = str(tx.get("categoria_id") or "")
        if cat_id in ignored_categories:
            continue

        # 1ª tentativa: entrada_dre_raw embutido (legado / CA v1)
        entrada_dre_raw = tx.get("entrada_dre_raw") or category_map.get(cat_id, "")
        dre_field = ENTRADA_DRE_MAP.get(entrada_dre_raw.upper()) if entrada_dre_raw else None

        # 2ª tentativa: mapeia por nome da categoria (CA API v2 não tem entrada_dre)
        if not dre_field:
            cat_name = tx.get("categoria_nome") or tx.get("descricao") or ""
            tipo_tx = tx.get("tipo", "Despesa")
            dre_field = _nome_to_dre_field(cat_name, tipo_tx)

        if not dre_field:
            continue

        valor = float(tx.get("valor") or 0)
        cat_name = tx.get("categoria_nome") or tx.get("descricao") or cat_id

        totals[dre_field] = totals.get(dre_field, 0.0) + valor
        subcats.setdefault(dre_field, []).append({"categoria": cat_name, "valor": valor})

    return totals, subcats


def calculate_derived(totals: dict) -> dict:
    """Calcula campos derivados e retorna dicionário DRE completo."""
    t = dict(totals)

    t["FATURAMENTO_LIQUIDO"] = t.get("FATURAMENTO_BRUTO", 0) - t.get("IMPOSTOS", 0)
    t["LUCRO_BRUTO"] = t["FATURAMENTO_LIQUIDO"] - t.get("CUSTOS_TOTAIS", 0)
    t["LUCRO_OPERACIONAL_EBITDA"] = (
        t["LUCRO_BRUTO"]
        - t.get("DEPTO_PESSOAL", 0)
        - t.get("ADMINISTRATIVAS", 0)
        - t.get("COMERCIAIS", 0)
        - t.get("MARKETING", 0)
        - t.get("IMOVEL", 0)
    )
    t["LUCRO_LIQUIDO"] = (
        t["LUCRO_OPERACIONAL_EBITDA"]
        + t.get("RECEITAS_FINANCEIRAS", 0)
        + t.get("OUTRAS_RECEITAS", 0)
        - t.get("DESPESAS_FINANCEIRAS", 0)
        - t.get("OUTRAS_DESPESAS", 0)
        - t.get("INVESTIMENTOS", 0)
    )
    t["LUCRO_REMANESCENTE"] = t["LUCRO_LIQUIDO"] - t.get("RETIRADA_SOCIO", 0)
    return t


def calculate_kpis(dre: dict, benchmarks: dict) -> dict:
    """Calcula KPIs e percentuais para os cards do dashboard."""
    fat = dre.get("FATURAMENTO_BRUTO", 0) or 1  # evita divisão por zero
    ll = dre.get("LUCRO_LIQUIDO", 0)
    lb = dre.get("LUCRO_BRUTO", 0)
    lop = dre.get("LUCRO_OPERACIONAL_EBITDA", 0)
    custos = dre.get("CUSTOS_TOTAIS", 0) or 1

    return {
        "pct_ll":    round(ll / fat * 100, 1),
        "pct_lb":    round(lb / fat * 100, 1),
        "pct_ebitda": round(lop / fat * 100, 1),
        "markup":    round((fat / custos - 1) * 100, 1),
        "pct_imp":   round(dre.get("IMPOSTOS", 0) / fat * 100, 1),
        "pct_cus":   round(dre.get("CUSTOS_TOTAIS", 0) / fat * 100, 1),
        "pct_pes":   round(dre.get("DEPTO_PESSOAL", 0) / fat * 100, 1),
        "pct_adm":   round(dre.get("ADMINISTRATIVAS", 0) / fat * 100, 1),
        "pct_com":   round(dre.get("COMERCIAIS", 0) / fat * 100, 1),
        "pct_mkt":   round(dre.get("MARKETING", 0) / fat * 100, 1),
        "pct_imo":   round(dre.get("IMOVEL", 0) / fat * 100, 1),
        "benchmarks": benchmarks,
    }


def build_snapshot_data(
    snapshot_month: str,
    transactions: list[dict],
    category_map: dict[str, str],
    ignored_accounts: list[str],
    ignored_categories: list[str],
    benchmarks: dict,
) -> dict:
    """
    Pipeline completo: transações → snapshot JSON de um mês.
    Retorna o dict pronto para salvar em DashSnapshot.data_json.
    """
    totals, subcats = aggregate_transactions(
        transactions, category_map, ignored_accounts, ignored_categories
    )
    dre = calculate_derived(totals)
    kpis = calculate_kpis(dre, benchmarks)

    return {
        "MES": snapshot_month_label(snapshot_month),
        "snapshot_month": snapshot_month,
        **dre,
        "subcats": subcats,
        "kpis": kpis,
    }


def build_category_map(categories: list[dict]) -> dict[str, str]:
    """
    Constrói mapa {ca_category_id → entrada_dre} a partir da lista
    retornada por ContaAzulClient.list_categories_dre().
    """
    result = {}
    for cat in categories:
        cat_id = str(cat.get("id") or "")
        entrada = str(cat.get("entrada_dre") or cat.get("entradaDre") or "").upper()
        if cat_id and entrada:
            result[cat_id] = entrada
    return result
