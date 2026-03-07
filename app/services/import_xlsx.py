import pandas as pd
import unicodedata
from typing import List, Dict


# 🔒 Colunas CANÔNICAS obrigatórias
CANONICAL_COLUMNS = [
    "DATA ATENDIMENTO",
    "CLIENTE / PACIENTE",
    "CATEGORIA",
    "PRODUTOS/SERVIÇOS",
    "DETALHES DO ITEM",
    "QUANTIDADE",
    "VALOR UNITARIO",
    "FORMA DE PAGAMENTO",
    "CONTA DE RECEBIMENTO",
    "CONDICAO DE PAGAMENTO",
    "VENCIMENTO",
]

# 🔓 Colunas opcionais — lidas se presentes, ignoradas se ausentes
OPTIONAL_COLUMNS = [
    "NUMERO_VENDA",   # Número da venda (senão usa automático do CA)
    "DESCONTO",       # Desconto em R$ por venda
    "CENTRO_CUSTO",   # UUID do centro de custo no CA
]

# ✅ Aliases aceitos por coluna
ALIASES = {
    "DATA ATENDIMENTO": ["DATA ATENDIMENTO", "DATA", "DATA DA VENDA", "DATA VENDA"],
    "CLIENTE / PACIENTE": ["CLIENTE / PACIENTE", "CLIENTE", "PACIENTE", "NOME", "NOME DO CLIENTE"],
    "CATEGORIA": ["CATEGORIA", "CATEGORIAS"],
    "PRODUTOS/SERVIÇOS": ["PRODUTOS/SERVIÇOS", "PRODUTOS", "SERVICOS", "SERVIÇOS", "PRODUTOS SERVICOS", "PRODUTOS SERVIÇOS"],
    "DETALHES DO ITEM": ["DETALHES DO ITEM", "DETALHES", "OBS", "OBSERVACAO", "OBSERVAÇÃO"],
    "QUANTIDADE": ["QUANTIDADE", "QTD", "QTDE"],
    "VALOR UNITARIO": ["VALOR UNITARIO", "VALOR UNITÁRIO", "VALOR", "PRECO", "PREÇO", "VALOR UN"],
    "FORMA DE PAGAMENTO": ["FORMA DE PAGAMENTO", "PAGAMENTO", "MEIO DE PAGAMENTO"],
    "CONTA DE RECEBIMENTO": ["CONTA DE RECEBIMENTO", "CONTA", "CONTA RECEBIMENTO"],
    "CONDICAO DE PAGAMENTO": ["CONDICAO DE PAGAMENTO", "CONDIÇÃO DE PAGAMENTO", "CONDICAO", "CONDIÇÃO", "PARCELAS"],
    "VENCIMENTO": ["VENCIMENTO", "DATA VENCIMENTO", "VENC", "DUE DATE"],
    # Opcionais
    "NUMERO_VENDA": ["NUMERO_VENDA", "NUMERO VENDA", "N DA VENDA", "NUM VENDA", "NO DA VENDA"],
    "DESCONTO": ["DESCONTO", "DESCTO", "DESC"],
    "CENTRO_CUSTO": ["CENTRO_CUSTO", "CENTRO CUSTO", "CENTRO DE CUSTO"],
}


def normalize_col(col: str) -> str:
    col = str(col).strip()
    col = unicodedata.normalize("NFKD", col).encode("ASCII", "ignore").decode("ASCII")
    col = col.upper()  # upper APÓS normalize para tratar casos como Nº → No → NO
    col = col.replace("/", " ").replace("-", " ").replace("_", " ")
    col = " ".join(col.split())
    return col


def _find_source_column(df_columns: List[str], canonical: str) -> str | None:
    norm_map = {normalize_col(c): c for c in df_columns}
    for alias in ALIASES.get(canonical, [canonical]):
        alias_norm = normalize_col(alias)
        if alias_norm in norm_map:
            return norm_map[alias_norm]
    return None


def read_base_sheet(file_path: str, sheet_name: str = "Base") -> List[Dict]:
    """
    Lê a planilha e devolve registros com colunas canônicas + opcionais presentes.
    Colunas obrigatórias ausentes geram erro.
    Colunas opcionais ausentes são ignoradas (campo fica None no registro).
    """
    xls = pd.ExcelFile(file_path, engine="openpyxl")

    if sheet_name not in xls.sheet_names:
        print(f"[IMPORT] Aba '{sheet_name}' não encontrada. Abas: {xls.sheet_names}")
        sheet_name = xls.sheet_names[0]

    df = pd.read_excel(xls, sheet_name=sheet_name)
    print(f"[IMPORT] Usando aba: {sheet_name}")
    print(f"[IMPORT] Colunas originais: {list(df.columns)}")
    print(f"[IMPORT] Total de linhas (bruto): {len(df)}")

    # ── Colunas obrigatórias ─────────────────────────────────────
    source_cols = {}
    missing = []
    for canonical in CANONICAL_COLUMNS:
        src = _find_source_column(list(df.columns), canonical)
        if not src:
            missing.append(canonical)
        else:
            source_cols[canonical] = src

    if missing:
        raise ValueError(
            f"Colunas ausentes na planilha: {missing}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    # ── Colunas opcionais — só inclui as que existem na planilha ──
    optional_found = {}
    for canonical in OPTIONAL_COLUMNS:
        src = _find_source_column(list(df.columns), canonical)
        if src:
            optional_found[canonical] = src
            print(f"[IMPORT] Coluna opcional encontrada: '{canonical}' ← '{src}'")
        else:
            print(f"[IMPORT] Coluna opcional ausente (ok): '{canonical}'")

    # ── Monta DF com obrigatórias ────────────────────────────────
    df2 = df[[source_cols[c] for c in CANONICAL_COLUMNS]].copy()
    df2.columns = CANONICAL_COLUMNS

    # ── Adiciona opcionais encontradas ───────────────────────────
    for canonical, src in optional_found.items():
        df2[canonical] = df[src].values

    # Remove linhas totalmente vazias
    before = len(df2)
    df2 = df2.dropna(how="all")
    after = len(df2)
    print(f"[IMPORT] Linhas antes limpeza: {before} | depois: {after}")

    # Conversões — obrigatórias
    for col in ["DATA ATENDIMENTO", "VENCIMENTO"]:
        df2[col] = pd.to_datetime(df2[col], errors="coerce").dt.date

    df2["QUANTIDADE"] = pd.to_numeric(df2["QUANTIDADE"], errors="coerce").fillna(0)
    df2["VALOR UNITARIO"] = pd.to_numeric(df2["VALOR UNITARIO"], errors="coerce").fillna(0)

    for col in ["CLIENTE / PACIENTE", "CATEGORIA", "PRODUTOS/SERVIÇOS", "DETALHES DO ITEM",
                "FORMA DE PAGAMENTO", "CONTA DE RECEBIMENTO", "CONDICAO DE PAGAMENTO"]:
        df2[col] = df2[col].astype(str).fillna("").map(lambda x: x.strip())

    # Conversões — opcionais
    if "DESCONTO" in df2.columns:
        df2["DESCONTO"] = pd.to_numeric(df2["DESCONTO"], errors="coerce")  # NaN se vazio

    for col in ["NUMERO_VENDA", "CENTRO_CUSTO"]:
        if col in df2.columns:
            df2[col] = df2[col].astype(str).replace("nan", "").replace("None", "").map(
                lambda x: x.strip() if x.strip() else None
            )

    records = df2.to_dict(orient="records")
    print(f"[IMPORT] Registros gerados: {len(records)}")
    return records
