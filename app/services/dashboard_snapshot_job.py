"""
Dashboard Snapshot Job

Executa para um cliente específico ou para todos os ativos.
Chamado:
  - Automaticamente pelo APScheduler todo dia 5 às 6h (run_all_snapshots_job)
  - Manualmente via POST /dashboard/snapshot/run/{client_id}
"""

from __future__ import annotations
import json
import traceback
from datetime import datetime, date

from app.db.session import SessionLocal
from app.db.dashboard_models import DashClient, DashSnapshot
from app.services.conta_azul_client import DashboardCAClient
from app.services.dashboard_service import (
    build_category_map,
    build_snapshot_data,
    month_date_range,
)


def run_snapshot(dash_client_id: int, target_month: str) -> dict:
    """
    Executa snapshot de um mês para um cliente.

    target_month: "YYYY-MM" (ex: "2025-03")
    Retorna {"ok": True} ou {"ok": False, "error": "..."}
    """
    print(f"[SNAPSHOT] Iniciando: client_id={dash_client_id} mês={target_month}")

    db = SessionLocal()
    try:
        client = db.query(DashClient).filter(DashClient.id == dash_client_id).first()
        if not client:
            return {"ok": False, "error": f"DashClient {dash_client_id} não encontrado"}
        if not client.active:
            return {"ok": False, "error": f"DashClient {dash_client_id} inativo"}

        ignored_accounts: list[str] = json.loads(client.ignored_accounts or "[]")
        ignored_categories: list[str] = json.loads(client.ignored_categories or "[]")
        benchmarks: dict = json.loads(client.benchmarks or "{}")
    except Exception as e:
        return {"ok": False, "error": f"Erro ao carregar cliente: {e}"}
    finally:
        db.close()

    try:
        ca = DashboardCAClient(dash_client_id)
    except Exception as e:
        return {"ok": False, "error": f"Erro ao inicializar CA client: {e}"}

    # Busca categorias para montar o mapa entrada_dre
    try:
        categories = ca.list_categories_dre()
        category_map = build_category_map(categories)
        print(f"[SNAPSHOT] {len(category_map)} categorias mapeadas")
    except Exception as e:
        return {"ok": False, "error": f"Erro ao buscar categorias CA: {e}"}

    # Busca transações do mês filtradas por data de pagamento
    date_from, date_to = month_date_range(target_month)
    try:
        transactions = ca.list_transactions(date_from=date_from, date_to=date_to)
        print(f"[SNAPSHOT] {len(transactions)} transações encontradas em {target_month}")
    except Exception as e:
        return {"ok": False, "error": f"Erro ao buscar transações CA: {e}"}

    # Monta dados do snapshot
    try:
        snapshot_data = build_snapshot_data(
            snapshot_month=target_month,
            transactions=transactions,
            category_map=category_map,
            ignored_accounts=ignored_accounts,
            ignored_categories=ignored_categories,
            benchmarks=benchmarks,
        )
    except Exception as e:
        return {"ok": False, "error": f"Erro ao calcular DRE: {e}"}

    # Salva no banco (upsert)
    db = SessionLocal()
    try:
        snap = (
            db.query(DashSnapshot)
            .filter(
                DashSnapshot.client_id == dash_client_id,
                DashSnapshot.snapshot_month == target_month,
            )
            .first()
        )
        if snap:
            snap.data_json = json.dumps(snapshot_data, ensure_ascii=False)
            snap.updated_at = datetime.utcnow()
            print(f"[SNAPSHOT] Atualizado snapshot existente id={snap.id}")
        else:
            snap = DashSnapshot(
                client_id=dash_client_id,
                snapshot_month=target_month,
                data_json=json.dumps(snapshot_data, ensure_ascii=False),
            )
            db.add(snap)
            print("[SNAPSHOT] Novo snapshot criado")
        db.commit()
        return {"ok": True, "snapshot_month": target_month, "mes_label": snapshot_data.get("MES")}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": f"Erro ao salvar snapshot: {e}"}
    finally:
        db.close()


def run_snapshot_last_n_months(dash_client_id: int, n_months: int = 12) -> list[dict]:
    """
    Executa snapshot dos últimos N meses para um cliente.
    Útil no onboarding para popular o histórico inicial.
    """
    today = date.today()
    results = []
    for i in range(n_months - 1, -1, -1):
        # Calcula o mês: hoje - i meses
        month = today.month - i
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        target = f"{year:04d}-{month:02d}"
        result = run_snapshot(dash_client_id, target)
        results.append({"month": target, **result})
    return results


def run_all_snapshots_job():
    """
    Job executado pelo APScheduler todo dia 5 às 6h.
    Roda o mês anterior para todos os clientes ativos.
    """
    today = date.today()
    # Mês anterior
    month = today.month - 1
    year = today.year
    if month <= 0:
        month = 12
        year -= 1
    target_month = f"{year:04d}-{month:02d}"

    print(f"[SNAPSHOT_JOB] Iniciando job automático para {target_month}")

    db = SessionLocal()
    try:
        clients = db.query(DashClient).filter(DashClient.active == True).all()
        client_ids = [c.id for c in clients]
    finally:
        db.close()

    print(f"[SNAPSHOT_JOB] {len(client_ids)} clientes ativos")
    for cid in client_ids:
        try:
            result = run_snapshot(cid, target_month)
            status = "OK" if result.get("ok") else f"ERRO: {result.get('error')}"
            print(f"[SNAPSHOT_JOB] client_id={cid}: {status}")
        except Exception as e:
            print(f"[SNAPSHOT_JOB] client_id={cid}: EXCEÇÃO: {e}")
            traceback.print_exc()

    print("[SNAPSHOT_JOB] Job concluído")
