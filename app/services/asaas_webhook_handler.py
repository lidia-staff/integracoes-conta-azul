import time
import logging
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.db.session import SessionLocal
from app.db.models import Company, AsaasCredential, AsaasProcessedEvent, AsaasExecutionLog
from app.services.asaas_client import AsaasClient
from app.services.conta_azul_client import ContaAzulClient
from app.services.contaazul_people import get_or_create_customer_uuid_cached

logger = logging.getLogger(__name__)

# Eventos Asaas que disparam o fluxo de sincronização com CA
SUPPORTED_EVENTS = {"PAYMENT_RECEIVED", "PAYMENT_CONFIRMED"}


def handle_payment_webhook(company_id: int, payload: dict) -> dict:
    """
    Ponto de entrada chamado pela rota POST /asaas/webhook/{company_id}.
    Retorna {"ok": True, "result": "success"|"skipped"|"error"}.
    """
    event = payload.get("event", "")
    payment_data = payload.get("payment", {})
    payment_id = payment_data.get("id", "")

    if not payment_id:
        logger.warning(f"[ASAAS_WEBHOOK] company={company_id} payload sem payment.id")
        return {"ok": True, "result": "skipped", "reason": "no payment id"}

    if event not in SUPPORTED_EVENTS:
        logger.debug(f"[ASAAS_WEBHOOK] company={company_id} evento ignorado: {event}")
        return {"ok": True, "result": "skipped", "reason": f"event {event} not handled"}

    db: Session = SessionLocal()
    try:
        # Idempotência: já processamos este pagamento?
        already = db.query(AsaasProcessedEvent).filter_by(
            company_id=company_id,
            asaas_payment_id=payment_id,
        ).first()
        if already:
            logger.info(f"[ASAAS_WEBHOOK] company={company_id} payment={payment_id} já processado ({already.status})")
            return {"ok": True, "result": "skipped", "reason": "already processed"}

        cred = db.query(AsaasCredential).filter_by(company_id=company_id).first()
        if not cred:
            logger.error(f"[ASAAS_WEBHOOK] company={company_id} sem credencial Asaas")
            return {"ok": True, "result": "skipped", "reason": "no asaas credential"}

        company = db.query(Company).filter_by(id=company_id).first()
        if not company or not company.access_token:
            logger.error(f"[ASAAS_WEBHOOK] company={company_id} sem token CA")
            _save_log(db, company_id, payment_id, "error",
                      error_detail="Empresa sem token Conta Azul configurado",
                      payload_summary=str(payload)[:500])
            return {"ok": True, "result": "error"}

        asaas_client = AsaasClient(api_key=cred.api_key, environment=cred.environment)
        ca_client = ContaAzulClient(company_id=company_id, db=db)

    except Exception as e:
        logger.exception(f"[ASAAS_WEBHOOK] company={company_id} erro ao inicializar clientes")
        db.close()
        return {"ok": True, "result": "error"}

    start_ms = int(time.time() * 1000)
    ca_customer_id = None
    ca_receivable_id = None
    error_detail = None

    try:
        # Busca detalhes completos do pagamento no Asaas
        payment = asaas_client.get_payment(payment_id)

        # Executa as 3 ações no CA
        ca_customer_id, ca_receivable_id = _sync_to_ca(
            company_id=company_id,
            payment=payment,
            asaas_client=asaas_client,
            ca_client=ca_client,
            db=db,
        )
        status = "success"
        logger.info(f"[ASAAS_WEBHOOK] company={company_id} payment={payment_id} → "
                    f"cliente={ca_customer_id} recebível={ca_receivable_id} ✓")

    except Exception as e:
        status = "error"
        error_detail = str(e)
        logger.exception(f"[ASAAS_WEBHOOK] company={company_id} payment={payment_id} ERRO: {e}")

    duration_ms = int(time.time() * 1000) - start_ms

    # Salva idempotência + log
    try:
        processed = AsaasProcessedEvent(
            company_id=company_id,
            asaas_payment_id=payment_id,
            status="ok" if status == "success" else "error",
            error_detail=error_detail,
        )
        db.add(processed)
        db.flush()
    except IntegrityError:
        db.rollback()
        logger.warning(f"[ASAAS_WEBHOOK] company={company_id} payment={payment_id} race condition — ignorado")
        db.close()
        return {"ok": True, "result": "skipped", "reason": "already processed (race)"}

    _save_log(
        db=db,
        company_id=company_id,
        payment_id=payment_id,
        status=status,
        ca_customer_id=ca_customer_id,
        ca_receivable_id=ca_receivable_id,
        error_detail=error_detail,
        payload_summary=str(payload)[:500],
        duration_ms=duration_ms,
    )
    db.commit()
    db.close()

    return {"ok": True, "result": status}


def _sync_to_ca(
    company_id: int,
    payment: dict,
    asaas_client: AsaasClient,
    ca_client: ContaAzulClient,
    db: Session,
) -> tuple:
    """
    Executa as 3 ações no CA:
      1. Criar/atualizar cliente
      2. Criar conta a receber
      3. Baixar (marcar como pago)
    Retorna (ca_customer_id, ca_receivable_id).
    """
    # ── Passo 1: Resolver cliente ──────────────────────────────────────────
    customer_asaas_id = payment.get("customer")
    customer_name = None

    if customer_asaas_id:
        try:
            cust = asaas_client.get_customer(customer_asaas_id)
            customer_name = cust.get("name") or cust.get("nome")
        except Exception as e:
            logger.warning(f"[ASAAS_WEBHOOK] Erro ao buscar cliente Asaas {customer_asaas_id}: {e}")

    if not customer_name:
        customer_name = payment.get("description") or f"Cliente Asaas {customer_asaas_id or 'desconhecido'}"

    ca_customer_id = get_or_create_customer_uuid_cached(
        db=db,
        client=ca_client,
        company_id=company_id,
        customer_name=customer_name,
    )

    # ── Passo 2: Criar conta a receber ──────────────────────────────────────
    value = float(payment.get("value", 0))
    due_date = payment.get("dueDate") or payment.get("paymentDate") or datetime.utcnow().strftime("%Y-%m-%d")
    description = payment.get("description") or f"Pagamento Asaas {payment.get('id', '')}"

    receivable_payload = {
        "id_pessoa": ca_customer_id,
        "descricao": description[:200],
        "valor": value,
        "data_vencimento": due_date,
    }

    receivable_resp = ca_client.create_receivable(receivable_payload)
    ca_receivable_id = (
        receivable_resp.get("id")
        or receivable_resp.get("id_conta_a_receber")
        or receivable_resp.get("uuid")
    )
    if not ca_receivable_id:
        raise RuntimeError(f"CA não retornou ID da conta a receber: {receivable_resp}")

    # ── Passo 3: Baixar conta a receber ──────────────────────────────────────
    payment_date = (
        payment.get("paymentDate")
        or payment.get("confirmedDate")
        or datetime.utcnow().strftime("%Y-%m-%d")
    )

    ca_client.mark_receivable_paid(
        receivable_id=ca_receivable_id,
        value=value,
        payment_date=payment_date,
    )

    return ca_customer_id, ca_receivable_id


def _save_log(
    db: Session,
    company_id: int,
    payment_id: str,
    status: str,
    ca_customer_id: str = None,
    ca_receivable_id: str = None,
    error_detail: str = None,
    payload_summary: str = None,
    duration_ms: int = None,
):
    log = AsaasExecutionLog(
        company_id=company_id,
        asaas_payment_id=payment_id,
        status=status,
        ca_customer_id=ca_customer_id,
        ca_receivable_id=ca_receivable_id,
        error_detail=error_detail,
        payload_summary=payload_summary,
        duration_ms=duration_ms,
    )
    db.add(log)
