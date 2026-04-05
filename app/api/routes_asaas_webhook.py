import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.db.session import SessionLocal
from app.db.models import Company, AsaasCredential
from app.services.asaas_webhook_handler import handle_payment_webhook

logger = logging.getLogger(__name__)

router = APIRouter(tags=["asaas-webhook"])


@router.post("/asaas/webhook/{company_id}")
async def asaas_webhook(company_id: int, request: Request):
    """
    Endpoint receptor de webhooks do Asaas.
    Sempre retorna HTTP 200 — o Asaas retenta em caso de falha de rede,
    mas não deve receber 4xx/5xx para eventos já processados.
    """
    try:
        payload = await request.json()
    except Exception:
        # Payload inválido — loga e confirma recebimento
        logger.warning(f"[ASAAS_WEBHOOK] company={company_id} payload inválido (não é JSON)")
        return JSONResponse({"ok": True, "result": "skipped", "reason": "invalid payload"})

    # Verifica se a empresa existe e tem credencial antes de processar
    db = SessionLocal()
    try:
        company = db.query(Company).filter_by(id=company_id).first()
        if not company:
            logger.warning(f"[ASAAS_WEBHOOK] company_id={company_id} não encontrado")
            return JSONResponse({"ok": True, "result": "skipped", "reason": "company not found"})
        has_cred = db.query(AsaasCredential).filter_by(company_id=company_id).first() is not None
    finally:
        db.close()

    if not company.asaas_enabled:
        logger.warning(f"[ASAAS_WEBHOOK] company={company_id} Asaas não habilitado")
        return JSONResponse({"ok": True, "result": "skipped", "reason": "asaas not enabled"})

    if not has_cred:
        logger.warning(f"[ASAAS_WEBHOOK] company={company_id} sem credencial Asaas")
        return JSONResponse({"ok": True, "result": "skipped", "reason": "no credential"})

    logger.info(f"[ASAAS_WEBHOOK] company={company_id} event={payload.get('event')} "
                f"payment={payload.get('payment', {}).get('id', 'n/a')}")

    try:
        result = handle_payment_webhook(company_id=company_id, payload=payload)
        return JSONResponse(result)
    except Exception as e:
        logger.exception(f"[ASAAS_WEBHOOK] company={company_id} erro não tratado: {e}")
        return JSONResponse({"ok": True, "result": "error", "reason": "internal error"})
