import os
import hashlib
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.db.session import SessionLocal
from app.db.dashboard_models import DashUser

_bearer = HTTPBearer(auto_error=False)

_SECRET = os.getenv("DASHBOARD_JWT_SECRET", "dashboard-secret-change-in-production")
_ALGORITHM = "HS256"
_EXPIRE_HOURS = 72


# ── Password hashing ────────────────────────────────────────────────


def hash_password(plain: str) -> str:
    return hashlib.sha256(plain.encode()).hexdigest()


def verify_password(plain: str, hashed: str) -> bool:
    return hash_password(plain) == hashed


# ── JWT ─────────────────────────────────────────────────────────────


def create_token(user_id: int, role: str, partner_id: int | None, client_id: int | None) -> str:
    payload = {
        "sub": str(user_id),
        "role": role,
        "partner_id": partner_id,
        "client_id": client_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=_EXPIRE_HOURS),
    }
    return jwt.encode(payload, _SECRET, algorithm=_ALGORITHM)


def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, _SECRET, algorithms=[_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")


# ── FastAPI dependency ───────────────────────────────────────────────


def get_current_user(credentials: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> dict:
    if not credentials:
        raise HTTPException(status_code=401, detail="Não autenticado")
    return verify_token(credentials.credentials)


def require_master(user: dict = Depends(get_current_user)) -> dict:
    if user.get("role") != "master":
        raise HTTPException(status_code=403, detail="Acesso restrito a Master")
    return user


def require_master_or_partner(user: dict = Depends(get_current_user)) -> dict:
    if user.get("role") not in ("master", "partner"):
        raise HTTPException(status_code=403, detail="Acesso restrito a Master ou Parceiro")
    return user


# ── Authenticate user (login) ────────────────────────────────────────


def authenticate_user(email: str, password: str) -> DashUser | None:
    db = SessionLocal()
    try:
        user = db.query(DashUser).filter(
            DashUser.email == email.strip().lower(),
            DashUser.active == True,
        ).first()
        if not user:
            return None
        if not verify_password(password, user.password_hash):
            return None
        return user
    finally:
        db.close()
