from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime

from app.db.session import Base


class DashPartner(Base):
    """Parceiros (ex: Santos Inteligência Financeira)."""
    __tablename__ = "dash_partners"

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    slug = Column(String(100), nullable=True, unique=True)
    logo_url = Column(Text, nullable=True)
    primary_color = Column(String(20), nullable=True, default="#F26522")
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    clients = relationship("DashClient", back_populates="partner", cascade="all, delete-orphan")
    users = relationship("DashUser", back_populates="partner")


class DashClient(Base):
    """Clientes finais do dashboard (Body Face, Kimberly, etc.)."""
    __tablename__ = "dash_clients"

    id = Column(Integer, primary_key=True)
    partner_id = Column(Integer, ForeignKey("dash_partners.id"), nullable=False)
    name = Column(String(200), nullable=False)
    # segment: servico | estetica | construcao | comercio
    segment = Column(String(50), nullable=True, default="servico")
    logo_url = Column(Text, nullable=True)
    primary_color = Column(String(20), nullable=True, default="#F26522")
    bg_color = Column(String(20), nullable=True, default="#0f0f0f")

    # Credenciais Conta Azul — independentes do módulo de vendas
    ca_access_token = Column(Text, nullable=True)
    ca_refresh_token = Column(Text, nullable=True)
    ca_token_expires_at = Column(DateTime, nullable=True)

    # Filtros e configuração (armazenados como JSON em texto)
    # ignored_accounts: lista de IDs de contas CA a ignorar
    ignored_accounts = Column(Text, nullable=True, default="[]")
    # ignored_categories: lista de IDs de categorias CA a ignorar
    ignored_categories = Column(Text, nullable=True, default="[]")
    # benchmarks: {"EBITDA": 15, "MARGEM_CONTRIBUICAO": 40, "MARKUP": 50, ...}
    benchmarks = Column(Text, nullable=True, default="{}")

    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    partner = relationship("DashPartner", back_populates="clients")
    snapshots = relationship("DashSnapshot", back_populates="client", cascade="all, delete-orphan")
    users = relationship("DashUser", back_populates="client")


class DashUser(Base):
    """Usuários do sistema de dashboard."""
    __tablename__ = "dash_users"
    __table_args__ = (
        UniqueConstraint("email", name="uq_dash_users_email"),
    )

    id = Column(Integer, primary_key=True)
    email = Column(String(200), nullable=False)
    password_hash = Column(String(200), nullable=False)
    # role: master | partner | client
    role = Column(String(20), nullable=False, default="client")

    # master: partner_id e client_id nulos
    # partner: partner_id preenchido, client_id nulo
    # client: partner_id nulo, client_id preenchido
    partner_id = Column(Integer, ForeignKey("dash_partners.id"), nullable=True)
    client_id = Column(Integer, ForeignKey("dash_clients.id"), nullable=True)

    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    partner = relationship("DashPartner", back_populates="users")
    client = relationship("DashClient", back_populates="users")


class DashSnapshot(Base):
    """Snapshot mensal do DRE — gerado do CA API e salvo como JSON."""
    __tablename__ = "dash_snapshots"
    __table_args__ = (
        UniqueConstraint("client_id", "snapshot_month", name="uq_dash_snapshot_client_month"),
    )

    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("dash_clients.id"), nullable=False)
    # snapshot_month: "2025-03" (YYYY-MM)
    snapshot_month = Column(String(7), nullable=False)
    # data_json: estrutura completa do mês (campos DRE + subcats)
    data_json = Column(Text, nullable=False, default="{}")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    client = relationship("DashClient", back_populates="snapshots")
