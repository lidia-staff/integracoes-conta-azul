from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.session import Base
from sqlalchemy import UniqueConstraint, Index


class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    slug = Column(String(100), nullable=True, unique=True)
    review_mode = Column(Boolean, default=True)
    access_token = Column(Text, nullable=True)
    refresh_token = Column(Text, nullable=True)
    token_expires_at = Column(DateTime, nullable=True)
    ca_financial_account_id = Column(String, nullable=True)
    default_item_id = Column(String, nullable=True)
    access_pin = Column(String(64), nullable=True)
    group_mode = Column(String(20), nullable=True, default="grouped")
    ca_sale_status = Column(String(30), nullable=True, default="EM_ANDAMENTO")
    # ca_sale_status: EM_ANDAMENTO | APROVADO | CONCLUIDO
    item_type = Column(String(20), nullable=True, default="servico")
    # item_type: servico | produto
    asaas_enabled = Column(Boolean, default=False)
    upload_enabled = Column(Boolean, default=True)
    batches = relationship("UploadBatch", back_populates="company", cascade="all, delete-orphan")
    sales = relationship("Sale", back_populates="company", cascade="all, delete-orphan")
    customers = relationship("CompanyCustomer", back_populates="company", cascade="all, delete-orphan")
    products = relationship("CompanyProduct", back_populates="company", cascade="all, delete-orphan")
    payment_accounts = relationship("CompanyPaymentAccount", back_populates="company", cascade="all, delete-orphan")
    cost_centers = relationship("CompanyCostCenter", back_populates="company", cascade="all, delete-orphan")
    categories = relationship("CompanyCategory", back_populates="company", cascade="all, delete-orphan")
    asaas_credential = relationship("AsaasCredential", back_populates="company", uselist=False, cascade="all, delete-orphan")


class CompanyPaymentAccount(Base):
    __tablename__ = "company_payment_accounts"
    __table_args__ = (
        UniqueConstraint("company_id", "payment_method_key", name="uq_company_payment_method"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    payment_method_key = Column(String(50), nullable=False)
    ca_financial_account_id = Column(String(80), nullable=False)
    label = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    company = relationship("Company", back_populates="payment_accounts")


class CompanyProduct(Base):
    """Cache de produtos/serviços do CA por empresa."""
    __tablename__ = "company_products"
    __table_args__ = (
        UniqueConstraint("company_id", "product_key", name="uq_company_product_key"),
        Index("ix_company_products_company_key", "company_id", "product_key"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    product_key = Column(String(250), nullable=False)   # nome normalizado
    product_name = Column(String(200), nullable=True)   # nome original
    ca_product_id = Column(String(80), nullable=False)  # UUID no CA
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    company = relationship("Company", back_populates="products")



class CompanyCostCenter(Base):
    """Mapeamento nome (planilha) → UUID do centro de custo no CA."""
    __tablename__ = "company_cost_centers"
    __table_args__ = (
        UniqueConstraint("company_id", "name_key", name="uq_company_cost_center_key"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    name_key = Column(String(250), nullable=False)     # nome normalizado (ex: LAVANDERIA)
    label = Column(String(200), nullable=True)          # nome original
    ca_cost_center_id = Column(String(80), nullable=False)  # UUID no CA
    created_at = Column(DateTime, default=datetime.utcnow)
    company = relationship("Company", back_populates="cost_centers")


class CompanyCategory(Base):
    """Mapeamento nome (planilha) → UUID da categoria financeira no CA."""
    __tablename__ = "company_categories"
    __table_args__ = (
        UniqueConstraint("company_id", "name_key", name="uq_company_category_key"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    name_key = Column(String(250), nullable=False)      # nome normalizado
    label = Column(String(200), nullable=True)           # nome original
    ca_category_id = Column(String(80), nullable=False)  # UUID no CA
    created_at = Column(DateTime, default=datetime.utcnow)
    company = relationship("Company", back_populates="categories")

class UploadBatch(Base):
    __tablename__ = "upload_batches"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    filename = Column(String(255), nullable=False)
    status = Column(String(30), default="PROCESSADO")
    created_at = Column(DateTime, default=datetime.utcnow)
    company = relationship("Company", back_populates="batches")
    sales = relationship("Sale", back_populates="batch", cascade="all, delete-orphan")


class Sale(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False)
    group_key = Column(String(500), nullable=False)
    hash_unique = Column(String(100), nullable=False)
    sale_date = Column(Date, nullable=False)
    customer_name = Column(String(200), nullable=False)
    payment_method = Column(String(100), nullable=False)
    payment_terms = Column(String(100), nullable=False)
    receiving_account = Column(String(120), nullable=False)
    due_date = Column(Date, nullable=False)
    total_amount = Column(Numeric(12, 2), nullable=False)
    status = Column(String(40), nullable=False)
    error_summary = Column(Text, nullable=True)
    ca_sale_id = Column(String(80), nullable=True)
    sale_number = Column(String(50), nullable=True)
    discount_amount = Column(Numeric(12, 2), nullable=True)
    cost_center_id = Column(String(80), nullable=True)
    company = relationship("Company", back_populates="sales")
    batch = relationship("UploadBatch", back_populates="sales")
    items = relationship("SaleItem", back_populates="sale", cascade="all, delete-orphan")


class SaleItem(Base):
    __tablename__ = "sale_items"
    id = Column(Integer, primary_key=True)
    sale_id = Column(Integer, ForeignKey("sales.id"), nullable=False)
    category = Column(String(150), nullable=True)
    product_service = Column(String(200), nullable=False)
    details = Column(String(250), nullable=True)
    qty = Column(Numeric(12, 2), nullable=False)
    unit_price = Column(Numeric(12, 2), nullable=False)
    line_total = Column(Numeric(12, 2), nullable=False)
    sale = relationship("Sale", back_populates="items")


class AsaasCredential(Base):
    __tablename__ = "asaas_credentials"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False, unique=True)
    api_key = Column(Text, nullable=False)
    environment = Column(String(20), default="production")  # "production" | "sandbox"
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    company = relationship("Company", back_populates="asaas_credential")


class AsaasProcessedEvent(Base):
    """Tabela de idempotência — evita processar o mesmo pagamento Asaas duas vezes."""
    __tablename__ = "asaas_processed_events"
    __table_args__ = (
        UniqueConstraint("company_id", "asaas_payment_id", name="uq_asaas_processed"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    asaas_payment_id = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # "ok" | "error" | "skipped"
    error_detail = Column(Text, nullable=True)
    processed_at = Column(DateTime, default=datetime.utcnow)


class AsaasExecutionLog(Base):
    """Auditoria de cada execução do fluxo Asaas → CA."""
    __tablename__ = "asaas_execution_logs"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    asaas_payment_id = Column(String(100), nullable=True)
    status = Column(String(20), nullable=False)  # "success" | "error" | "skipped"
    ca_customer_id = Column(String(100), nullable=True)
    ca_receivable_id = Column(String(100), nullable=True)
    error_detail = Column(Text, nullable=True)
    payload_summary = Column(Text, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class CompanyCustomer(Base):
    __tablename__ = "company_customers"
    __table_args__ = (
        UniqueConstraint("company_id", "customer_key", name="uq_company_customer_key"),
        Index("ix_company_customers_company_key", "company_id", "customer_key"),
    )
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    customer_key = Column(String(250), nullable=False)
    customer_name = Column(String(200), nullable=True)
    ca_customer_id = Column(String(80), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    company = relationship("Company", back_populates="customers")
