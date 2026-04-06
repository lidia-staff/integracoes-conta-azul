import os
import datetime as dt
import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import traceback

from app.db.session import SessionLocal
from app.db.models import Company
from app.db.dashboard_models import DashClient


class ContaAzulClient:
    """
    Cliente Conta Azul com Token Manager automático + LOGS DETALHADOS
    """

    def __init__(self, company_id: int):
        print(f"[CA_CLIENT] Inicializando para company_id={company_id}")
        self.company_id = company_id

        self.api_base = os.getenv("CA_API_BASE_URL", "https://api-v2.contaazul.com").rstrip("/")
        self.auth_url = os.getenv("CA_AUTH_URL", "https://auth.contaazul.com/oauth2/token")

        self.client_id = os.getenv("CA_CLIENT_ID")
        self.client_secret = os.getenv("CA_CLIENT_SECRET")
        
        print(f"[CA_CLIENT] API Base: {self.api_base}")
        print(f"[CA_CLIENT] Client ID configurado: {bool(self.client_id)}")
        
        if not self.client_id or not self.client_secret:
            raise RuntimeError("CA_CLIENT_ID ou CA_CLIENT_SECRET não configurados")

        # Carrega tokens do banco
        try:
            self._load_company_tokens()
            print(f"[CA_CLIENT] Tokens carregados com sucesso")
        except Exception as e:
            print(f"[CA_CLIENT] ERRO ao carregar tokens: {e}")
            raise

        # Pré-refresh se já nasceu expirado
        if self._is_token_expired():
            print(f"[CA_CLIENT] Token expirado, fazendo refresh preventivo")
            self._refresh_token()

    def _load_company_tokens(self):
        """Carrega access_token, refresh_token e expires_at do banco."""
        print(f"[CA_CLIENT] Carregando tokens do banco para company_id={self.company_id}")
        db: Session = SessionLocal()
        try:
            c = db.query(Company).filter(Company.id == self.company_id).first()
            if not c:
                raise RuntimeError(f"Company {self.company_id} não encontrada")
            
            print(f"[CA_CLIENT] Company encontrada: {c.name}")
            print(f"[CA_CLIENT] Has access_token: {bool(c.access_token)}")
            print(f"[CA_CLIENT] Has refresh_token: {bool(c.refresh_token)}")
            print(f"[CA_CLIENT] Token expires_at: {c.token_expires_at}")
            
            if not c.refresh_token:
                raise RuntimeError(
                    f"Company {self.company_id} sem refresh_token. "
                    "Execute o fluxo OAuth em /api/contaazul/start primeiro."
                )
            if not c.access_token:
                raise RuntimeError(f"Company {self.company_id} sem access_token salvo no banco")

            self.access_token = c.access_token
            self.refresh_token = c.refresh_token
            self.token_expires_at = c.token_expires_at
        finally:
            db.close()

    def _now_utc(self) -> dt.datetime:
        """Retorna datetime atual com timezone UTC."""
        return dt.datetime.now(dt.timezone.utc)

    def _as_aware_utc(self, value: dt.datetime | None) -> dt.datetime | None:
        """Converte datetime naive para aware UTC."""
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)

    def _is_token_expired(self) -> bool:
        """Verifica se token está expirado (com margem de 2min de segurança)."""
        exp = self._as_aware_utc(self.token_expires_at)
        if exp is None:
            print(f"[CA_CLIENT] Token sem expires_at, considerando expirado")
            return True
        
        now = self._now_utc()
        expired = now >= (exp - dt.timedelta(minutes=2))
        print(f"[CA_CLIENT] Token expirado? {expired} (now={now}, expires={exp})")
        return expired

    def _headers(self):
        """Headers para requests ao Conta Azul."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _refresh_token(self):
        """Renova access_token usando refresh_token e persiste no banco."""
        print(f"[CA_CLIENT] ===== INICIANDO REFRESH TOKEN =====")
        print(f"[CA_CLIENT] Company ID: {self.company_id}")

        # 1) Lê refresh_token mais recente do banco
        db: Session = SessionLocal()
        try:
            c = db.query(Company).filter(Company.id == self.company_id).first()
            if not c:
                raise RuntimeError(f"Company {self.company_id} não encontrada para refresh")
            if not c.refresh_token:
                raise RuntimeError(
                    f"Company {self.company_id} sem refresh_token. "
                    "Reautorize em /api/contaazul/start"
                )
            refresh_to_use = c.refresh_token
            print(f"[CA_CLIENT] Refresh token carregado do banco")
        finally:
            db.close()

        # 2) Chama endpoint de refresh do Conta Azul
        print(f"[CA_CLIENT] Fazendo POST para {self.auth_url}")
        try:
            r = requests.post(
                self.auth_url,
                auth=(self.client_id, self.client_secret),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={"grant_type": "refresh_token", "refresh_token": refresh_to_use},
                timeout=30,
            )
            print(f"[CA_CLIENT] Refresh response status: {r.status_code}")
        except requests.RequestException as e:
            print(f"[CA_CLIENT] ERRO de rede no refresh: {e}")
            raise RuntimeError(f"Erro de rede ao fazer refresh: {e}")

        if r.status_code >= 400:
            print(f"[CA_CLIENT] ERRO no refresh: {r.status_code} - {r.text}")
            raise RuntimeError(
                f"Token refresh failed [{r.status_code}]: {r.text}. "
                f"Reautorize a company {self.company_id} em /api/contaazul/start"
            )

        data = r.json()
        new_access = data.get("access_token")
        if not new_access:
            print(f"[CA_CLIENT] ERRO: Refresh não retornou access_token: {data}")
            raise RuntimeError(f"Refresh retornou sem access_token: {data}")

        new_refresh = data.get("refresh_token", refresh_to_use)
        expires_in = int(data.get("expires_in", 3600))
        new_expires_at = self._now_utc() + dt.timedelta(seconds=expires_in)
        
        print(f"[CA_CLIENT] Novo access_token obtido, expires_in={expires_in}s")

        # 3) Persiste com lock
        db = SessionLocal()
        try:
            c = (
                db.query(Company)
                .filter(Company.id == self.company_id)
                .with_for_update()
                .first()
            )
            if not c:
                raise RuntimeError(f"Company {self.company_id} não encontrada para salvar refresh")

            c.access_token = new_access
            c.refresh_token = new_refresh
            c.token_expires_at = new_expires_at
            db.add(c)
            db.commit()
            print(f"[CA_CLIENT] ✅ Tokens salvos no banco")
        except SQLAlchemyError as e:
            db.rollback()
            print(f"[CA_CLIENT] ERRO ao salvar tokens: {e}")
            raise RuntimeError(f"Erro ao persistir tokens no banco: {e}")
        finally:
            db.close()

        # 4) Atualiza em memória
        self.access_token = new_access
        self.refresh_token = new_refresh
        self.token_expires_at = new_expires_at
        print(f"[CA_CLIENT] ===== REFRESH CONCLUÍDO COM SUCESSO =====")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        timeout: int = 30,
        _retried: bool = False,
    ):
        """Request centralizada com Token Manager automático."""
        print(f"[CA_CLIENT] ===== REQUEST =====")
        print(f"[CA_CLIENT] {method} {path}")
        print(f"[CA_CLIENT] Params: {params}")
        
        # Pré-refresh se perto de expirar
        if self._is_token_expired():
            print(f"[CA_CLIENT] Token perto de expirar, fazendo refresh preventivo")
            self._refresh_token()

        url = f"{self.api_base}{path}"
        print(f"[CA_CLIENT] URL completa: {url}")
        
        try:
            r = requests.request(
                method, 
                url, 
                headers=self._headers(), 
                params=params, 
                json=json, 
                timeout=timeout
            )
            print(f"[CA_CLIENT] Response status: {r.status_code}")
        except requests.RequestException as e:
            print(f"[CA_CLIENT] ERRO de rede: {e}")
            print(f"[CA_CLIENT] Traceback: {traceback.format_exc()}")
            raise RuntimeError(f"Erro de rede ao chamar {method} {path}: {e}")

        # Retry automático em 401
        if r.status_code == 401 and not _retried:
            print(f"[CA_CLIENT] 401 detectado, fazendo refresh e retry")
            self._refresh_token()
            
            try:
                r = requests.request(
                    method, 
                    url, 
                    headers=self._headers(), 
                    params=params, 
                    json=json, 
                    timeout=timeout
                )
                print(f"[CA_CLIENT] Response após retry: {r.status_code}")
            except requests.RequestException as e:
                print(f"[CA_CLIENT] ERRO no retry: {e}")
                raise RuntimeError(f"Erro de rede no retry: {e}")

        # Se ainda 401 após refresh
        if r.status_code == 401:
            print(f"[CA_CLIENT] 401 após refresh - token inválido")
            raise RuntimeError(
                f"401 Unauthorized após refresh. "
                f"Company {self.company_id} precisa reautorizar em /api/contaazul/start"
            )

        # Outros erros HTTP
        if r.status_code >= 400:
            print(f"[CA_CLIENT] ERRO HTTP {r.status_code}: {r.text}")
            raise RuntimeError(
                f"Conta Azul API error [{r.status_code}] {method} {path}: {r.text}"
            )

        # Parse response
        txt = (r.text or "").strip()
        if txt.startswith("{") or txt.startswith("["):
            result = r.json()
            print(f"[CA_CLIENT] Response JSON (primeiros 200 chars): {str(result)[:200]}")
            return result
        
        print(f"[CA_CLIENT] Response texto: {txt[:200]}")
        return txt

    # ========== ENDPOINTS CONTA AZUL ==========

    def get_next_sale_number(self) -> int:
        """Retorna próximo número de venda disponível."""
        resp = self._request("GET", "/v1/venda/proximo-numero", timeout=20)
        try:
            return int(str(resp).strip())
        except Exception:
            raise RuntimeError(f"Resposta inesperada do próximo número: {resp}")

    def list_financial_accounts(self):
        """Lista TODAS as contas financeiras com paginação."""
        print(f"[CA_CLIENT] Listando contas financeiras (paginação PT)")
        
        all_accounts = []
        pagina = 1
        tamanho_pagina = 50
        
        while True:
            params = {"pagina": pagina, "tamanho_pagina": tamanho_pagina}
            print(f"[CA_CLIENT] Buscando página {pagina} (tamanho: {tamanho_pagina})")
            
            response = self._request("GET", "/v1/conta-financeira", params=params, timeout=30)
            
            if isinstance(response, dict):
                # API do CA usa 'itens' e 'itens_totais'
                accounts = response.get("itens", [])
                total = response.get("itens_totais", 0)
                
                all_accounts.extend(accounts)
                print(f"[CA_CLIENT] Página {pagina}: +{len(accounts)} contas | Total: {len(all_accounts)}/{total}")
                
                # Para se não tiver mais contas ou chegou no total
                if not accounts or len(all_accounts) >= total:
                    break
                    
                pagina += 1
            elif isinstance(response, list):
                # Retornou lista direta
                print(f"[CA_CLIENT] Lista direta: {len(response)} contas")
                return response
            else:
                break
        
        print(f"[CA_CLIENT] Total carregado: {len(all_accounts)} contas")
        return all_accounts

    def list_products(self, busca: str, pagina: int = 1, tamanho_pagina: int = 50, status: str = "ATIVO"):
        """Lista produtos/serviços."""
        params = {
            "pagina": pagina, 
            "tamanho_pagina": tamanho_pagina, 
            "busca": busca, 
            "status": status
        }
        return self._request("GET", "/v1/produtos", params=params, timeout=30)

    def list_people(self, nome: str, tipo_perfil: str = "Cliente") -> dict:
        """Lista pessoas (clientes, fornecedores, etc)."""
        params = {"nome": nome, "tipo_perfil": tipo_perfil}
        return self._request("GET", "/v1/pessoas", params=params, timeout=30)

    def create_person_cliente(self, nome: str) -> dict:
        """Cria novo cliente no Conta Azul."""
        payload = {
            "nome": nome,
            "tipo_pessoa": "Física",
            "perfis": [{"tipo_perfil": "Cliente"}],
            "ativo": True,
        }
        return self._request("POST", "/v1/pessoas", json=payload, timeout=30)

    def create_sale(self, payload: dict) -> dict:
        """Cria venda no Conta Azul."""
        return self._request("POST", "/v1/venda", json=payload, timeout=60)

    def create_product(self, nome: str, tipo: str = "Prestado") -> dict:
        """Cria produto no Conta Azul. tipo: Prestado | Tomado | Prestado e Tomado"""
        payload = {"nome": nome, "tipo": tipo, "ativo": True}
        return self._request("POST", "/v1/produtos", json=payload, timeout=30)

    def list_services(self, busca: str, pagina: int = 1, tamanho_pagina: int = 50) -> dict:
        """Lista serviços cadastrados no Conta Azul."""
        params = {
            "pagina": pagina,
            "tamanho_pagina": tamanho_pagina,
            "busca_textual": busca,
        }
        return self._request("GET", "/v1/servicos", params=params, timeout=30)

    def create_service(self, nome: str) -> dict:
        """Cria serviço no Conta Azul."""
        payload = {
            "descricao": nome,
            "tipo_servico": "PRESTADO",
            "status": "ATIVO",
        }
        return self._request("POST", "/v1/servicos", json=payload, timeout=30)

    # ── Asaas: contas a receber ──────────────────────────────────────

    def create_receivable(self, payload: dict) -> dict:
        """Cria conta a receber no Conta Azul. POST /v1/conta-a-receber"""
        return self._request("POST", "/v1/conta-a-receber", json=payload, timeout=30)

    def get_receivable(self, receivable_id: str) -> dict:
        """Busca conta a receber pelo ID. GET /v1/conta-a-receber/{id}"""
        return self._request("GET", f"/v1/conta-a-receber/{receivable_id}", timeout=30)

    def mark_receivable_paid(self, receivable_id: str, value: float, payment_date: str) -> dict:
        """Baixa (marca como pago) uma conta a receber. PATCH /v1/conta-a-receber/{id}/receber"""
        payload = {"valor": value, "data_pagamento": payment_date}
        return self._request("PATCH", f"/v1/conta-a-receber/{receivable_id}/receber",
                             json=payload, timeout=30)

    # ── Dashboard: lançamentos financeiros ──────────────────────────

    def list_transactions(
        self,
        date_from: str,
        date_to: str,
        tipo: str | None = None,
        pagina: int = 1,
        tamanho_pagina: int = 100,
    ) -> list:
        """
        Lista lançamentos financeiros liquidados filtrados por data de pagamento.

        date_from / date_to: "YYYY-MM-DD"
        tipo: "Receita" | "Despesa" | None (ambos)
        Retorna lista completa paginando automaticamente.
        """
        print(f"[CA_CLIENT] list_transactions {date_from} → {date_to} tipo={tipo}")
        all_items: list = []
        page = pagina

        while True:
            params: dict = {
                "pagina": page,
                "tamanho_pagina": tamanho_pagina,
                "data_pagamento_de": date_from,
                "data_pagamento_ate": date_to,
                "situacao": "LIQUIDADO",
            }
            if tipo:
                params["tipo"] = tipo

            resp = self._request("GET", "/v1/lancamento", params=params, timeout=30)

            if isinstance(resp, list):
                all_items.extend(resp)
                break
            elif isinstance(resp, dict):
                items = resp.get("itens", resp.get("items", []))
                total = resp.get("itens_totais", resp.get("total", 0))
                all_items.extend(items)
                print(f"[CA_CLIENT] list_transactions página {page}: +{len(items)} | total {len(all_items)}/{total}")
                if not items or len(all_items) >= total:
                    break
                page += 1
            else:
                break

        print(f"[CA_CLIENT] list_transactions total carregado: {len(all_items)}")
        return all_items

    def list_categories_dre(self) -> list:
        """
        Lista categorias financeiras do Conta Azul, incluindo o campo entrada_dre.
        Tenta os endpoints conhecidos da API CA e retorna lista completa.
        """
        print("[CA_CLIENT] list_categories_dre")
        all_items: list = []

        # Endpoints possíveis do CA para categorias — tenta em sequência
        candidates = [
            "/v1/plano-de-contas",
            "/v1/categoria-lancamento",
            "/v1/categoria",
        ]

        for endpoint in candidates:
            try:
                page = 1
                tamanho_pagina = 100
                found: list = []
                while True:
                    params = {"pagina": page, "tamanho_pagina": tamanho_pagina}
                    resp = self._request("GET", endpoint, params=params, timeout=30)
                    if isinstance(resp, list):
                        found.extend(resp)
                        break
                    elif isinstance(resp, dict):
                        items = resp.get("itens", resp.get("items", resp.get("data", [])))
                        total = resp.get("itens_totais", resp.get("total", len(items)))
                        found.extend(items)
                        if not items or len(found) >= total:
                            break
                        page += 1
                    else:
                        break
                if found:
                    print(f"[CA_CLIENT] list_categories_dre: {len(found)} itens via {endpoint}")
                    all_items = found
                    break
            except Exception as e:
                print(f"[CA_CLIENT] list_categories_dre {endpoint} falhou: {e}")
                continue

        print(f"[CA_CLIENT] list_categories_dre total: {len(all_items)}")
        return all_items


# ── Dashboard CA Client ──────────────────────────────────────────────


class DashboardCAClient(ContaAzulClient):
    """
    Versão do ContaAzulClient que carrega e persiste tokens em dash_clients,
    não em companies. Usado exclusivamente pelo módulo Dashboard.
    """

    def __init__(self, dash_client_id: int):
        self.dash_client_id = dash_client_id
        # inicializa atributos antes de chamar o parent (que chama _load_company_tokens)
        self.company_id = None  # não usado neste cliente
        self.api_base = os.getenv("CA_API_BASE_URL", "https://api-v2.contaazul.com").rstrip("/")
        self.auth_url = os.getenv("CA_AUTH_URL", "https://auth.contaazul.com/oauth2/token")
        self.client_id = os.getenv("CA_CLIENT_ID")
        self.client_secret = os.getenv("CA_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise RuntimeError("CA_CLIENT_ID ou CA_CLIENT_SECRET não configurados")

        self._load_dash_tokens()

        if self._is_token_expired():
            self._refresh_token()

    def _load_dash_tokens(self):
        db = SessionLocal()
        try:
            c = db.query(DashClient).filter(DashClient.id == self.dash_client_id).first()
            if not c:
                raise RuntimeError(f"DashClient {self.dash_client_id} não encontrado")
            if not c.ca_refresh_token:
                raise RuntimeError(
                    f"DashClient {self.dash_client_id} sem refresh_token. "
                    "Execute o fluxo OAuth em /dashboard/onboarding primeiro."
                )
            if not c.ca_access_token:
                raise RuntimeError(f"DashClient {self.dash_client_id} sem access_token")

            self.access_token = c.ca_access_token
            self.refresh_token = c.ca_refresh_token
            self.token_expires_at = c.ca_token_expires_at
        finally:
            db.close()

    # Sobrescreve os métodos que salvam tokens no banco
    def _load_company_tokens(self):
        self._load_dash_tokens()

    def _refresh_token(self):
        """Faz refresh e persiste em dash_clients (não em companies)."""
        db = SessionLocal()
        try:
            c = db.query(DashClient).filter(DashClient.id == self.dash_client_id).first()
            if not c or not c.ca_refresh_token:
                raise RuntimeError(f"DashClient {self.dash_client_id} sem refresh_token para refresh")
            refresh_to_use = c.ca_refresh_token
        finally:
            db.close()

        import requests as _req
        r = _req.post(
            self.auth_url,
            auth=(self.client_id, self.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_to_use},
            timeout=30,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Token refresh failed [{r.status_code}]: {r.text}")

        data = r.json()
        new_access = data.get("access_token")
        if not new_access:
            raise RuntimeError(f"Refresh sem access_token: {data}")

        new_refresh = data.get("refresh_token", refresh_to_use)
        expires_in = int(data.get("expires_in", 3600))
        new_expires_at = self._now_utc() + __import__("datetime").timedelta(seconds=expires_in)

        db = SessionLocal()
        try:
            c = db.query(DashClient).filter(DashClient.id == self.dash_client_id).with_for_update().first()
            c.ca_access_token = new_access
            c.ca_refresh_token = new_refresh
            c.ca_token_expires_at = new_expires_at
            db.commit()
        finally:
            db.close()

        self.access_token = new_access
        self.refresh_token = new_refresh
        self.token_expires_at = new_expires_at
