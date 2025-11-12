# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# Versão pydeck: desempenho melhor com muitos pontos mantendo
# todas as funcionalidades (filtros, busca, salvar coords, edição).
# Planilha: https://docs.google.com/spreadsheets/d/1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k (aba "Página1")
# -------------------------------------------------------------

import os
import json
import re
import math
import time
import hashlib
import requests
import pandas as pd
import pydeck as pdk
import bcrypt
from io import StringIO
from typing import Tuple, Optional, List, Dict

import streamlit as st
from streamlit_cookies_manager import EncryptedCookieManager

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, RefreshError

# Geocoding
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

# Streamlit config
st.set_page_config(page_title="Distribuidores", layout="wide")

# -----------------------------
# CONFIGURAÇÃO GOOGLE SHEETS
# -----------------------------
SHEET_ID = "1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k"
SHEET_NAME = "Página1"
COLUNAS = ["Distribuidor", "Contato", "Email", "Estado", "Cidade", "Latitude", "Longitude"]

# -----------------------------
# PATHS LOCAIS / ARQUIVOS
# -----------------------------
USUARIOS_FILE = "usuarios.json"

# -----------------------------
# SCOPES GOOGLE
# -----------------------------
SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Globals for gspread objects
GC = None
WORKSHEET = None

# -----------------------------
# UTIL: limpar/formatar strings
# -----------------------------
def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


# -----------------------------
# INICIALIZAÇÃO DO GOOGLE SHEETS
# - autentica via Service Account JSON guardado em st.secrets["gcp_service_account"]
# - cria aba e colunas se necessário (Latitude/Longitude criadas automaticamente)
# -----------------------------
def init_gsheets():
    global GC, WORKSHEET
    if "gcp_service_account" not in st.secrets:
        st.error("❌ Google Service Account não encontrada nos Secrets do Streamlit Cloud.")
        st.stop()
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
        GC = gspread.authorize(creds)
        sh = GC.open_by_key(SHEET_ID)
        try:
            WORKSHEET = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            # criar e inicializar colunas
            WORKSHEET = sh.add_worksheet(title=SHEET_NAME, rows="2000", cols="20")
            WORKSHEET.update([COLUNAS])
        # garantir que as colunas existam e estejam na ordem desejada
        header = WORKSHEET.row_values(1)
        if not header:
            WORKSHEET.update([COLUNAS])
        else:
            # criar colunas faltantes (Latitude/Longitude) se necessário
            missing = [c for c in COLUNAS if c not in header]
            if missing:
                new_header = header + missing
                WORKSHEET.update([new_header])
    except Exception as e:
        st.error("Erro ao autenticar/abrir Google Sheets: " + str(e))
        st.stop()


init_gsheets()

# -----------------------------
# CACHE simples para geocoding (memória durante sessão)
# -----------------------------
if "geo_cache" not in st.session_state:
    st.session_state.geo_cache = {}

# -----------------------------
# FUNÇÕES DE DADOS (Sheets)
# -----------------------------
@st.cache_data(ttl=300)
def carregar_dados() -> pd.DataFrame:
    """
    Carrega dados do Google Sheets, garante colunas básicas, sanitiza lat/lon.
    Se não houver linhas, retorna df com colunas.
    """
    try:
        records = WORKSHEET.get_all_records()
    except Exception as e:
        st.error("Erro ao ler planilha: " + str(e))
        return pd.DataFrame(columns=COLUNAS)

    if not records:
        # inicializar cabeçalho na planilha se necessário
        try:
            header = WORKSHEET.row_values(1)
            if not header:
                WORKSHEET.update([COLUNAS])
        except Exception:
            pass
        return pd.DataFrame(columns=COLUNAS)

    df = pd.DataFrame(records)
    # adicionar colunas faltantes
    for col in COLUNAS:
        if col not in df.columns:
            df[col] = ""

    # manter somente as colunas desejadas na ordem certa
    df = df[COLUNAS].copy()

    # Sanitizar Latitude/Longitude: converter para número, aceitar apenas faixa do Brasil
    def to_float_safe(x):
        if x is None:
            return pd.NA
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "":
            return pd.NA
        s = s.replace(",", ".").replace(" ", "")
        try:
            return float(s)
        except:
            return pd.NA

    df["Latitude"] = df["Latitude"].apply(to_float_safe)
    df["Longitude"] = df["Longitude"].apply(to_float_safe)

    # Limites aproximados do Brasil: latitude -35..6 , longitude -82..-30
    df.loc[~df["Latitude"].between(-35.0, 6.0, inclusive="both"), "Latitude"] = pd.NA
    df.loc[~df["Longitude"].between(-82.0, -30.0, inclusive="both"), "Longitude"] = pd.NA

    return df


def salvar_dados(df: pd.DataFrame):
    """
    Grava os dados no Google Sheets. Atualiza header e linhas.
    """
    try:
        df2 = df.copy()
        df2 = df2[COLUNAS].fillna("")
        # clear e update pode ser pesado; porém garante consistência
        WORKSHEET.clear()
        WORKSHEET.update([df2.columns.values.tolist()] + df2.values.tolist())
        # limpar cache carregamento
        try:
            st.cache_data.clear()
        except Exception:
            pass
    except Exception as e:
        st.error("Erro ao salvar dados na planilha: " + str(e))


# -----------------------------
# COOKIES (LOGIN PERSISTENTE)
# -----------------------------
cookies = EncryptedCookieManager(
    prefix="distribuidores_login",
    password="chave_secreta_segura_123"
)
if not cookies.ready():
    st.stop()

# -----------------------------
# CAPITAIS BRASILEIRAS (para regras de alocação)
# -----------------------------
CAPITAIS_BRASILEIRAS = [
    "Rio Branco-AC","Maceió-AL","Macapá-AP","Manaus-AM","Salvador-BA","Fortaleza-CE",
    "Brasília-DF","Vitória-ES","Goiânia-GO","São Luís-MA","Cuiabá-MT","Campo Grande-MS",
    "Belo Horizonte-MG","Belém-PA","João Pessoa-PB","Curitiba-PR","Recife-PE","Teresina-PI",
    "Rio de Janeiro-RJ","Natal-RN","Porto Alegre-RS","Boa Vista-RR","Florianópolis-SC",
    "São Paulo-SP","Aracaju-SE","Palmas-TO"
]


def cidade_eh_capital(cidade: str, uf: str) -> bool:
    return f"{cidade}-{uf}" in CAPITAIS_BRASILEIRAS


# -----------------------------
# CENTROIDES FIXOS POR UF (fallback para zoom)
# -----------------------------
STATE_CENTROIDS = {
    "AC": {"center": [-8.77, -70.55], "zoom": 5.5},
    "AL": {"center": [-9.62, -36.82], "zoom": 6.5},
    "AP": {"center": [1.41, -51.77], "zoom": 5.5},
    "AM": {"center": [-3.07, -61.67], "zoom": 4.5},
    "BA": {"center": [-13.29, -41.71], "zoom": 5.5},
    "CE": {"center": [-5.20, -39.53], "zoom": 6.5},
    "DF": {"center": [-15.79, -47.88], "zoom": 9},
    "ES": {"center": [-19.19, -40.34], "zoom": 6.5},
    "GO": {"center": [-16.64, -49.31], "zoom": 6},
    "MA": {"center": [-2.55, -44.30], "zoom": 5.5},
    "MT": {"center": [-12.64, -55.42], "zoom": 4.5},
    "MS": {"center": [-20.51, -54.54], "zoom": 5.5},
    "MG": {"center": [-18.10, -44.38], "zoom": 5.5},
    "PA": {"center": [-5.53, -52.29], "zoom": 4.5},
    "PB": {"center": [-7.06, -35.55], "zoom": 6.5},
    "PR": {"center": [-24.89, -51.55], "zoom": 6.5},
    "PE": {"center": [-8.28, -35.07], "zoom": 6.5},
    "PI": {"center": [-7.71, -42.73], "zoom": 5.5},
    "RJ": {"center": [-22.90, -43.20], "zoom": 7},
    "RN": {"center": [-5.22, -36.52], "zoom": 6.5},
    "RS": {"center": [-30.03, -51.23], "zoom": 5.5},
    "RO": {"center": [-10.83, -63.34], "zoom": 5.5},
    "RR": {"center": [2.82, -60.67], "zoom": 5.5},
    "SC": {"center": [-27.33, -49.44], "zoom": 6.5},
    "SP": {"center": [-22.19, -48.79], "zoom": 6.5},
    "SE": {"center": [-10.90, -37.07], "zoom": 6.5},
    "TO": {"center": [-9.45, -48.26], "zoom": 5.5},
}

# -----------------------------
# Carregar estados/cidades via IBGE (caching com st.cache_data)
# -----------------------------
@st.cache_data(ttl=3600)
def carregar_estados() -> List[Dict]:
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return sorted(resp.json(), key=lambda e: e["nome"])


@st.cache_data(ttl=3600)
def carregar_cidades(uf: str) -> List[Dict]:
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return sorted(resp.json(), key=lambda c: c["nome"])


@st.cache_data(ttl=3600)
def carregar_todas_cidades() -> List[str]:
    cidades = []
    estados = carregar_estados()
    for estado in estados:
        uf = estado["sigla"]
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            for c in resp.json():
                cidades.append(f"{c['nome']} - {uf}")
    return sorted(cidades)


# -----------------------------
# GEOCODING (Nominatim)
# - usa cache em st.session_state.geo_cache para evitar repetir
# - grava coords na planilha quando encontrado (se for novo)
# -----------------------------
def geocode_with_cache(city: str, uf: str) -> Tuple[Optional[float], Optional[float]]:
    key = f"{city.strip().lower()}___{uf.strip().upper()}"
    cache = st.session_state.geo_cache
    if key in cache:
        return cache[key]

    # tentar Nominatim (padrão)
    geolocator = Nominatim(user_agent="distribuidores_app_streamlit", timeout=8)
    try:
        loc = geolocator.geocode(f"{city}, {uf}, Brasil")
        if loc:
            lat, lon = float(loc.latitude), float(loc.longitude)
            # sanity check Brasil range
            if -35.0 <= lat <= 6.0 and -82.0 <= lon <= -30.0:
                cache[key] = (lat, lon)
                st.session_state.geo_cache = cache
                return lat, lon
    except (GeocoderTimedOut, GeocoderUnavailable, Exception):
        # não propaga erro, retorna None
        pass

    cache[key] = (None, None)
    st.session_state.geo_cache = cache
    return None, None


# -----------------------------
# UTIL: cor por nome (gera hex)
# -----------------------------
def cor_distribuidor_hex(nome: str) -> str:
    if not nome:
        return "#777777"
    # usar hash md5 para consistência entre execuções
    h = hashlib.md5(nome.encode("utf-8")).hexdigest()
    # pegar 6 chars hex
    return f"#{h[:6]}".upper()


# -----------------------------
# Função que monta o dataframe para pydeck
# Garante que cada linha tem lat/lon válidos; se não tiver, tenta geocodificar e grava.
# -----------------------------
def preparar_dataframe_para_mapa(df: pd.DataFrame, salvar_novas_coords: bool = True) -> pd.DataFrame:
    """
    Retorna uma cópia de df filtrada com colunas 'lat' e 'lon' prontas para o pydeck.
    Tenta geocodificar cidades sem coords válidas e grava as coords encontradas na planilha.
    """
    df2 = df.copy()
    # Garantir colunas de latitude/longitude existem
    if "Latitude" not in df2.columns:
        df2["Latitude"] = pd.NA
    if "Longitude" not in df2.columns:
        df2["Longitude"] = pd.NA

    # Percorrer linhas sem coords válidas e tentar geocodificar
    need_save = False
    updated_rows = []
    for idx, row in df2.iterrows():
        lat = row["Latitude"]
        lon = row["Longitude"]
        if pd.isna(lat) or pd.isna(lon):
            cidade = _safe_str(row.get("Cidade", ""))
            estado = _safe_str(row.get("Estado", ""))
            if cidade and estado:
                lat_g, lon_g = geocode_with_cache(cidade, estado)
                if lat_g is not None and lon_g is not None:
                    df2.at[idx, "Latitude"] = lat_g
                    df2.at[idx, "Longitude"] = lon_g
                    need_save = True
                    updated_rows.append(idx)
        # else: deixa como está (assume que está limpo)
    if need_save and salvar_novas_coords:
        # salvar apenas se realmente encontrou novas coordenadas
        try:
            salvar_dados(df2)
        except Exception:
            # não interrompe exibição do mapa por falha no save
            st.warning("Aviso: ocorreu um erro ao salvar novas coordenadas na planilha.")
    # construir colunas lat/lon para pydeck (float)
    df2["lat"] = pd.to_numeric(df2["Latitude"], errors="coerce")
    df2["lon"] = pd.to_numeric(df2["Longitude"], errors="coerce")
    df2 = df2.dropna(subset=["lat", "lon"])
    return df2


# -----------------------------
# Função que calcula view_state (center + zoom) para pydeck a partir de dados
# pydeck usa latitude/longitude = center, zoom numeric
# Vamos converter span em um zoom razoável (heurística)
# -----------------------------
def calculate_view_state(df_points: pd.DataFrame, fallback_state: Optional[str] = None) -> Dict:
    default_center = (-14.2350, -51.9253)
    if df_points is None or df_points.empty:
        if fallback_state and fallback_state in STATE_CENTROIDS:
            c = STATE_CENTROIDS[fallback_state]["center"]
            return {"latitude": c[0], "longitude": c[1], "zoom": STATE_CENTROIDS[fallback_state]["zoom"]}
        return {"latitude": default_center[0], "longitude": default_center[1], "zoom": 4.8}

    lats = df_points["lat"].astype(float)
    lons = df_points["lon"].astype(float)
    if lats.empty or lons.empty:
        if fallback_state and fallback_state in STATE_CENTROIDS:
            c = STATE_CENTROIDS[fallback_state]["center"]
            return {"latitude": c[0], "longitude": c[1], "zoom": STATE_CENTROIDS[fallback_state]["zoom"]}
        return {"latitude": default_center[0], "longitude": default_center[1], "zoom": 4.8}

    center_lat = float(lats.mean())
    center_lon = float(lons.mean())
    lat_span = (lats.max() - lats.min()) if lats.max() != lats.min() else 0.01
    lon_span = (lons.max() - lons.min()) if lons.max() != lons.min() else 0.01
    span = max(lat_span, lon_span)

    # heurística simples para zoom a partir do span (valores calibrados)
    if span < 0.02:
        zoom = 12.5
    elif span < 0.2:
        zoom = 10.5
    elif span < 1.0:
        zoom = 8.5
    elif span < 3.0:
        zoom = 6.8
    else:
        zoom = 5.0

    return {"latitude": center_lat, "longitude": center_lon, "zoom": zoom}


# -----------------------------
# FUNÇÕES VALIDATION (telefone / email)
# -----------------------------
def validar_telefone(tel: str) -> bool:
    padrao = r'^\(\d{2}\) \d{4,5}-\d{4}$'
    return re.match(padrao, _safe_str(tel)) is not None


def validar_email(email: str) -> bool:
    padrao = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(padrao, _safe_str(email)) is not None


# -----------------------------
# INIT USUARIOS (arquivo local)
# -----------------------------
def init_usuarios():
    try:
        with open(USUARIOS_FILE, "r", encoding="utf-8") as f:
            usuarios = json.load(f)
            if not isinstance(usuarios, dict):
                raise ValueError("Formato inválido de usuarios.json")
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        senha_hash = bcrypt.hashpw("admin123".encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        usuarios = {"admin": {"senha": senha_hash, "nivel": "editor"}}
        with open(USUARIOS_FILE, "w", encoding="utf-8") as f:
            json.dump(usuarios, f, indent=4, ensure_ascii=False)
    return usuarios


usuarios = init_usuarios()
usuario_cookie = cookies.get("usuario", "")
nivel_cookie = cookies.get("nivel", "")
logado = usuario_cookie != "" and nivel_cookie != ""
usuario_atual = usuario_cookie if logado else None
nivel_acesso = nivel_cookie if logado else None

# -----------------------------
# LOGIN
# -----------------------------
if not logado:
    st.title("🔐 Login de Acesso")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if usuario in usuarios and bcrypt.checkpw(senha.encode("utf-8"), usuarios[usuario]["senha"].encode("utf-8")):
            cookies["usuario"] = usuario
            cookies["nivel"] = usuarios[usuario]["nivel"]
            cookies.save()
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha incorretos!")
    st.stop()

st.sidebar.write(f"👤 {usuario_atual} ({nivel_acesso})")
if st.sidebar.button("🚪 Sair"):
    cookies["usuario"] = ""
    cookies["nivel"] = ""
    cookies.save()
    st.experimental_rerun()

# -----------------------------
# Carregar dados inicial (sessão)
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()
if "cidade_busca" not in st.session_state:
    st.session_state.cidade_busca = ""
if "estado_filtro" not in st.session_state:
    st.session_state.estado_filtro = ""
if "distribuidores_selecionados" not in st.session_state:
    st.session_state.distribuidores_selecionados = []

# -----------------------------
# UI: menu lateral
# -----------------------------
menu = ["Cadastro", "Lista / Editar / Excluir", "Mapa"]
choice = st.sidebar.radio("Navegação", menu)

# =============================
# CADASTRO
# =============================
if choice == "Cadastro" and nivel_cookie == "editor":
    st.subheader("Cadastrar Novo Distribuidor")
    col1, col2 = st.columns(2)
    with col1:
        estados = carregar_estados()
        siglas = [e["sigla"] for e in estados]
        estado_sel = st.selectbox("Estado", siglas)
        cidades = [c["nome"] for c in carregar_cidades(estado_sel)] if estado_sel else []
        cidades_sel = st.multiselect("Cidades", cidades)
    with col2:
        nome = st.text_input("Nome do Distribuidor")
        contato = st.text_input("Contato (formato: (XX) XXXXX-XXXX)")
        email = st.text_input("Email")

    if st.button("Adicionar Distribuidor"):
        if not nome.strip() or not contato.strip() or not email.strip() or not estado_sel or not cidades_sel:
            st.error("Preencha todos os campos!")
        elif not validar_telefone(contato.strip()):
            st.error("Contato inválido! Use o formato (XX) XXXXX-XXXX")
        elif not validar_email(email.strip()):
            st.error("Email inválido!")
        elif nome in st.session_state.df["Distribuidor"].tolist():
            st.error("Distribuidor já cadastrado!")
        else:
            cidades_ocupadas = []
            for c in cidades_sel:
                if c in st.session_state.df["Cidade"].tolist() and not cidade_eh_capital(c, estado_sel):
                    dist_existente = st.session_state.df.loc[st.session_state.df["Cidade"] == c, "Distribuidor"].iloc[0]
                    cidades_ocupadas.append(f"{c} (atualmente atribuída a {dist_existente})")
            if cidades_ocupadas:
                st.error("As seguintes cidades já estão atribuídas a outros distribuidores:\n" + "\n".join(cidades_ocupadas))
            else:
                novos = []
                for c in cidades_sel:
                    lat, lon = geocode_with_cache(c, estado_sel)
                    # converter e validar intervalo
                    try:
                        if lat is None or lon is None:
                            lat_v, lon_v = pd.NA, pd.NA
                        else:
                            lat_v = float(lat)
                            lon_v = float(lon)
                            if not (-35.0 <= lat_v <= 6.0 and -82.0 <= lon_v <= -30.0):
                                lat_v, lon_v = pd.NA, pd.NA
                    except:
                        lat_v, lon_v = pd.NA, pd.NA
                    novos.append([nome, contato, email, estado_sel, c, lat_v, lon_v])
                novo_df = pd.DataFrame(novos, columns=COLUNAS)
                st.session_state.df = pd.concat([st.session_state.df, novo_df], ignore_index=True)
                salvar_dados(st.session_state.df)
                st.session_state.df = carregar_dados()
                st.success(f"✅ Distribuidor '{nome}' adicionado!")

# =============================
# LISTA / EDITAR / EXCLUIR
# =============================
elif choice == "Lista / Editar / Excluir":
    st.subheader("Distribuidores Cadastrados")
    st.dataframe(st.session_state.df[["Distribuidor", "Contato", "Email", "Estado", "Cidade"]], use_container_width=True)

    if nivel_cookie == "editor":
        with st.expander("✏️ Editar"):
            if not st.session_state.df.empty:
                dist_edit = st.selectbox("Distribuidor", st.session_state.df["Distribuidor"].unique())
                dados = st.session_state.df[st.session_state.df["Distribuidor"] == dist_edit]
                nome_edit = st.text_input("Nome", value=dist_edit)
                contato_edit = st.text_input("Contato", value=dados.iloc[0]["Contato"])
                email_edit = st.text_input("Email", value=dados.iloc[0]["Email"])
                estado_edit = st.selectbox(
                    "Estado",
                    sorted(st.session_state.df["Estado"].unique()),
                    index=sorted(st.session_state.df["Estado"].unique()).index(dados.iloc[0]["Estado"])
                )
                cidades_disponiveis = [c["nome"] for c in carregar_cidades(estado_edit)]
                cidades_novas = st.multiselect("Cidades", cidades_disponiveis, default=dados["Cidade"].tolist())

                if st.button("Salvar Alterações"):
                    if not validar_telefone(contato_edit.strip()):
                        st.error("Contato inválido! Use o formato (XX) XXXXX-XXXX")
                    elif not validar_email(email_edit.strip()):
                        st.error("Email inválido!")
                    else:
                        outras_linhas = st.session_state.df[st.session_state.df["Distribuidor"] != dist_edit]
                        cidades_ocupadas = []
                        for cidade in cidades_novas:
                            if cidade in outras_linhas["Cidade"].tolist() and not cidade_eh_capital(cidade, estado_edit):
                                dist_existente = outras_linhas.loc[outras_linhas["Cidade"] == cidade, "Distribuidor"].iloc[0]
                                cidades_ocupadas.append(f"{cidade} (atualmente atribuída a {dist_existente})")
                        if cidades_ocupadas:
                            st.error("As seguintes cidades já estão atribuídas a outros distribuidores:\n" + "\n".join(cidades_ocupadas))
                        else:
                            st.session_state.df = st.session_state.df[st.session_state.df["Distribuidor"] != dist_edit]
                            novos = []
                            for cidade in cidades_novas:
                                lat, lon = geocode_with_cache(cidade, estado_edit)
                                try:
                                    if lat is None or lon is None:
                                        lat_v, lon_v = pd.NA, pd.NA
                                    else:
                                        lat_v = float(lat)
                                        lon_v = float(lon)
                                        if not (-35.0 <= lat_v <= 6.0 and -82.0 <= lon_v <= -30.0):
                                            lat_v, lon_v = pd.NA, pd.NA
                                except:
                                    lat_v, lon_v = pd.NA, pd.NA
                                novos.append([nome_edit, contato_edit, email_edit, estado_edit, cidade, lat_v, lon_v])
                            novo_df = pd.DataFrame(novos, columns=COLUNAS)
                            st.session_state.df = pd.concat([st.session_state.df, novo_df], ignore_index=True)
                            salvar_dados(st.session_state.df)
                            st.session_state.df = carregar_dados()
                            st.success("✅ Alterações salvas!")

        with st.expander("🗑️ Excluir"):
            if not st.session_state.df.empty:
                dist_del = st.selectbox("Distribuidor para excluir", st.session_state.df["Distribuidor"].unique())
                if st.button("Excluir Distribuidor"):
                    st.session_state.df = st.session_state.df[st.session_state.df["Distribuidor"] != dist_del]
                    salvar_dados(st.session_state.df)
                    st.session_state.df = carregar_dados()
                    st.success(f"🗑️ '{dist_del}' removido!")

# =============================
# MAPA (pydeck) - filtros na sidebar, busca por cidade mostrando mensagens/tabela
# =============================
elif choice == "Mapa":
    st.subheader("🗺️ Mapa de Distribuidores (pydeck)")

    # Sidebar filtros combinados
    st.sidebar.markdown("### 🔎 Filtros do Mapa")

    # garantir chaves de session_state
    if "estado_filtro" not in st.session_state:
        st.session_state.estado_filtro = ""
    if "cidade_busca" not in st.session_state:
        st.session_state.cidade_busca = ""
    if "distribuidores_selecionados" not in st.session_state:
        st.session_state.distribuidores_selecionados = []

    # Estado (com opção vazia)
    estados = carregar_estados()
    siglas = [e["sigla"] for e in estados]
    estado_options = [""] + siglas
    # calcular index seguro
    try:
        estado_index = 0 if st.session_state.estado_filtro == "" else estado_options.index(st.session_state.estado_filtro)
    except ValueError:
        estado_index = 0
    estado_filtro = st.sidebar.selectbox("Filtrar por Estado", estado_options, index=estado_index)
    st.session_state.estado_filtro = estado_filtro

    # Opções do multiselect Filtrar Distribuidores
    if estado_filtro:
        distribuidores_opcoes = st.session_state.df.loc[st.session_state.df["Estado"] == estado_filtro, "Distribuidor"].dropna().unique().tolist()
    else:
        distribuidores_opcoes = st.session_state.df["Distribuidor"].dropna().unique().tolist()
    distribuidores_opcoes = sorted(distribuidores_opcoes)

    distribuidores_selecionados = st.sidebar.multiselect(
        "Filtrar Distribuidores (opcional)",
        distribuidores_opcoes,
        default=st.session_state.distribuidores_selecionados
    )
    st.session_state.distribuidores_selecionados = [d for d in distribuidores_selecionados if d in distribuidores_opcoes]

    # Busca por cidade (lista filtrada por estado se houver)
    todas_cidades = carregar_todas_cidades()
    if estado_filtro:
        todas_cidades = [c for c in todas_cidades if c.endswith(f" - {estado_filtro}")]
    try:
        cidade_index = 0 if st.session_state.cidade_busca == "" else (todas_cidades.index(st.session_state.cidade_busca) + 1 if st.session_state.cidade_busca in todas_cidades else 0)
    except Exception:
        cidade_index = 0
    cidade_selecionada_sidebar = st.sidebar.selectbox("Buscar Cidade", [""] + todas_cidades, index=cidade_index)
    if cidade_selecionada_sidebar:
        st.session_state.cidade_busca = cidade_selecionada_sidebar

    # Botão limpar filtros: reseta session_state (sem rerun)
    if st.sidebar.button("Limpar filtros"):
        st.session_state.estado_filtro = ""
        st.session_state.distribuidores_selecionados = []
        st.session_state.cidade_busca = ""

    # Aplicar filtros combinados
    df_filtro = st.session_state.df.copy()
    if st.session_state.estado_filtro:
        df_filtro = df_filtro[df_filtro["Estado"] == st.session_state.estado_filtro]
    if st.session_state.distribuidores_selecionados:
        df_filtro = df_filtro[df_filtro["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

    # Se houve busca de cidade (prioridade de exibição de mensagem/tabela)
    if st.session_state.cidade_busca:
        try:
            cidade_nome, estado_sigla = st.session_state.cidade_busca.split(" - ")
            df_cidade = st.session_state.df[
                (st.session_state.df["Cidade"].str.lower() == cidade_nome.lower()) &
                (st.session_state.df["Estado"].str.upper() == estado_sigla.upper())
            ]
        except Exception:
            df_cidade = pd.DataFrame(columns=COLUNAS)

        # Mensagem e tabela conforme comportamento desejado
        if df_cidade.empty:
            st.warning(f"❌ Nenhum distribuidor encontrado em **{st.session_state.cidade_busca}**.")
            # Mostrar mapa centrado no estado (se escolhido) ou no BR
            zoom_to_state = None
            if st.session_state.estado_filtro:
                df_state = st.session_state.df[st.session_state.df["Estado"] == st.session_state.estado_filtro]
                df_state_map = preparar_dataframe_para_mapa(df_state, salvar_novas_coords=False)
                view = calculate_view_state(df_state_map, fallback_state=st.session_state.estado_filtro)
            else:
                view = calculate_view_state(pd.DataFrame(columns=["lat", "lon"]), fallback_state=None)

            # construir pydeck vazio (somente center)
            initial_view = pdk.ViewState(latitude=view["latitude"], longitude=view["longitude"], zoom=view["zoom"])
            st.pydeck_chart(pdk.Deck(initial_view_state=initial_view, layers=[]), use_container_width=True)
        else:
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{st.session_state.cidade_busca}**:")
            # Mostrar tabela com Distribuidor, Contato, Email
            st.dataframe(df_cidade[["Distribuidor", "Contato", "Email"]].reset_index(drop=True), use_container_width=True)

            # Criar mapa apenas com df_cidade (aplica filtro de distribuidores caso tenham sido selecionados)
            df_cidade_map = df_cidade.copy()
            if st.session_state.distribuidores_selecionados:
                df_cidade_map = df_cidade_map[df_cidade_map["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

            df_for_map = preparar_dataframe_para_mapa(df_cidade_map, salvar_novas_coords=True)
            # montar camada de pontos
            if df_for_map.empty:
                st.warning("Nenhuma coordenada válida disponível para os distribuidores encontrados; o mapa será centrado no estado.")
                view = calculate_view_state(pd.DataFrame(columns=["lat", "lon"]), fallback_state=st.session_state.estado_filtro)
                initial_view = pdk.ViewState(latitude=view["latitude"], longitude=view["longitude"], zoom=view["zoom"])
                st.pydeck_chart(pdk.Deck(initial_view_state=initial_view, layers=[]), use_container_width=True)
            else:
                df_for_map["color"] = df_for_map["Distribuidor"].apply(lambda n: int(cor_distribuidor_hex(n).lstrip("#"), 16) & 0xFFFFFF)
                # pydeck espera array de ints RGB ou rgba; converter hex->(r,g,b)
                def hex_to_rgb_int(h: str):
                    h = h.lstrip("#")
                    return [int(h[i:i+2], 16) for i in (0, 2, 4)]
                df_for_map["rgb"] = df_for_map["Distribuidor"].apply(lambda n: hex_to_rgb_int(cor_distribuidor_hex(n)))

                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=df_for_map,
                    get_position=["lon", "lat"],
                    get_fill_color="rgb",
                    get_radius=8000,  # em metros; ajustar conforme zoom
                    pickable=True,
                    radius_min_pixels=4,
                    radius_max_pixels=60,
                    auto_highlight=True
                )

                view = calculate_view_state(df_for_map, fallback_state=st.session_state.estado_filtro)
                tooltip = {
                    "html": "<b>{Distribuidor}</b><br/>{Cidade} - {Estado}<br/>{Contato}<br/>{Email}",
                    "style": {"backgroundColor": "white", "color": "black"}
                }
                deck = pdk.Deck(layers=[layer],
                                initial_view_state=pdk.ViewState(latitude=view["latitude"],
                                                                longitude=view["longitude"],
                                                                zoom=view["zoom"],
                                                                pitch=0),
                                tooltip=tooltip)
                st.pydeck_chart(deck, use_container_width=True)

    else:
        # Sem busca por cidade: aplicar filtros combinados e mostrar mapa geral
        df_for_map = preparar_dataframe_para_mapa(df_filtro, salvar_novas_coords=True)
        if df_for_map.empty:
            # fallback: centro do estado ou do Brasil
            view = calculate_view_state(pd.DataFrame(columns=["lat", "lon"]), fallback_state=st.session_state.estado_filtro)
            initial_view = pdk.ViewState(latitude=view["latitude"], longitude=view["longitude"], zoom=view["zoom"])
            st.pydeck_chart(pdk.Deck(initial_view_state=initial_view, layers=[]), use_container_width=True)
        else:
            df_for_map["rgb"] = df_for_map["Distribuidor"].apply(lambda n: [int(c, 16) for c in (cor_distribuidor_hex(n).lstrip("#")[0:2], cor_distribuidor_hex(n).lstrip("#")[2:4], cor_distribuidor_hex(n).lstrip("#")[4:6])])
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=df_for_map,
                get_position=["lon", "lat"],
                get_fill_color="rgb",
                get_radius=8000,
                pickable=True,
                radius_min_pixels=3,
                radius_max_pixels=60,
                auto_highlight=True
            )
            view = calculate_view_state(df_for_map, fallback_state=st.session_state.estado_filtro)
            tooltip = {
                "html": "<b>{Distribuidor}</b><br/>{Cidade} - {Estado}<br/>{Contato}<br/>{Email}",
                "style": {"backgroundColor": "white", "color": "black"}
            }
            deck = pdk.Deck(layers=[layer],
                            initial_view_state=pdk.ViewState(latitude=view["latitude"], longitude=view["longitude"], zoom=view["zoom"], pitch=0),
                            tooltip=tooltip)
            st.pydeck_chart(deck, use_container_width=True)
