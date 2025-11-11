# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# Versão final: pydeck, geocodificação automática e salvamento de coords
# -------------------------------------------------------------

import streamlit as st
st.set_page_config(page_title="Distribuidores", layout="wide")

import os
import json
import re
import time
import math
import pandas as pd
import pydeck as pdk
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import requests
import bcrypt

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, RefreshError

# Cookies (login persistente)
try:
    from streamlit_cookies_manager import EncryptedCookieManager
except Exception:
    EncryptedCookieManager = None

# -----------------------------
# CONFIGURAÇÃO GOOGLE SHEETS
# -----------------------------
# sua planilha confirmada (IDs e aba)
SHEET_ID = "1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k"
SHEET_NAME = "Página1"

# columns expected in the sheet (case-sensitive)
COLUNAS = ["Distribuidor", "Contato", "Email", "Estado", "Cidade", "Latitude", "Longitude"]

# -----------------------------
# Inicializar Google Sheets client
# -----------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
GC = None
WORKSHEET = None

def init_gsheets():
    global GC, WORKSHEET
    if "gcp_service_account" not in st.secrets:
        st.error("❌ Google Service Account não configurada nos Secrets do Streamlit Cloud.")
        st.stop()
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPE)
        GC = gspread.authorize(creds)
        sh = GC.open_by_key(SHEET_ID)
        try:
            WORKSHEET = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            WORKSHEET = sh.add_worksheet(title=SHEET_NAME, rows="1000", cols=str(len(COLUNAS)))
            WORKSHEET.update([COLUNAS])
    except (DefaultCredentialsError, RefreshError, Exception) as e:
        st.error("Erro ao autenticar Google Sheets. Verifique o Secret da Service Account.\n" + str(e))
        st.stop()

init_gsheets()

# -----------------------------
# FUNÇÕES DE DADOS (Sheets) - cache para evitar leituras repetidas
# -----------------------------
@st.cache_data(ttl=300)
def carregar_dados():
    """Busca dados do Google Sheets, garante colunas e sanitiza lat/lon."""
    try:
        records = WORKSHEET.get_all_records()
    except Exception as e:
        st.error("Erro ao ler planilha: " + str(e))
        return pd.DataFrame(columns=COLUNAS)

    if not records:
        df = pd.DataFrame(columns=COLUNAS)
        try:
            WORKSHEET.clear()
            WORKSHEET.update([COLUNAS])
        except Exception:
            pass
        return df

    df = pd.DataFrame(records)
    # garante colunas esperadas
    for col in COLUNAS:
        if col not in df.columns:
            df[col] = ""
    df = df[COLUNAS].copy()

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

    # normaliza nomes para lowercase? mantemos colnames originais e criamos lat/lon numéricos na runtime
    df["Latitude"] = df["Latitude"].apply(to_float_safe)
    df["Longitude"] = df["Longitude"].apply(to_float_safe)

    # Validar limites aproximados do Brasil (lat: -35..6, lon: -82..-30)
    df.loc[~df["Latitude"].between(-35.0, 6.0, inclusive="both"), "Latitude"] = pd.NA
    df.loc[~df["Longitude"].between(-82.0, -30.0, inclusive="both"), "Longitude"] = pd.NA

    return df

def salvar_dados(df):
    """Grava os dados no Google Sheets (substitui todo o conteúdo). Limpar cache depois."""
    try:
        df2 = df.copy()
        df2 = df2[COLUNAS].fillna("")
        WORKSHEET.clear()
        WORKSHEET.update([df2.columns.values.tolist()] + df2.values.tolist())
        st.cache_data.clear()
    except Exception as e:
        st.error("Erro ao salvar dados na planilha: " + str(e))

# -----------------------------
# Geocodificação inteligente e escrita incremental
# -----------------------------
def geocode_unique_city_state(df, user_agent="distribuidores_app", sleep_between=1.0, max_calls=None, save_to_sheet=True):
    """
    Geocodifica apenas combinações únicas (Cidade, Estado) que ainda não possuem lat/lon.
    Atualiza o DataFrame e, se save_to_sheet=True, grava as linhas modificadas de volta na planilha.
    sleep_between: tempo entre chamadas ao Nominatim (recomendado 1s)
    max_calls: limite de chamadas (None = sem limite)
    """
    df = df.copy()
    # cria colunas se não existirem
    if "Latitude" not in df.columns:
        df["Latitude"] = pd.NA
    if "Longitude" not in df.columns:
        df["Longitude"] = pd.NA

    # identificar cidades sem coords
    need_geo = df[df["Latitude"].isna() | df["Longitude"].isna()]
    if need_geo.empty:
        return df  # nada a fazer

    # construir lista única (cidade, estado)
    unique_pairs = need_geo[["Cidade", "Estado"]].drop_duplicates().reset_index(drop=True)

    geolocator = Nominatim(user_agent=user_agent, timeout=10)

    # We'll store resolved coords in a dict to reuse across rows
    resolved = {}

    calls = 0
    for _, row in unique_pairs.iterrows():
        cidade = str(row["Cidade"]).strip()
        estado = str(row["Estado"]).strip()
        if not cidade or not estado or cidade.lower() in ["nan", "none"]:
            continue
        key = f"{cidade}|||{estado}"
        if key in resolved:
            continue
        # check if any existing row (even outside need_geo) already has coords for same pair
        existing = df[(df["Cidade"].str.strip().str.lower() == cidade.lower()) & (df["Estado"].str.strip().str.upper() == estado.upper()) & df["Latitude"].notna() & df["Longitude"].notna()]
        if not existing.empty:
            lat = float(existing.iloc[0]["Latitude"])
            lon = float(existing.iloc[0]["Longitude"])
            resolved[key] = (lat, lon)
            continue
        # perform geocode
        if max_calls is not None and calls >= max_calls:
            break
        try:
            query = f"{cidade}, {estado}, Brasil"
            location = geolocator.geocode(query)
            if location:
                lat = float(location.latitude)
                lon = float(location.longitude)
                # ensure bounds roughly within Brazil
                if -35.0 <= lat <= 6.0 and -82.0 <= lon <= -30.0:
                    resolved[key] = (lat, lon)
                else:
                    resolved[key] = (pd.NA, pd.NA)
            else:
                resolved[key] = (pd.NA, pd.NA)
        except (GeocoderTimedOut, GeocoderUnavailable, Exception):
            resolved[key] = (pd.NA, pd.NA)
        calls += 1
        time.sleep(sleep_between)

    # apply resolved coords back to dataframe
    modified_rows = []
    for idx, r in df.iterrows():
        cidade = str(r["Cidade"]).strip()
        estado = str(r["Estado"]).strip()
        key = f"{cidade}|||{estado}"
        if key in resolved:
            lat, lon = resolved[key]
            # only update if missing or invalid
            if pd.isna(r["Latitude"]) or pd.isna(r["Longitude"]):
                df.at[idx, "Latitude"] = lat if (lat is not None) else pd.NA
                df.at[idx, "Longitude"] = lon if (lon is not None) else pd.NA
                modified_rows.append(idx)

    # write back to sheet only the modified rows to avoid rewriting all (but gspread doesn't have row-by-row atomic update easily)
    # We'll fetch current sheet values, update only rows and write whole sheet to avoid complex range calculations.
    if save_to_sheet and modified_rows:
        try:
            # fetch full sheet raw values to preserve order, header etc
            all_values = WORKSHEET.get_all_values()
            if not all_values:
                # fallback to salvar_dados (recreate)
                salvar_dados(df)
            else:
                header = all_values[0]
                # build mapping col->index
                col_index = {c: header.index(c) for c in header if c in header}
                # prepare updated rows list
                updated_values = []
                # iterate rows and update when index matches modified_rows
                for i in range(1, len(all_values)):
                    row_vals = all_values[i]
                    # row index i -> df index i-1 if sheet and df align
                    # We'll rely on the sheet and df being in the same row order.
                    df_idx = i - 1
                    if df_idx in modified_rows:
                        # ensure row_vals has enough cols
                        row_vals_extended = row_vals + [""] * (len(header) - len(row_vals))
                        # update Latitude / Longitude in proper columns (if present)
                        if "Latitude" in header:
                            lat_col = header.index("Latitude")
                            row_vals_extended[lat_col] = "" if pd.isna(df.at[df_idx, "Latitude"]) else str(df.at[df_idx, "Latitude"])
                        if "Longitude" in header:
                            lon_col = header.index("Longitude")
                            row_vals_extended[lon_col] = "" if pd.isna(df.at[df_idx, "Longitude"]) else str(df.at[df_idx, "Longitude"])
                        updated_values.append((i + 1, row_vals_extended))  # sheet rows are 1-indexed + header
                # If we have updates, perform batch updates by rewriting whole sheet for simplicity/reliability
                if updated_values:
                    # write full df back to sheet (safer)
                    salvar_dados(df)
        except Exception as e:
            # do not block the app; just warn
            st.warning("Não foi possível gravar automaticamente as coordenadas na planilha: " + str(e))

    return df

# -----------------------------
# COOKIES (LOGIN PERSISTENTE)
# -----------------------------
if EncryptedCookieManager is None:
    st.warning("streamlit_cookies_manager não instalado: login persistente ficará desativado. Instale via pip para ativar.")
    cookies = None
else:
    cookies = EncryptedCookieManager(
        prefix="distribuidores_login",
        password="chave_secreta_segura_123"
    )
    if not cookies.ready():
        st.stop()

# -----------------------------
# CAPITAIS BRASILEIRAS + CENTROIDES
# -----------------------------
CAPITAIS_BRASILEIRAS = [
    "Rio Branco-AC","Maceió-AL","Macapá-AP","Manaus-AM","Salvador-BA","Fortaleza-CE",
    "Brasília-DF","Vitória-ES","Goiânia-GO","São Luís-MA","Cuiabá-MT","Campo Grande-MS",
    "Belo Horizonte-MG","Belém-PA","João Pessoa-PB","Curitiba-PR","Recife-PE","Teresina-PI",
    "Rio de Janeiro-RJ","Natal-RN","Porto Alegre-RS","Boa Vista-RR","Florianópolis-SC",
    "São Paulo-SP","Aracaju-SE","Palmas-TO"
]

def cidade_eh_capital(cidade, uf):
    return f"{cidade}-{uf}" in CAPITAIS_BRASILEIRAS

STATE_CENTROIDS = {
    "AC": {"center": [-8.77, -70.55], "zoom": 6},
    "AL": {"center": [-9.62, -36.82], "zoom": 7},
    "AP": {"center": [1.41, -51.77], "zoom": 6},
    "AM": {"center": [-3.07, -61.67], "zoom": 5},
    "BA": {"center": [-13.29, -41.71], "zoom": 6},
    "CE": {"center": [-5.20, -39.53], "zoom": 7},
    "DF": {"center": [-15.79, -47.88], "zoom": 10},
    "ES": {"center": [-19.19, -40.34], "zoom": 8},
    "GO": {"center": [-16.64, -49.31], "zoom": 7},
    "MA": {"center": [-2.55, -44.30], "zoom": 6},
    "MT": {"center": [-12.64, -55.42], "zoom": 5},
    "MS": {"center": [-20.51, -54.54], "zoom": 6},
    "MG": {"center": [-18.10, -44.38], "zoom": 6},
    "PA": {"center": [-5.53, -52.29], "zoom": 5},
    "PB": {"center": [-7.06, -35.55], "zoom": 7},
    "PR": {"center": [-24.89, -51.55], "zoom": 7},
    "PE": {"center": [-8.28, -35.07], "zoom": 7},
    "PI": {"center": [-7.71, -42.73], "zoom": 6},
    "RJ": {"center": [-22.90, -43.20], "zoom": 8},
    "RN": {"center": [-5.22, -36.52], "zoom": 7},
    "RS": {"center": [-30.03, -51.23], "zoom": 6},
    "RO": {"center": [-10.83, -63.34], "zoom": 6},
    "RR": {"center": [2.82, -60.67], "zoom": 6},
    "SC": {"center": [-27.33, -49.44], "zoom": 7},
    "SP": {"center": [-22.19, -48.79], "zoom": 7},
    "SE": {"center": [-10.90, -37.07], "zoom": 7},
    "TO": {"center": [-9.45, -48.26], "zoom": 6}
}

# -----------------------------
# FUNÇÕES AUXILIARES (IBGE + GEO) - cacheadas
# -----------------------------
@st.cache_data
def carregar_estados():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    resp = requests.get(url, timeout=10)
    return sorted(resp.json(), key=lambda e: e['nome'])

@st.cache_data
def carregar_cidades(uf):
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
    resp = requests.get(url, timeout=10)
    return sorted(resp.json(), key=lambda c: c['nome'])

@st.cache_data
def carregar_todas_cidades():
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
# FUNÇÕES DE MAPA (pydeck)
# -----------------------------
def preparar_df_para_mapa(df):
    """Prepara e reduz o DataFrame para o mapa (colunas mínimas, tipos corretos)."""
    df_map = df.copy()
    # nomes coerentes para pydeck
    df_map = df_map[["Distribuidor", "Estado", "Cidade", "Latitude", "Longitude"]].rename(
        columns={"Latitude": "latitude", "Longitude": "longitude"}
    )
    df_map["latitude"] = pd.to_numeric(df_map["latitude"], errors="coerce")
    df_map["longitude"] = pd.to_numeric(df_map["longitude"], errors="coerce")
    df_map = df_map.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    df_map = df_map[(df_map["latitude"].between(-35.0, 6.0)) & (df_map["longitude"].between(-82.0, -30.0))]
    return df_map

def calcular_centroid_e_zoom(df_map, estado_filtro=None):
    """Calcula centro e zoom aproximado para pydeck (zoom em escala 0-20)."""
    if not df_map.empty:
        lat_mean = float(df_map["latitude"].mean())
        lon_mean = float(df_map["longitude"].mean())
        lat_span = df_map["latitude"].max() - df_map["latitude"].min() if df_map.shape[0] > 1 else 0.01
        lon_span = df_map["longitude"].max() - df_map["longitude"].min() if df_map.shape[0] > 1 else 0.01
        span = max(lat_span, lon_span)
        if span < 0.02:
            zoom = 13
        elif span < 0.2:
            zoom = 11
        elif span < 1.0:
            zoom = 9
        elif span < 3.0:
            zoom = 7
        else:
            zoom = 5
        return [lat_mean, lon_mean], zoom

    # fallback: centro do estado ou Brasil
    if estado_filtro and estado_filtro in STATE_CENTROIDS:
        c = STATE_CENTROIDS[estado_filtro]
        return c["center"], c["zoom"]
    return [-14.2350, -51.9253], 4

def criar_pydeck(df_map, center, zoom, max_points=5000):
    """Cria objeto pydeck otimizado. Usa amostragem se houver muitos pontos."""
    df_plot = df_map.copy()
    total = len(df_plot)
    if total == 0:
        layer = pdk.Layer("ScatterplotLayer", data=[], get_position='[longitude, latitude]')
    else:
        if total > max_points:
            # amostragem proporcional por estado para preservar distribuição
            df_plot = df_plot.groupby('Estado', group_keys=False).apply(lambda x: x.sample(max(1, int(max_points * len(x)/total))))
            df_plot = df_plot.reset_index(drop=True)
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_plot,
            get_position='[longitude, latitude]',
            get_radius=30000,
            radius_scale=1,
            radius_min_pixels=4,
            radius_max_pixels=40,
            pickable=True,
            auto_highlight=True,
        )
    view_state = pdk.ViewState(latitude=center[0], longitude=center[1], zoom=zoom)
    tooltip = {"html": "<b>{Distribuidor}</b><br/>{Cidade} - {Estado}", "style": {"backgroundColor": "steelblue", "color": "white"}}
    deck = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip)
    return deck

# -----------------------------
# LOGIN PERSISTENTE - arquivo local como fallback
# -----------------------------
USUARIOS_FILE = "usuarios.json"

def init_usuarios():
    try:
        with open(USUARIOS_FILE, "r") as f:
            usuarios = json.load(f)
            if not isinstance(usuarios, dict):
                raise ValueError("Formato inválido")
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        senha_hash = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
        usuarios = {"admin": {"senha": senha_hash, "nivel": "editor"}}
        with open(USUARIOS_FILE, "w") as f:
            json.dump(usuarios, f, indent=4)
    return usuarios

usuarios = init_usuarios()
usuario_cookie = cookies.get("usuario", "") if cookies else ""
nivel_cookie = cookies.get("nivel", "") if cookies else ""
logado = usuario_cookie != "" and nivel_cookie != ""
usuario_atual = usuario_cookie if logado else None
nivel_acesso = nivel_cookie if logado else None

if not logado:
    st.title("🔐 Login de Acesso")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if usuario in usuarios and bcrypt.checkpw(senha.encode(), usuarios[usuario]["senha"].encode()):
            if cookies:
                cookies["usuario"] = usuario
                cookies["nivel"] = usuarios[usuario]["nivel"]
                cookies.save()
            st.experimental_rerun()
        else:
            st.error("Usuário ou senha incorretos!")
    st.stop()

st.sidebar.write(f"👤 {usuario_atual} ({nivel_acesso})")
if st.sidebar.button("🚪 Sair"):
    if cookies:
        cookies["usuario"] = ""
        cookies["nivel"] = ""
        cookies.save()
    st.experimental_rerun()

# -----------------------------
# CARREGAR DADOS (sessão)
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()
if "cidade_busca" not in st.session_state:
    st.session_state.cidade_busca = ""

menu = ["Cadastro", "Lista / Editar / Excluir", "Mapa"]
choice = st.sidebar.radio("Navegação", menu)

# validações simples
def validar_telefone(tel):
    padrao = r'^\(\d{2}\) \d{4,5}-\d{4}$'
    return re.match(padrao, tel)

def validar_email(email):
    padrao = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(padrao, email)

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
                    # Tentaremos obter coords aqui (geocoding local) mas não bloqueamos cadastro se não achar
                    lat, lon = "", ""
                    try:
                        location = Nominatim(user_agent="distribuidores_app", timeout=5).geocode(f"{c}, {estado_sel}, Brasil")
                        if location:
                            lat = float(location.latitude)
                            lon = float(location.longitude)
                            if not (-35.0 <= lat <= 6.0 and -82.0 <= lon <= -30.0):
                                lat, lon = pd.NA, pd.NA
                        else:
                            lat, lon = pd.NA, pd.NA
                    except:
                        lat, lon = pd.NA, pd.NA
                    novos.append([nome, contato, email, estado_sel, c, lat, lon])
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
    st.dataframe(st.session_state.df[["Distribuidor","Contato","Email","Estado","Cidade"]], use_container_width=True)

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
                                lat, lon = "", ""
                                try:
                                    location = Nominatim(user_agent="distribuidores_app", timeout=5).geocode(f"{cidade}, {estado_edit}, Brasil")
                                    if location:
                                        lat = float(location.latitude)
                                        lon = float(location.longitude)
                                        if not (-35.0 <= lat <= 6.0 and -82.0 <= lon <= -30.0):
                                            lat, lon = pd.NA, pd.NA
                                    else:
                                        lat, lon = pd.NA, pd.NA
                                except:
                                    lat, lon = pd.NA, pd.NA
                                novos.append([nome_edit, contato_edit, email_edit, estado_edit, cidade, lat, lon])
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
# MAPA (pydeck otimizado + geocoding)
# =============================
elif choice == "Mapa":
    st.subheader("🗺️ Mapa de Distribuidores")

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
    estado_filtro = st.sidebar.selectbox("Filtrar por Estado", [""] + siglas, index=(0 if st.session_state.estado_filtro == "" else ( [""] + siglas).index(st.session_state.estado_filtro)))
    st.session_state.estado_filtro = estado_filtro

    # Opções do multiselect Filtrar Distribuidores
    if estado_filtro:
        distribuidores_opcoes = st.session_state.df.loc[st.session_state.df["Estado"] == estado_filtro, "Distribuidor"].dropna().unique().tolist()
    else:
        distribuidores_opcoes = st.session_state.df["Distribuidor"].dropna().unique().tolist()
    distribuidores_opcoes = sorted(distribuidores_opcoes)

    distribuidores_selecionados = st.sidebar.multiselect("Filtrar Distribuidores (opcional)", distribuidores_opcoes, default=st.session_state.distribuidores_selecionados)
    st.session_state.distribuidores_selecionados = [d for d in distribuidores_selecionados if d in distribuidores_opcoes]

    # Busca por cidade (lista filtrada por estado se houver)
    todas_cidades = carregar_todas_cidades()
    if estado_filtro:
        todas_cidades = [c for c in todas_cidades if c.endswith(f" - {estado_filtro}")]
    cidade_index = 0 if st.session_state.cidade_busca == "" else (todas_cidades.index(st.session_state.cidade_busca) + 1 if st.session_state.cidade_busca in todas_cidades else 0)
    cidade_selecionada_sidebar = st.sidebar.selectbox("Buscar Cidade", [""] + todas_cidades, index=cidade_index)
    if cidade_selecionada_sidebar:
        st.session_state.cidade_busca = cidade_selecionada_sidebar

    # Botão limpar filtros
    if st.sidebar.button("Limpar filtros"):
        st.session_state.estado_filtro = ""
        st.session_state.distribuidores_selecionados = []
        st.session_state.cidade_busca = ""

    # Controle de renderização: opção para "only on click" para evitar rerender automático
    st.sidebar.markdown("---")
    atualizar_so_ao_clicar = st.sidebar.checkbox("Atualizar mapa somente ao clicar", value=False)
    render_button = None
    if atualizar_so_ao_clicar:
        render_button = st.sidebar.button("Renderizar mapa")

    # Preparar df com filtros
    df_filtro = st.session_state.df.copy()
    if st.session_state.estado_filtro:
        df_filtro = df_filtro[df_filtro["Estado"] == st.session_state.estado_filtro]
    if st.session_state.distribuidores_selecionados:
        df_filtro = df_filtro[df_filtro["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

    # Antes de renderizar: geocodificar (apenas se houver entradas sem lat/lon)
    # Usaremos geocode_unique_city_state para geocodificar combinações únicas e salvar no sheet
    # Importante: esse processo faz batches; com ~300 distribuidores será executado rapidamente (~300s se 1s por chamada)
    # Para acelerar teste inicial, reduzimos sleep_between = 1.0 e max_calls = None (padrão). Ajuste se necessário.
    with st.spinner("Verificando coordenadas (pode demorar um pouco na primeira vez)..."):
        df_filtro = geocode_unique_city_state(df_filtro, sleep_between=1.0, save_to_sheet=True)
        # atualizar sessão com possíveis coordenadas gravadas
        st.session_state.df = carregar_dados()
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

        if df_cidade.empty:
            st.warning(f"❌ Nenhum distribuidor encontrado em **{st.session_state.cidade_busca}**.")
            # calc fallback center
            zoom_to_state = None
            if st.session_state.estado_filtro:
                df_state = st.session_state.df[st.session_state.df["Estado"] == st.session_state.estado_filtro]
                lats = pd.to_numeric(df_state["Latitude"], errors="coerce").dropna()
                lons = pd.to_numeric(df_state["Longitude"], errors="coerce").dropna()
                lats = lats[(lats >= -35.0) & (lats <= 6.0)]
                lons = lons[(lons >= -82.0) & (lons <= -30.0)]
                if not lats.empty and not lons.empty:
                    center_lat = float(lats.mean())
                    center_lon = float(lons.mean())
                    lat_span = lats.max() - lats.min() if lats.max() != lats.min() else 0.1
                    lon_span = lons.max() - lons.min() if lons.max() != lons.min() else 0.1
                    span = max(lat_span, lon_span)
                    if span < 0.2:
                        zoom = 11
                    elif span < 1.0:
                        zoom = 9
                    elif span < 3.0:
                        zoom = 8
                    else:
                        zoom = 6
                    zoom_to_state = {"center": [center_lat, center_lon], "zoom": zoom}
                else:
                    if st.session_state.estado_filtro in STATE_CENTROIDS:
                        zoom_to_state = STATE_CENTROIDS[st.session_state.estado_filtro]
                    else:
                        zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 4}
            else:
                zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 4}

            # Mostrar um mapa vazio (com view_state)
            df_empty = pd.DataFrame(columns=["Distribuidor","latitude","longitude","Estado","Cidade"])
            center, zoom = (zoom_to_state["center"], zoom_to_state["zoom"]) if zoom_to_state else ([-14.2350, -51.9253], 4)
            deck = criar_pydeck(df_empty, center, zoom)
            if not atualizar_so_ao_clicar or (atualizar_so_ao_clicar and render_button):
                st.pydeck_chart(deck)
        else:
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{st.session_state.cidade_busca}**:")
            st.dataframe(df_cidade[["Distribuidor", "Contato", "Email"]].reset_index(drop=True), use_container_width=True)

            df_cidade_map = df_cidade.copy()
            if st.session_state.distribuidores_selecionados:
                df_cidade_map = df_cidade_map[df_cidade_map["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

            df_map_ready = preparar_df_para_mapa(df_cidade_map)
            center, zoom = calcular_centroid_e_zoom(df_map_ready, estado_filtro=st.session_state.estado_filtro)
            deck = criar_pydeck(df_map_ready, center, zoom)
            if not atualizar_so_ao_clicar or (atualizar_so_ao_clicar and render_button):
                st.pydeck_chart(deck)
    else:
        # Sem busca por cidade: aplicar filtros combinados e mostrar mapa geral
        df_map_ready = preparar_df_para_mapa(df_filtro)

        # Se muitos pontos, avisar e amostrar
        MAX_POINTS = 3000
        if len(df_map_ready) > MAX_POINTS:
            st.info(f"⚡ A base tem {len(df_map_ready)} pontos — mostrando amostra para desempenho (máx. {MAX_POINTS}).")

        center, zoom = calcular_centroid_e_zoom(df_map_ready, estado_filtro=st.session_state.estado_filtro)
        deck = criar_pydeck(df_map_ready, center, zoom, max_points=MAX_POINTS)
        if not atualizar_so_ao_clicar or (atualizar_so_ao_clicar and render_button):
            st.pydeck_chart(deck)

# -----------------------------
# DICAS DE DEPLOY e OPMIZACAO ADICIONAL
# -----------------------------
st.sidebar.markdown("---")
st.sidebar.markdown("**Dicas para reduzir ainda mais o tempo de carregamento:**")
st.sidebar.markdown("- Habilite `Atualizar mapa somente ao clicar` para evitar renderizações automáticas em cada ajuste de filtro.")
st.sidebar.markdown("- O app já cacheia a leitura de dados (`carregar_dados()`).")
st.sidebar.markdown("- Se tiver **> 10k** pontos, considere usar cluster ou heatmap e/ou um servidor de tiles.")
st.sidebar.markdown("- Para deploy no Streamlit Cloud: configure o secret `gcp_service_account` e não esqueça de instalar dependências.")

st.markdown("---")
st.markdown("**Pronto.** Versão do mapa otimizada com `pydeck` e geocodificação automática (salva coords na planilha).")
