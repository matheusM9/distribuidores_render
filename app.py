# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# Versão final: filtros sidebar, busca cidade com mensagem/tabela,
# limpeza de filtros, zoom por estado robusto, sanitização lat/lon.
# Base: https://docs.google.com/spreadsheets/d/1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k (aba "Página1")
# -------------------------------------------------------------

import os
import json
import re
import requests
import pandas as pd
import folium
import bcrypt
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

import streamlit as st
from streamlit_folium import st_folium
from streamlit_cookies_manager import EncryptedCookieManager

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, RefreshError

st.set_page_config(page_title="Distribuidores", layout="wide")

# -----------------------------
# CONFIGURAÇÃO GOOGLE SHEETS
# -----------------------------
SHEET_ID = "1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k"
SHEET_NAME = "Página1"
COLUNAS = ["Distribuidor", "Contato", "Email", "Estado", "Cidade", "Latitude", "Longitude"]

# -----------------------------
# Inicializar Google Sheets client
# -----------------------------
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
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
# FUNÇÕES DE DADOS (Sheets)
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
    for col in COLUNAS:
        if col not in df.columns:
            df[col] = ""

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
        s = s.replace(",", ".")
        s = s.replace(" ", "")
        try:
            return float(s)
        except:
            return pd.NA

    df["Latitude"] = df["Latitude"].apply(to_float_safe)
    df["Longitude"] = df["Longitude"].apply(to_float_safe)

    # Validar limites aproximados do Brasil (lat: -35..6, lon: -82..-30). Valores fora são considerados inválidos.
    df.loc[~df["Latitude"].between(-35.0, 6.0, inclusive="both"), "Latitude"] = pd.NA
    df.loc[~df["Longitude"].between(-82.0, -30.0, inclusive="both"), "Longitude"] = pd.NA

    return df


def salvar_dados(df):
    """Grava os dados no Google Sheets (sem cache)"""
    try:
        df2 = df.copy()
        df2 = df2[COLUNAS].fillna("")
        WORKSHEET.clear()
        WORKSHEET.update([df2.columns.values.tolist()] + df2.values.tolist())
        # limpar cache do carregamento de dados
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
# CAPITAIS BRASILEIRAS
# -----------------------------
CAPITAIS_BRASILEIRAS = [
    "Rio Branco-AC", "Maceió-AL", "Macapá-AP", "Manaus-AM", "Salvador-BA", "Fortaleza-CE",
    "Brasília-DF", "Vitória-ES", "Goiânia-GO", "São Luís-MA", "Cuiabá-MT", "Campo Grande-MS",
    "Belo Horizonte-MG", "Belém-PA", "João Pessoa-PB", "Curitiba-PR", "Recife-PE", "Teresina-PI",
    "Rio de Janeiro-RJ", "Natal-RN", "Porto Alegre-RS", "Boa Vista-RR", "Florianópolis-SC",
    "São Paulo-SP", "Aracaju-SE", "Palmas-TO"
]


def cidade_eh_capital(cidade, uf):
    return f"{cidade}-{uf}" in CAPITAIS_BRASILEIRAS


# -----------------------------
# CENTROIDES FIXOS POR UF (fallback seguro)
# -----------------------------
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
    "TO": {"center": [-9.45, -48.26], "zoom": 6},
}

# -----------------------------
# FUNÇÕES AUXILIARES (IBGE + GEO)
# -----------------------------
@st.cache_data
def carregar_estados():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    resp = requests.get(url)
    return sorted(resp.json(), key=lambda e: e["nome"])


@st.cache_data
def carregar_cidades(uf):
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
    resp = requests.get(url)
    return sorted(resp.json(), key=lambda c: c["nome"])


@st.cache_data
def carregar_todas_cidades():
    cidades = []
    estados = carregar_estados()
    for estado in estados:
        uf = estado["sigla"]
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
        resp = requests.get(url)
        if resp.status_code == 200:
            for c in resp.json():
                cidades.append(f"{c['nome']} - {uf}")
    return sorted(cidades)


def obter_coordenadas(cidade, estado):
    geolocator = Nominatim(user_agent="distribuidores_app", timeout=5)
    try:
        location = geolocator.geocode(f"{cidade}, {estado}, Brasil")
        if location:
            return location.latitude, location.longitude
        else:
            return "", ""
    except (GeocoderTimedOut, GeocoderUnavailable):
        return "", ""


@st.cache_data
def obter_geojson_cidade(cidade, estado_sigla):
    cidades_data = carregar_cidades(estado_sigla)
    cidade_info = next((c for c in cidades_data if c["nome"] == cidade), None)
    if not cidade_info:
        return None
    geojson_url = (
        f"https://servicodados.ibge.gov.br/api/v2/malhas/{cidade_info['id']}"
        "?formato=application/vnd.geo+json&qualidade=intermediaria"
    )
    try:
        resp = requests.get(geojson_url, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None


@st.cache_data
def obter_geojson_estados():
    url = (
        "https://servicodados.ibge.gov.br/api/v2/malhas/"
        "?formato=application/vnd.geo+json&qualidade=simplificada&incluir=estados"
    )
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            geojson = resp.json()
            for feature in geojson.get("features", []):
                feature["properties"]["style"] = {
                    "color": "#000000",
                    "weight": 3,
                    "dashArray": "0",
                    "fillOpacity": 0,
                }
            return geojson
    except:
        pass
    return None


def cor_distribuidor(nome):
    h = abs(hash(nome)) % 0xAAAAAA
    h += 0x111111
    return f"#{h:06X}"


# extrai coords recursivamente de geojson
def _extract_coords_from_geojson_coords(coords, out):
    if isinstance(coords[0], (float, int)):
        out.append((coords[1], coords[0]))
    else:
        for c in coords:
            _extract_coords_from_geojson_coords(c, out)


def _centroid_and_bbox_from_feature(feature):
    coords = []
    geom = feature.get("geometry", {})
    if not geom:
        return None, None
    _extract_coords_from_geojson_coords(geom.get("coordinates", []), coords)
    if not coords:
        return None, None
    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    centroid = [sum(lats) / len(lats), sum(lons) / len(lons)]
    bbox = [min(lats), min(lons), max(lats), max(lons)]
    return centroid, bbox


def _state_feature_by_sigla(geojson_estados, sigla):
    for feat in geojson_estados.get("features", []):
        props = feat.get("properties", {})
        if props.get("sigla") == sigla or props.get("UF") == sigla or props.get("ESTADO") == sigla:
            return feat
    for feat in geojson_estados.get("features", []):
        props = feat.get("properties", {})
        nome = props.get("nome") or props.get("NOME") or ""
        if sigla in nome:
            return feat
    return None


def criar_mapa(df, filtro_distribuidores=None, zoom_to_state=None):
    default_location = [-14.2350, -51.9253]
    zoom_start = 5
    if zoom_to_state and isinstance(zoom_to_state, dict):
        center = zoom_to_state.get("center", default_location)
        zoom_start = zoom_to_state.get("zoom", 6)
        mapa = folium.Map(location=center, zoom_start=zoom_start, tiles="CartoDB positron")
    else:
        mapa = folium.Map(location=default_location, zoom_start=zoom_start, tiles="CartoDB positron")

    for _, row in df.iterrows():
        if filtro_distribuidores and row["Distribuidor"] not in filtro_distribuidores:
            continue
        cidade = row.get("Cidade", "")
        estado = row.get("Estado", "")
        geojson = None
        try:
            if cidade and estado:
                geojson = obter_geojson_cidade(cidade, estado)
        except:
            geojson = None

        cor = cor_distribuidor(row.get("Distribuidor", ""))
        if geojson and "features" in geojson:
            try:
                folium.GeoJson(
                    geojson,
                    style_function=lambda feature, cor=cor: {
                        "fillColor": cor,
                        "color": "#666666",
                        "weight": 1.2,
                        "fillOpacity": 0.55,
                    },
                    tooltip=f"{row.get('Distribuidor','')} ({cidade} - {estado})",
                ).add_to(mapa)
            except:
                pass
        else:
            try:
                lat = row.get("Latitude", pd.NA)
                lon = row.get("Longitude", pd.NA)
                if pd.isna(lat) or pd.isna(lon):
                    continue
                if not (-35.0 <= lat <= 6.0 and -82.0 <= lon <= -30.0):
                    continue
                folium.CircleMarker(
                    location=[float(lat), float(lon)],
                    radius=8,
                    color="#333333",
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.8,
                    popup=f"{row.get('Distribuidor','')} ({cidade} - {estado})",
                ).add_to(mapa)
            except:
                continue

    geo_estados = obter_geojson_estados()
    if geo_estados:
        try:
            folium.GeoJson(
                geo_estados,
                name="Divisas Estaduais",
                style_function=lambda f: f.get("properties", {}).get("style", {
                    "color": "#000000",
                    "weight": 3,
                    "fillOpacity": 0,
                }),
                tooltip=folium.GeoJsonTooltip(fields=["nome"], aliases=["Estado:"]),
            ).add_to(mapa)
        except:
            pass

    folium.LayerControl().add_to(mapa)
    return mapa


# -----------------------------
# LOGIN PERSISTENTE
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
usuario_cookie = cookies.get("usuario", "")
nivel_cookie = cookies.get("nivel", "")
logado = usuario_cookie != "" and nivel_cookie != ""
usuario_atual = usuario_cookie if logado else None
nivel_acesso = nivel_cookie if logado else None

if not logado:
    st.title("🔐 Login de Acesso")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if usuario in usuarios and bcrypt.checkpw(senha.encode(), usuarios[usuario]["senha"].encode()):
            cookies["usuario"] = usuario
            cookies["nivel"] = usuarios[usuario]["nivel"]
            cookies.save()
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos!")
    st.stop()

st.sidebar.write(f"👤 {usuario_atual} ({nivel_acesso})")
if st.sidebar.button("🚪 Sair"):
    cookies["usuario"] = ""
    cookies["nivel"] = ""
    cookies.save()
    st.rerun()

# -----------------------------
# CARREGAR DADOS (sessão)
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()
if "cidade_busca" not in st.session_state:
    st.session_state.cidade_busca = ""

menu = ["Cadastro", "Lista / Editar / Excluir", "Mapa"]
choice = st.sidebar.radio("Navegação", menu)


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
                st.error(
                    "As seguintes cidades já estão atribuídas a outros distribuidores:\n"
                    + "\n".join(cidades_ocupadas)
                )
            else:
                novos = []
                for c in cidades_sel:
                    lat, lon = obter_coordenadas(c, estado_sel)
                    try:
                        if lat is None or lon is None or lat == "" or lon == "":
                            lat_v, lon_v = pd.NA, pd.NA
                        else:
                            lat_v = float(str(lat).replace(",", "."))
                            lon_v = float(str(lon).replace(",", "."))
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
    st.dataframe(st.session_state.df[["Distribuidor", "Contato", "Email", "Estado", "Cidade"]],
                 use_container_width=True)

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
                            st.error(
                                "As seguintes cidades já estão atribuídas a outros distribuidores:\n"
                                + "\n".join(cidades_ocupadas)
                            )
                        else:
                            st.session_state.df = st.session_state.df[st.session_state.df["Distribuidor"] != dist_edit]
                            novos = []
                            for cidade in cidades_novas:
                                lat, lon = obter_coordenadas(cidade, estado_edit)
                                try:
                                    if lat is None or lon is None or lat == "" or lon == "":
                                        lat_v, lon_v = pd.NA, pd.NA
                                    else:
                                        lat_v = float(str(lat).replace(",", "."))
                                        lon_v = float(str(lon).replace(",", "."))
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
# MAPA (filtros na sidebar, com busca de cidade mostrando mensagens/tabela)
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
    estado_options = [""] + siglas
    estado_index = 0 if st.session_state.estado_filtro == "" else estado_options.index(st.session_state.estado_filtro)
    estado_filtro = st.sidebar.selectbox("Filtrar por Estado", estado_options, index=estado_index)
    st.session_state.estado_filtro = estado_filtro

    # Opções do multiselect Filtrar Distribuidores
    if estado_filtro:
        distribuidores_opcoes = st.session_state.df.loc[st.session_state.df["Estado"] == estado_filtro, "Distribuidor"]\
            .dropna().unique().tolist()
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
    cidade_index = 0 if st.session_state.cidade_busca == "" else (
        todas_cidades.index(st.session_state.cidade_busca) + 1
        if st.session_state.cidade_busca in todas_cidades else 0
    )
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
            # Mesmo quando não há distribuidores, mostra mapa centrado no estado (se escolhido) ou no BR
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
                        zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 5}
            else:
                zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 5}

            mapa = criar_mapa(pd.DataFrame(columns=COLUNAS), filtro_distribuidores=None, zoom_to_state=zoom_to_state)
            st_folium(mapa, width=1200, height=700)
        else:
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{st.session_state.cidade_busca}**:")
            # Mostrar tabela com Distribuidor, Contato, Email
            st.dataframe(df_cidade[["Distribuidor", "Contato", "Email"]].reset_index(drop=True),
                         use_container_width=True)

            # Criar mapa apenas com df_cidade (aplica filtro de distribuidores caso tenham sido selecionados)
            df_cidade_map = df_cidade.copy()
            if st.session_state.distribuidores_selecionados:
                df_cidade_map = df_cidade_map[df_cidade_map["Distribuidor"].isin(
                    st.session_state.distribuidores_selecionados)]

            # calcular zoom centrado em df_cidade_map (se tem coords válidas)
            zoom_to_state = None
            lats = pd.to_numeric(df_cidade_map["Latitude"], errors="coerce").dropna()
            lons = pd.to_numeric(df_cidade_map["Longitude"], errors="coerce").dropna()
            lats = lats[(lats >= -35.0) & (lats <= 6.0)]
            lons = lons[(lons >= -82.0) & (lons <= -30.0)]
            if not lats.empty and not lons.empty:
                center_lat = float(lats.mean())
                center_lon = float(lons.mean())
                lat_span = lats.max() - lats.min() if lats.max() != lats.min() else 0.02
                lon_span = lons.max() - lons.min() if lons.max() != lons.min() else 0.02
                span = max(lat_span, lon_span)
                if span < 0.02:
                    zoom = 13
                elif span < 0.2:
                    zoom = 11
                elif span < 1.0:
                    zoom = 9
                else:
                    zoom = 8
                zoom_to_state = {"center": [center_lat, center_lon], "zoom": zoom}
            else:
                if st.session_state.estado_filtro and st.session_state.estado_filtro in STATE_CENTROIDS:
                    zoom_to_state = STATE_CENTROIDS[st.session_state.estado_filtro]
                else:
                    zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 5}

            mapa = criar_mapa(
                df_cidade_map,
                filtro_distribuidores=(st.session_state.distribuidores_selecionados
                                       if st.session_state.distribuidores_selecionados else None),
                zoom_to_state=zoom_to_state
            )
            st_folium(mapa, width=1200, height=700)
    else:
        # Sem busca por cidade: aplicar filtros combinados e mostrar mapa geral
        # df_filtro já aplicado por estado e por distribuidores selecionados acima
        # Determinar zoom/centro de forma robusta (evitar Antártida)
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
                    zoom_to_state = {"center": [-14.2350, -51.9253], "zoom": 5}

        mapa = criar_mapa(
            df_filtro,
            filtro_distribuidores=(st.session_state.distribuidores_selecionados
                                   if st.session_state.distribuidores_selecionados else None),
            zoom_to_state=zoom_to_state
        )
        st_folium(mapa, width=1200, height=700)
