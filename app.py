# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# -------------------------------------------------------------
import streamlit as st
st.set_page_config(page_title="Distribuidores", layout="wide")

import os
import pandas as pd
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import requests
import json
import bcrypt
import re
from streamlit_cookies_manager import EncryptedCookieManager

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials
from google.auth.exceptions import DefaultCredentialsError, RefreshError

# -----------------------------
# CONFIGURAÇÃO GOOGLE SHEETS
# -----------------------------
SHEET_ID = "1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k"
SHEET_NAME = "Página1"
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
# FUNÇÕES DE DADOS (Sheets)
# -----------------------------

@st.cache_data(ttl=300)  # cache por 5 minutos
def carregar_dados():
    """Lê os dados do Google Sheets e mantém cache temporário para evitar excesso de requisições"""
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
    df = df[COLUNAS]
    return df


def salvar_dados(df):
    """Grava os dados no Google Sheets (sem cache)"""
    try:
        df2 = df.copy()
        df2 = df2[COLUNAS].fillna("")
        WORKSHEET.clear()
        WORKSHEET.update([df2.columns.values.tolist()] + df2.values.tolist())
        st.cache_data.clear()  # limpa cache para forçar recarregamento atualizado
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
    "Rio Branco-AC","Maceió-AL","Macapá-AP","Manaus-AM","Salvador-BA","Fortaleza-CE",
    "Brasília-DF","Vitória-ES","Goiânia-GO","São Luís-MA","Cuiabá-MT","Campo Grande-MS",
    "Belo Horizonte-MG","Belém-PA","João Pessoa-PB","Curitiba-PR","Recife-PE","Teresina-PI",
    "Rio de Janeiro-RJ","Natal-RN","Porto Alegre-RS","Boa Vista-RR","Florianópolis-SC",
    "São Paulo-SP","Aracaju-SE","Palmas-TO"
]

def cidade_eh_capital(cidade, uf):
    return f"{cidade}-{uf}" in CAPITAIS_BRASILEIRAS

# -----------------------------
# FUNÇÕES AUXILIARES (IBGE + GEO)
# -----------------------------
@st.cache_data
def carregar_estados():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    resp = requests.get(url)
    return sorted(resp.json(), key=lambda e: e['nome'])

@st.cache_data
def carregar_cidades(uf):
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
    resp = requests.get(url)
    return sorted(resp.json(), key=lambda c: c['nome'])

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
    geojson_url = f"https://servicodados.ibge.gov.br/api/v2/malhas/{cidade_info['id']}?formato=application/vnd.geo+json&qualidade=intermediaria"
    try:
        resp = requests.get(geojson_url, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

@st.cache_data
def obter_geojson_estados():
    url = "https://servicodados.ibge.gov.br/api/v2/malhas/?formato=application/vnd.geo+json&qualidade=simplificada&incluir=estados"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            geojson = resp.json()
            for feature in geojson.get("features", []):
                feature["properties"]["style"] = {
                    "color": "#000000",
                    "weight": 3,
                    "dashArray": "0",
                    "fillOpacity": 0
                }
            return geojson
    except:
        pass
    return None

def cor_distribuidor(nome):
    h = abs(hash(nome)) % 0xAAAAAA
    h += 0x111111
    return f"#{h:06X}"

def criar_mapa(df, filtro_distribuidores=None):
    mapa = folium.Map(location=[-14.2350, -51.9253], zoom_start=5, tiles="CartoDB positron")
    for _, row in df.iterrows():
        if filtro_distribuidores and row["Distribuidor"] not in filtro_distribuidores:
            continue
        cidade = row["Cidade"]
        estado = row["Estado"]
        geojson = obter_geojson_cidade(cidade, estado)
        cor = cor_distribuidor(row["Distribuidor"])
        if geojson and "features" in geojson:
            folium.GeoJson(
                geojson,
                style_function=lambda feature, cor=cor: {
                    "fillColor": cor,
                    "color": "#666666",
                    "weight": 1.2,
                    "fillOpacity": 0.55
                },
                tooltip=f"{row['Distribuidor']} ({cidade} - {estado})"
            ).add_to(mapa)
        else:
            try:
                lat = float(row["Latitude"]) if row["Latitude"] not in (None, "") else -14.2350
                lon = float(row["Longitude"]) if row["Longitude"] not in (None, "") else -51.9253
                folium.CircleMarker(
                   location=[lat, lon],
                   radius=12,
                   color="#333333",
                   fill=True,
                   fill_color=cor,
                   fill_opacity=0.6,
                   popup=f"{row['Distribuidor']} ({cidade} - {estado})"
                ).add_to(mapa)
            except:
                continue
    geo_estados = obter_geojson_estados()
    if geo_estados:
        folium.GeoJson(
            geo_estados,
            name="Divisas Estaduais",
            style_function=lambda f: f.get("properties", {}).get("style", {
                "color": "#000000",
                "weight": 3,
                "fillOpacity": 0
            }),
            tooltip=folium.GeoJsonTooltip(fields=["nome"], aliases=["Estado:"])
        ).add_to(mapa)
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
                st.error("As seguintes cidades já estão atribuídas a outros distribuidores:\n" + "\n".join(cidades_ocupadas))
            else:
                novos = []
                for c in cidades_sel:
                    lat, lon = obter_coordenadas(c, estado_sel)
                    novos.append([nome, contato, email, estado_sel, c, lat, lon])
                novo_df = pd.DataFrame(novos, columns=COLUNAS)
                st.session_state.df = pd.concat([st.session_state.df, novo_df], ignore_index=True)
                salvar_dados(st.session_state.df)
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
                                lat, lon = obter_coordenadas(cidade, estado_edit)
                                novos.append([nome_edit, contato_edit, email_edit, estado_edit, cidade, lat, lon])
                            novo_df = pd.DataFrame(novos, columns=COLUNAS)
                            st.session_state.df = pd.concat([st.session_state.df, novo_df], ignore_index=True)
                            salvar_dados(st.session_state.df)
                            st.success("✅ Alterações salvas!")

        with st.expander("🗑️ Excluir"):
            if not st.session_state.df.empty:
                dist_del = st.selectbox("Distribuidor para excluir", st.session_state.df["Distribuidor"].unique())
                if st.button("Excluir Distribuidor"):
                    st.session_state.df = st.session_state.df[st.session_state.df["Distribuidor"] != dist_del]
                    salvar_dados(st.session_state.df)
                    st.success(f"🗑️ '{dist_del}' removido!")

# =============================
# MAPA COM AUTOCOMPLETE E LIMPAR BUSCA
# =============================
elif choice == "Mapa":
    st.subheader("🗺️ Mapa de Distribuidores")
    distribuidores = st.multiselect("Filtrar Distribuidores", st.session_state.df["Distribuidor"].unique())
    st.markdown("### 🔎 Buscar Cidade")
    todas_cidades = carregar_todas_cidades()

    col1, col2 = st.columns([4,1])
    with col1:
        index_cidade = 0 if st.session_state.cidade_busca == "" else (todas_cidades.index(st.session_state.cidade_busca) + 1 if st.session_state.cidade_busca in todas_cidades else 0)
        cidade_selecionada = st.selectbox("Digite o nome da cidade e selecione:", [""] + todas_cidades, index=index_cidade)
    with col2:
        if st.button("Limpar busca"):
            st.session_state.cidade_busca = ""
            cidade_selecionada = ""

    if cidade_selecionada:
        st.session_state.cidade_busca = cidade_selecionada

    if st.session_state.cidade_busca:
        cidade_nome, estado_sigla = st.session_state.cidade_busca.split(" - ")
        df_cidade = st.session_state.df[
            (st.session_state.df["Cidade"].str.lower() == cidade_nome.lower()) &
            (st.session_state.df["Estado"].str.upper() == estado_sigla.upper())
        ]
        if df_cidade.empty:
            st.warning(f"❌ Nenhum distribuidor encontrado em **{cidade_nome} - {estado_sigla}**.")
        else:
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{cidade_nome} - {estado_sigla}**:")
            st.dataframe(df_cidade[["Distribuidor","Contato","Email","Estado","Cidade"]], use_container_width=True)
            mapa = criar_mapa(df_cidade)
            st_folium(mapa, width=1200, height=700)
    else:
        mapa = criar_mapa(st.session_state.df, filtro_distribuidores=distribuidores if distribuidores else None)
        st_folium(mapa, width=1200, height=700)
