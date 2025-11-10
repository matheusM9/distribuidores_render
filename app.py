# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# Versão otimizada: cache 5min, escrita pontual, mapa mais leve.
# Mantém 3 abas, filtros na sidebar, busca cidade com mensagem/tabela.
# Dependências: streamlit, streamlit-folium, folium, gspread, oauth2client, geopy, pandas, requests, bcrypt, streamlit-cookies-manager
# Base: planilha ID: 1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k (aba "Página1")
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
import math
import time

# Google Sheets (gspread + oauth2client compatible)
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
# INICIALIZAÇÃO GSPREAD (USANDO service account em st.secrets)
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
            # cria a aba com colunas se não existir
            WORKSHEET = sh.add_worksheet(title=SHEET_NAME, rows="1000", cols=str(len(COLUNAS)))
            WORKSHEET.update([COLUNAS])
    except (DefaultCredentialsError, RefreshError, Exception) as e:
        st.error("Erro ao autenticar Google Sheets. Verifique o Secret da Service Account.\n" + str(e))
        st.stop()

init_gsheets()

# -----------------------------
# UTIL: sanitização e validação de coordenadas
# -----------------------------
def to_float_safe(x):
    if x is None:
        return pd.NA
    if isinstance(x, (float, int)) and not (isinstance(x, bool)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return pd.NA
    s = s.replace(",", ".")
    s = s.replace(" ", "")
    try:
        v = float(s)
        return v
    except:
        return pd.NA

def lat_lon_is_valid(lat, lon):
    try:
        if pd.isna(lat) or pd.isna(lon):
            return False
        lat = float(lat); lon = float(lon)
        return (-35.0 <= lat <= 6.0) and (-82.0 <= lon <= -30.0)
    except:
        return False

# -----------------------------
# CACHE E LEITURA (5 minutos)
# -----------------------------
@st.cache_data(ttl=300)
def carregar_dados():
    """Lê toda a aba do Google Sheets, garante colunas e sanitiza lat/lon."""
    try:
        records = WORKSHEET.get_all_records()
    except Exception as e:
        # retorna df vazio com colunas esperadas
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
    # garantir colunas
    for col in COLUNAS:
        if col not in df.columns:
            df[col] = ""

    df = df[COLUNAS].copy()

    # sanitizar lat/lon
    df["Latitude"] = df["Latitude"].apply(to_float_safe)
    df["Longitude"] = df["Longitude"].apply(to_float_safe)

    # validar faixa do Brasil
    df.loc[~df["Latitude"].between(-35.0, 6.0, inclusive="both"), "Latitude"] = pd.NA
    df.loc[~df["Longitude"].between(-82.0, -30.0, inclusive="both"), "Longitude"] = pd.NA

    return df

def refresh_local_df():
    """Forçar recarregamento do cache e atualizar session_state.df"""
    # Limpa cache da função e recarrega
    carregar_dados.cache_clear()
    df_new = carregar_dados()
    st.session_state.df = df_new

# -----------------------------
# ESCRITA PONTUAL (append / editar linha específica)
# -----------------------------
def append_row_to_sheet(row_values):
    """Adiciona uma nova linha ao final do sheet (usa append_row)."""
    try:
        # garante ordem de colunas
        values = [row_values.get(c, "") for c in COLUNAS]
        WORKSHEET.append_row(values, value_input_option="USER_ENTERED")
        return True, None
    except Exception as e:
        return False, str(e)

def find_row_index(distribuidor, cidade, estado):
    """Procura a primeira linha (índice 1-based) que combina Distribuidor + Cidade + Estado.
       Retorna None se não encontrado."""
    try:
        # Pesquisar por "Distribuidor" na coluna correspondente
        # Vamos obter a coluna inteira de Distribuidor e comparar localmente (evita muitas buscas)
        col_dist = WORKSHEET.col_values(1)  # coluna A, assumindo ordem COLUNAS
        # col_values inclui o cabeçalho; a primeira linha é o header
        for idx, val in enumerate(col_dist[1:], start=2):  # start=2 => 1-based index
            if str(val).strip() == str(distribuidor).strip():
                # conferir cidade e estado nessa linha
                row = WORKSHEET.row_values(idx)
                # row may be shorter; map to columns
                row_map = {}
                for i, c in enumerate(COLUNAS):
                    row_map[c] = row[i] if i < len(row) else ""
                if (str(row_map.get("Cidade","")).strip().lower() == str(cidade).strip().lower()
                    and str(row_map.get("Estado","")).strip().upper() == str(estado).strip().upper()):
                    return idx
        return None
    except Exception:
        return None

def update_sheet_row_by_index(row_index, row_values):
    """Atualiza uma linha inteira pelo índice (1-based)."""
    try:
        # Prepara valores na ordem de COLUNAS
        values = [row_values.get(c, "") for c in COLUNAS]
        # gspread: update takes A{row}:G{row}
        cell_range = f"A{row_index}:{chr(ord('A') + len(COLUNAS) - 1)}{row_index}"
        WORKSHEET.update(cell_range, [values], value_input_option="USER_ENTERED")
        return True, None
    except Exception as e:
        return False, str(e)

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
# CAPITAIS E HELPERS GEO
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

# centroides fixos (fallback rápido)
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

@st.cache_data
def obter_geojson_estados():
    url = "https://servicodados.ibge.gov.br/api/v2/malhas/?formato=application/vnd.geo+json&qualidade=simplificada&incluir=estados"
    try:
        resp = requests.get(url, timeout=12)
        if resp.status_code == 200:
            geojson = resp.json()
            # anexa style property para performance
            for feature in geojson.get("features", []):
                feature["properties"]["style"] = {
                    "color": "#000000",
                    "weight": 2.0,
                    "dashArray": "0",
                    "fillOpacity": 0
                }
            return geojson
    except:
        return None
    return None

@st.cache_data
def obter_geojson_cidade(cidade, estado_sigla):
    # Mantemos a função em cache para não buscar repetidamente
    try:
        cidades_data = carregar_cidades(estado_sigla)
        cidade_info = next((c for c in cidades_data if c["nome"].lower() == cidade.lower()), None)
        if not cidade_info:
            return None
        geojson_url = f"https://servicodados.ibge.gov.br/api/v2/malhas/{cidade_info['id']}?formato=application/vnd.geo+json&qualidade=intermediaria"
        resp = requests.get(geojson_url, timeout=8)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

@st.cache_data
def carregar_estados():
    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            return sorted(resp.json(), key=lambda e: e['nome'])
    except:
        return []
    return []

@st.cache_data
def carregar_cidades(uf):
    try:
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            return sorted(resp.json(), key=lambda c: c['nome'])
    except:
        return []
    return []

@st.cache_data
def carregar_todas_cidades():
    cidades = []
    estados = carregar_estados()
    for estado in estados:
        uf = estado["sigla"]
        try:
            url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{uf}/municipios"
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200:
                for c in resp.json():
                    cidades.append(f"{c['nome']} - {uf}")
        except:
            continue
    return sorted(cidades)

def cor_distribuidor(nome):
    h = abs(hash(nome)) % 0xAAAAAA
    h += 0x111111
    return f"#{h:06X}"

# -----------------------------
# CRIAR MAPA (otimizado)
# -----------------------------
def criar_mapa(df, filtro_distribuidores=None, zoom_to_state=None, show_state_borders=True):
    # cria mapa centrado já com zoom definido
    default_location = [-14.2350, -51.9253]
    default_zoom = 5
    center = default_location
    zoom_start = default_zoom
    if zoom_to_state and isinstance(zoom_to_state, dict):
        center = zoom_to_state.get("center", default_location)
        zoom_start = zoom_to_state.get("zoom", default_zoom)

    mapa = folium.Map(location=center, zoom_start=zoom_start, tiles="CartoDB positron", control_scale=True)

    # Adicionar marcadores (somente linhas com lat/lon válidas)
    # Construir uma FeatureGroup para performance
    markers_group = folium.FeatureGroup(name="Distribuidores")
    # iterar apenas nas linhas que terão marcadores
    df_iter = df.copy()
    for _, row in df_iter.iterrows():
        if filtro_distribuidores and row.get("Distribuidor") not in filtro_distribuidores:
            continue
        lat = row.get("Latitude", pd.NA)
        lon = row.get("Longitude", pd.NA)
        if pd.isna(lat) or pd.isna(lon):
            continue
        try:
            lat_f = float(lat); lon_f = float(lon)
            if not (-35.0 <= lat_f <= 6.0 and -82.0 <= lon_f <= -30.0):
                continue
            popup_html = f"<b>{row.get('Distribuidor','')}</b><br/>{row.get('Cidade','')} - {row.get('Estado','')}<br/>{row.get('Contato','')}<br/>{row.get('Email','')}"
            folium.CircleMarker(
                location=[lat_f, lon_f],
                radius=7,
                color="#333333",
                fill=True,
                fill_color=cor_distribuidor(row.get("Distribuidor","")),
                fill_opacity=0.85,
                popup=folium.Popup(popup_html, max_width=300)
            ).add_to(markers_group)
        except:
            continue

    mapa.add_child(markers_group)

    # Adicionar fronteiras estaduais (em cache)
    if show_state_borders:
        geo_estados = obter_geojson_estados()
        if geo_estados:
            folium.GeoJson(
                geo_estados,
                name="Divisas Estaduais",
                style_function=lambda f: f.get("properties", {}).get("style", {"color": "#000000", "weight":2, "fillOpacity":0}),
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
# CARREGAR DADOS NA SESSÃO (usa cache)
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()
if "cidade_busca" not in st.session_state:
    st.session_state.cidade_busca = ""

menu = ["Cadastro", "Lista / Editar / Excluir", "Mapa"]
choice = st.sidebar.radio("Navegação", menu)

# validações
def validar_telefone(tel):
    padrao = r'^\(\d{2}\) \d{4,5}-\d{4}$'
    return re.match(padrao, tel)

def validar_email(email):
    padrao = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(padrao, email)

# =============================
# CADASTRO
# =============================
if choice == "Cadastro" and nivel_acesso == "editor":
    st.subheader("Cadastrar Novo Distribuidor")
    col1, col2 = st.columns(2)
    with col1:
        estados = carregar_estados()
        siglas = [e["sigla"] for e in estados] if estados else []
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
            novos = []
            for c in cidades_sel:
                lat, lon = obter_coordenadas(c, estado_sel)
                # sanitize
                lat_v = to_float_safe(lat)
                lon_v = to_float_safe(lon)
                if not lat_lon_is_valid(lat_v, lon_v):
                    lat_v, lon_v = pd.NA, pd.NA
                novos.append({"Distribuidor": nome, "Contato": contato, "Email": email, "Estado": estado_sel, "Cidade": c, "Latitude": lat_v, "Longitude": lon_v})
            # gravar cada linha pontualmente (append)
            errors = []
            for row in novos:
                ok, err = append_row_to_sheet(row)
                if not ok:
                    errors.append(err)
            # atualizar session df local sem forçar reload inteiro (carregar_dados cache será limpo e recarregado)
            if errors:
                st.error("Erro ao adicionar algumas linhas: " + "; ".join(errors))
            else:
                # limpar cache e recarregar localmente
                refresh_local_df()
                st.success(f"✅ Distribuidor '{nome}' adicionado!")

# =============================
# LISTA / EDITAR / EXCLUIR
# =============================
elif choice == "Lista / Editar / Excluir":
    st.subheader("Distribuidores Cadastrados")
    st.dataframe(st.session_state.df[["Distribuidor","Contato","Email","Estado","Cidade"]], use_container_width=True)

    if nivel_acesso == "editor":
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
                        # Para editar: vamos remover linhas antigas do distribuidor e acrescentar novas
                        # Estratégia: localizar linhas que possuem Distribuidor == dist_edit e Cidade == cidade_antiga e Estado == estado_antigo, atualizar in-place
                        # Montar novos registros
                        novas_linhas = []
                        for cidade in cidades_novas:
                            lat, lon = obter_coordenadas(cidade, estado_edit)
                            lat_v = to_float_safe(lat); lon_v = to_float_safe(lon)
                            if not lat_lon_is_valid(lat_v, lon_v):
                                lat_v, lon_v = pd.NA, pd.NA
                            novas_linhas.append({"Distribuidor": nome_edit, "Contato": contato_edit, "Email": email_edit, "Estado": estado_edit, "Cidade": cidade, "Latitude": lat_v, "Longitude": lon_v})

                        # Localizar linhas antigas do dist_edit (pode haver múltiplas cidades) e atualizar a primeira igualable; se houver mais novas do que antigas, append extras
                        # Carregar todas as linhas atuais (valores brutos) para encontrar índices
                        # Buscamos por (Distribuidor original, Cidade original, Estado original)
                        # Obter lista de linhas do sheet com Distribuidor == dist_edit
                        # Simples estratégia: atualizar as primeiras N linhas encontradas com o mesmo distribuidor,
                        # e se quantidade novas > linhas existentes, append extras; se novas < existentes, apagar extras (remoção por reescrita completa é complexa - mantemos as extras como estão)
                        # Para evitar operações destrutivas na planilha, vamos: atualizar as primeiras len(novas) ocorrências e, se precisar adicionar mais, usamos append.
                        # Primeiro coletar todas as ocorrências na sheet
                        try:
                            col_dist = WORKSHEET.col_values(1)
                            occ_indices = []
                            for idx, val in enumerate(col_dist[1:], start=2):
                                if str(val).strip() == str(dist_edit).strip():
                                    # confirmar cidade/estado linha: pegar row_values
                                    row_vals = WORKSHEET.row_values(idx)
                                    # map to columns
                                    row_map = {}
                                    for i, c in enumerate(COLUNAS):
                                        row_map[c] = row_vals[i] if i < len(row_vals) else ""
                                    # independentemente da cidade, consideramos essa ocorrência
                                    occ_indices.append(idx)
                            # atualizar as primeiras len(novas_linhas) ocorrências
                            errors = []
                            for i, new_row in enumerate(novas_linhas):
                                if i < len(occ_indices):
                                    row_idx = occ_indices[i]
                                    ok, err = update_sheet_row_by_index(row_idx, new_row)
                                    if not ok:
                                        errors.append(err)
                                else:
                                    ok, err = append_row_to_sheet(new_row)
                                    if not ok:
                                        errors.append(err)
                            if errors:
                                st.error("Erro ao salvar alterações: " + "; ".join(errors))
                            else:
                                refresh_local_df()
                                st.success("✅ Alterações salvas!")
                        except Exception as e:
                            st.error("Erro ao atualizar planilha: " + str(e))

        with st.expander("🗑️ Excluir"):
            if not st.session_state.df.empty:
                dist_del = st.selectbox("Distribuidor para excluir", st.session_state.df["Distribuidor"].unique())
                if st.button("Excluir Distribuidor"):
                    # Para excluir: faremos uma remoção segura — obtenha todas linhas e reescreva sem as linhas do distribuidor.
                    try:
                        all_values = WORKSHEET.get_all_values()
                        header = all_values[0] if all_values else COLUNAS
                        rows = all_values[1:] if len(all_values) > 1 else []
                        rows_filtered = [r for r in rows if not (len(r) > 0 and r[0].strip() == dist_del)]
                        # reescrever planilha inteira (operaçao pesada, mas execução rara)
                        WORKSHEET.clear()
                        new_values = [header] + rows_filtered
                        # evitar chamar update com lista vazia
                        if new_values:
                            WORKSHEET.update(new_values, value_input_option="USER_ENTERED")
                        refresh_local_df()
                        st.success(f"🗑️ '{dist_del}' removido!")
                    except Exception as e:
                        st.error("Erro ao excluir: " + str(e))

# =============================
# MAPA (filtros na sidebar)
# =============================
elif choice == "Mapa":
    st.subheader("🗺️ Mapa de Distribuidores")

    # SIDEBAR: filtros
    st.sidebar.markdown("### 🔎 Filtros do Mapa")

    # garantir session_state keys
    if "estado_filtro" not in st.session_state:
        st.session_state.estado_filtro = ""
    if "cidade_busca" not in st.session_state:
        st.session_state.cidade_busca = ""
    if "distribuidores_selecionados" not in st.session_state:
        st.session_state.distribuidores_selecionados = []

    # Estado
    estados = carregar_estados()
    siglas = [e["sigla"] for e in estados] if estados else []
    estado_filtro = st.sidebar.selectbox("Filtrar por Estado", [""] + siglas, index=(0 if st.session_state.estado_filtro == "" else ([""] + siglas).index(st.session_state.estado_filtro) if st.session_state.estado_filtro in ([""] + siglas) else 0))
    st.session_state.estado_filtro = estado_filtro

    # Opções distribuidores (filtradas por estado se selecionado), sempre ordenar
    if estado_filtro:
        distribuidores_opcoes = st.session_state.df.loc[st.session_state.df["Estado"] == estado_filtro, "Distribuidor"].dropna().unique().tolist()
    else:
        distribuidores_opcoes = st.session_state.df["Distribuidor"].dropna().unique().tolist()
    distribuidores_opcoes = sorted(distribuidores_opcoes)
    distribuidores_selecionados = st.sidebar.multiselect("Filtrar Distribuidores (opcional)", distribuidores_opcoes, default=st.session_state.distribuidores_selecionados)
    st.session_state.distribuidores_selecionados = [d for d in distribuidores_selecionados if d in distribuidores_opcoes]

    # Busca por cidade
    todas_cidades = carregar_todas_cidades()
    if estado_filtro:
        todas_cidades = [c for c in todas_cidades if c.endswith(f" - {estado_filtro}")]
    cidade_index = 0 if st.session_state.cidade_busca == "" else (todas_cidades.index(st.session_state.cidade_busca) + 1 if st.session_state.cidade_busca in todas_cidades else 0)
    cidade_selecionada_sidebar = st.sidebar.selectbox("Buscar Cidade", [""] + todas_cidades, index=cidade_index)
    if cidade_selecionada_sidebar:
        st.session_state.cidade_busca = cidade_selecionada_sidebar

    # Botão limpar filtros (reseta session_state variáveis)
    if st.sidebar.button("Limpar filtros"):
        st.session_state.estado_filtro = ""
        st.session_state.distribuidores_selecionados = []
        st.session_state.cidade_busca = ""

    # Aplicar filtros combinados ao dataframe (rápido: operação local)
    df_filtro = st.session_state.df.copy()

    if st.session_state.estado_filtro:
        df_filtro = df_filtro[df_filtro["Estado"] == st.session_state.estado_filtro]

    if st.session_state.distribuidores_selecionados:
        df_filtro = df_filtro[df_filtro["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

    # Se houve busca por cidade: mostrar mensagem/tabela como no comportamento anterior
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
            # Mostrar mapa centrado no estado (se selecionado) ou Brasil
            zoom_to_state = None
            if st.session_state.estado_filtro:
                # tentar coords válidas do estado
                df_state = st.session_state.df[st.session_state.df["Estado"] == st.session_state.estado_filtro]
                lats = pd.to_numeric(df_state["Latitude"], errors="coerce").dropna()
                lons = pd.to_numeric(df_state["Longitude"], errors="coerce").dropna()
                lats = lats[(lats >= -35.0) & (lats <= 6.0)]
                lons = lons[(lons >= -82.0) & (lons <= -30.0)]
                if not lats.empty and not lons.empty:
                    center_lat = float(lats.mean()); center_lon = float(lons.mean())
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
                    zoom_to_state = {"center":[center_lat, center_lon], "zoom": zoom}
                else:
                    zoom_to_state = STATE_CENTROIDS.get(st.session_state.estado_filtro, {"center":[-14.2350, -51.9253], "zoom":5})
            else:
                zoom_to_state = {"center":[-14.2350, -51.9253], "zoom":5}
            mapa = criar_mapa(pd.DataFrame(columns=COLUNAS), filtro_distribuidores=None, zoom_to_state=zoom_to_state)
            st_folium(mapa, width=1200, height=700)
        else:
            # há distribuidores para a cidade
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{st.session_state.cidade_busca}**:")
            st.dataframe(df_cidade[["Distribuidor","Contato","Email"]].reset_index(drop=True), use_container_width=True)

            # aplicar filtro de distribuidores selecionados se houver
            df_cidade_map = df_cidade.copy()
            if st.session_state.distribuidores_selecionados:
                df_cidade_map = df_cidade_map[df_cidade_map["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

            # calcular zoom centrado na cidade (ou fallback)
            lats = pd.to_numeric(df_cidade_map["Latitude"], errors="coerce").dropna()
            lons = pd.to_numeric(df_cidade_map["Longitude"], errors="coerce").dropna()
            lats = lats[(lats >= -35.0) & (lats <= 6.0)]
            lons = lons[(lons >= -82.0) & (lons <= -30.0)]
            if not lats.empty and not lons.empty:
                center_lat = float(lats.mean()); center_lon = float(lons.mean())
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
                zoom_to_state = {"center":[center_lat, center_lon], "zoom": zoom}
            else:
                zoom_to_state = STATE_CENTROIDS.get(st.session_state.estado_filtro, {"center":[-14.2350, -51.9253], "zoom":5})

            mapa = criar_mapa(df_cidade_map, filtro_distribuidores=(st.session_state.distribuidores_selecionados if st.session_state.distribuidores_selecionados else None), zoom_to_state=zoom_to_state)
            st_folium(mapa, width=1200, height=700)
    else:
        # sem busca por cidade: exibir mapa com df_filtro (aplicado estado + distribuidores)
        zoom_to_state = None
        if st.session_state.estado_filtro:
            df_state = st.session_state.df[st.session_state.df["Estado"] == st.session_state.estado_filtro]
            lats = pd.to_numeric(df_state["Latitude"], errors="coerce").dropna()
            lons = pd.to_numeric(df_state["Longitude"], errors="coerce").dropna()
            lats = lats[(lats >= -35.0) & (lats <= 6.0)]
            lons = lons[(lons >= -82.0) & (lons <= -30.0)]
            if not lats.empty and not lons.empty:
                center_lat = float(lats.mean()); center_lon = float(lons.mean())
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
                zoom_to_state = {"center":[center_lat, center_lon], "zoom": zoom}
            else:
                zoom_to_state = STATE_CENTROIDS.get(st.session_state.estado_filtro, {"center":[-14.2350, -51.9253], "zoom":5})
        mapa = criar_mapa(df_filtro, filtro_distribuidores=(st.session_state.distribuidores_selecionados if st.session_state.distribuidores_selecionados else None), zoom_to_state=zoom_to_state)
        st_folium(mapa, width=1200, height=700)

# -----------------------------
# FIM DO ARQUIVO
# -----------------------------
