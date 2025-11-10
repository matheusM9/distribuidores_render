# -------------------------------------------------------------
# DISTRIBUIDORES APP - STREAMLIT (GOOGLE SHEETS)
# Versão final completa — mantém 3 abas, login, filtros, mapa, edição segura,
# geocoding automático para Lat/Lon vazios, cache 5min, performance otimizada.
# Planilha base: ID = 1hxPKagOnMhBYI44G3vQHY_wQGv6iYTxHMd_0VLw2r-k (aba "Página1")
# Dependências:
# streamlit, streamlit-folium, folium, geopy, gspread, google-auth, pandas, requests,
# bcrypt, streamlit-cookies-manager, oauth2client (se precisar)
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

# Google Sheets (gspread + oauth)
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
    if isinstance(x, (float, int)) and not isinstance(x, bool):
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
    """Lê a aba inteira do Google Sheets, garante as colunas e sanitiza Latitude/Longitude."""
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

    # manter ordem e cópia
    df = df[COLUNAS].copy()

    # sanitizar lat/lon
    df["Latitude"] = df["Latitude"].apply(to_float_safe)
    df["Longitude"] = df["Longitude"].apply(to_float_safe)

    # validar faixa do Brasil
    # pandas >=1.3 uses between(..., inclusive="both"), older versions may accept inclusive=True
    try:
        df.loc[~df["Latitude"].between(-35.0, 6.0, inclusive="both"), "Latitude"] = pd.NA
        df.loc[~df["Longitude"].between(-82.0, -30.0, inclusive="both"), "Longitude"] = pd.NA
    except TypeError:
        # fallback for older pandas
        df.loc[~(df["Latitude"] >= -35.0) | ~(df["Latitude"] <= 6.0), "Latitude"] = pd.NA
        df.loc[~(df["Longitude"] >= -82.0) | ~(df["Longitude"] <= -30.0), "Longitude"] = pd.NA

    return df

def refresh_local_df():
    """Limpa o cache de carregar_dados e atualiza st.session_state.df."""
    try:
        carregar_dados.cache_clear()
    except Exception:
        pass
    st.session_state.df = carregar_dados()

# -----------------------------
# ESCRITA PONTUAL (append / editar linha preservando lat/lon)
# -----------------------------
def append_row_to_sheet(row_values):
    """Adiciona nova linha ao final do sheet (append_row)."""
    try:
        values = [row_values.get(c, "") if row_values.get(c, "") is not None else "" for c in COLUNAS]
        WORKSHEET.append_row(values, value_input_option="USER_ENTERED")
        return True, None
    except Exception as e:
        return False, str(e)

def get_header_and_rows():
    """Retorna header (lista) e rows (lista de listas) atuais do sheet."""
    all_values = WORKSHEET.get_all_values()
    if not all_values:
        return COLUNAS, []
    header = all_values[0]
    rows = all_values[1:]
    return header, rows

def find_matching_rows_indices(distribuidor, cidade=None, estado=None):
    """
    Retorna índices 1-based no sheet das linhas que correspondem ao distribuidor (e opcionalmente cidade e estado).
    """
    matches = []
    try:
        _, rows = get_header_and_rows()
        for i, row in enumerate(rows, start=2):  # start=2 porque header é linha 1
            val_dist = row[0] if len(row) >= 1 else ""
            val_cidade = row[4] if len(row) >= 5 else ""
            val_estado = row[3] if len(row) >= 4 else ""
            if str(val_dist).strip() == str(distribuidor).strip():
                if cidade and estado:
                    if str(val_cidade).strip().lower() == str(cidade).strip().lower() and str(val_estado).strip().upper() == str(estado).strip().upper():
                        matches.append(i)
                else:
                    matches.append(i)
    except Exception:
        pass
    return matches

def update_sheet_row_by_index_preserve_latlon(row_index, new_row_values):
    """
    Atualiza uma linha pelo índice 1-based preservando Latitude/Longitude caso novos valores venham vazios.
    new_row_values: dict com chaves de COLUNAS, pode omitir Latitude/Longitude para preservar.
    """
    try:
        row_vals = WORKSHEET.row_values(row_index)
        row_complete = [row_vals[i] if i < len(row_vals) else "" for i in range(len(COLUNAS))]

        new_row = []
        for i, col in enumerate(COLUNAS):
            if col in new_row_values and new_row_values[col] is not None and new_row_values[col] != "":
                val = new_row_values[col]
            else:
                val = row_complete[i]
            if val is None:
                val = ""
            new_row.append(val)
        start_col = "A"
        end_col = chr(ord("A") + len(COLUNAS) - 1)
        cell_range = f"{start_col}{row_index}:{end_col}{row_index}"
        WORKSHEET.update(cell_range, [new_row], value_input_option="USER_ENTERED")
        return True, None
    except Exception as e:
        return False, str(e)

# -----------------------------
# GEOCODING AUTOMÁTICO (para Lat/Lon vazios)
# -----------------------------
GEOCODER_USER_AGENT = "distribuidores_app_v1"
geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT, timeout=6)

def geocode_city_state(cidade, estado):
    """Tenta obter lat/lon para uma cidade+estado com Nominatim (OpenStreetMap)."""
    try:
        query = f"{cidade}, {estado}, Brasil"
        location = geolocator.geocode(query)
        if location:
            return location.latitude, location.longitude
    except (GeocoderTimedOut, GeocoderUnavailable):
        return None, None
    except Exception:
        return None, None
    return None, None

def autopopulate_missing_coords(max_updates=10, delay_between=1.0):
    """
    Procura linhas com Latitude/Longitude vazios e tenta geocodificar.
    Atualiza a planilha preservando demais colunas. Limita a max_updates por execução para não sobrecarregar.
    """
    try:
        df = st.session_state.df
    except Exception:
        return 0

    to_update = []
    for idx, row in df.iterrows():
        lat = row.get("Latitude", pd.NA)
        lon = row.get("Longitude", pd.NA)
        if pd.isna(lat) or pd.isna(lon):
            cidade = row.get("Cidade", "")
            estado = row.get("Estado", "")
            distribuidor = row.get("Distribuidor", "")
            if cidade and estado:
                to_update.append((idx, distribuidor, cidade, estado))
        if len(to_update) >= max_updates:
            break

    updated = 0
    for i, distribuidor, cidade, estado in to_update:
        latlon = geocode_city_state(cidade, estado)
        time.sleep(delay_between)
        if latlon and latlon[0] is not None:
            lat_v, lon_v = to_float_safe(latlon[0]), to_float_safe(latlon[1])
            if lat_lon_is_valid(lat_v, lon_v):
                sheet_indices = find_matching_rows_indices(distribuidor, cidade, estado)
                if sheet_indices:
                    for idx_sheet in sheet_indices:
                        new_vals = {"Latitude": lat_v, "Longitude": lon_v}
                        ok, err = update_sheet_row_by_index_preserve_latlon(idx_sheet, new_vals)
                        if ok:
                            updated += 1
                else:
                    row_values = {
                        "Distribuidor": distribuidor,
                        "Contato": "",
                        "Email": "",
                        "Estado": estado,
                        "Cidade": cidade,
                        "Latitude": lat_v,
                        "Longitude": lon_v
                    }
                    append_row_to_sheet(row_values)
                    updated += 1
    if updated > 0:
        refresh_local_df()
    return updated

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

@st.cache_data
def obter_geojson_estados():
    url = "https://servicodados.ibge.gov.br/api/v2/malhas/?formato=application/vnd.geo+json&qualidade=simplificada&incluir=estados"
    try:
        resp = requests.get(url, timeout=12)
        if resp.status_code == 200:
            geojson = resp.json()
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

def cor_distribuidor(nome):
    h = abs(hash(nome)) % 0xAAAAAA
    h += 0x111111
    return f"#{h:06X}"

# -----------------------------
# CRIAR MAPA (otimizado)
# -----------------------------
def criar_mapa(df, filtro_distribuidores=None, zoom_to_state=None, show_state_borders=True):
    default_location = [-14.2350, -51.9253]
    default_zoom = 5
    center = default_location
    zoom_start = default_zoom
    if zoom_to_state and isinstance(zoom_to_state, dict):
        center = zoom_to_state.get("center", default_location)
        zoom_start = zoom_to_state.get("zoom", default_zoom)

    mapa = folium.Map(location=center, zoom_start=zoom_start, tiles="CartoDB positron", control_scale=True)

    markers_group = folium.FeatureGroup(name="Distribuidores")
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
# LOGIN
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
# CARREGAR DADOS NA SESSÃO & AUTOPREENCHER COORDS
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()
if "cidade_busca" not in st.session_state:
    st.session_state.cidade_busca = ""

# Tentar autopreencher coords faltantes (limitar atualizações por execução para não sobrecarregar Nominatim)
if "last_autofill" not in st.session_state:
    st.session_state.last_autofill = 0

# faz autofill no máximo uma vez por 60s por sessão/usuário para evitar chamadas repetidas
AUTOFILL_COOLDOWN = 60
if time.time() - st.session_state.last_autofill > AUTOFILL_COOLDOWN:
    try:
        updated = autopopulate_missing_coords(max_updates=8, delay_between=1.0)
        if updated:
            st.session_state.last_autofill = time.time()
    except Exception:
        pass

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
                lat, lon = None, None
                try:
                    lat, lon = geocode_city_state(c, estado_sel)
                except:
                    lat, lon = None, None
                lat_v = to_float_safe(lat)
                lon_v = to_float_safe(lon)
                if not lat_lon_is_valid(lat_v, lon_v):
                    lat_v, lon_v = pd.NA, pd.NA
                novos.append({"Distribuidor": nome, "Contato": contato, "Email": email, "Estado": estado_sel, "Cidade": c, "Latitude": lat_v, "Longitude": lon_v})
            errors = []
            for row in novos:
                ok, err = append_row_to_sheet(row)
                if not ok:
                    errors.append(err)
            if errors:
                st.error("Erro ao adicionar algumas linhas: " + "; ".join(errors))
            else:
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
                        novas_linhas = []
                        for cidade in cidades_novas:
                            lat, lon = None, None
                            try:
                                lat, lon = geocode_city_state(cidade, estado_edit)
                            except:
                                lat, lon = None, None
                            lat_v = to_float_safe(lat)
                            lon_v = to_float_safe(lon)
                            if not lat_lon_is_valid(lat_v, lon_v):
                                lat_v, lon_v = pd.NA, pd.NA
                            novas_linhas.append({"Distribuidor": nome_edit, "Contato": contato_edit, "Email": email_edit, "Estado": estado_edit, "Cidade": cidade, "Latitude": lat_v, "Longitude": lon_v})

                        occ_indices = find_matching_rows_indices(dist_edit)
                        errors = []
                        for i, new_row in enumerate(novas_linhas):
                            if i < len(occ_indices):
                                idx = occ_indices[i]
                                ok, err = update_sheet_row_by_index_preserve_latlon(idx, new_row)
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

        with st.expander("🗑️ Excluir"):
            if not st.session_state.df.empty:
                dist_del = st.selectbox("Distribuidor para excluir", st.session_state.df["Distribuidor"].unique())
                if st.button("Excluir Distribuidor"):
                    try:
                        header, rows = get_header_and_rows()
                        rows_filtered = [r for r in rows if not (len(r) > 0 and r[0].strip() == dist_del)]
                        WORKSHEET.clear()
                        new_values = [header] + rows_filtered
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

    # Sidebar filtros
    st.sidebar.markdown("### 🔎 Filtros do Mapa")

    # garantir chaves em session_state
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

    # Distribuidores (opcional)
    if estado_filtro:
        distribuidores_opcoes = st.session_state.df.loc[st.session_state.df["Estado"] == estado_filtro, "Distribuidor"].dropna().unique().tolist()
    else:
        distribuidores_opcoes = st.session_state.df["Distribuidor"].dropna().unique().tolist()
    distribuidores_opcoes = sorted(distribuidores_opcoes)
    distribuidores_selecionados = st.sidebar.multiselect("Filtrar Distribuidores (opcional)", distribuidores_opcoes, default=st.session_state.distribuidores_selecionados)
    st.session_state.distribuidores_selecionados = [d for d in distribuidores_selecionados if d in distribuidores_opcoes]

    # Busca por cidade (dependente do estado)
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

    # Aplicar filtros locais ao dataframe (rápido)
    df_filtro = st.session_state.df.copy()
    if st.session_state.estado_filtro:
        df_filtro = df_filtro[df_filtro["Estado"] == st.session_state.estado_filtro]
    if st.session_state.distribuidores_selecionados:
        df_filtro = df_filtro[df_filtro["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

    # Se houve busca por cidade -> comportamento especial (mensagem/tabela + mapa centrado)
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
            else:
                zoom_to_state = {"center":[-14.2350, -51.9253], "zoom":5}
            mapa = criar_mapa(pd.DataFrame(columns=COLUNAS), filtro_distribuidores=None, zoom_to_state=zoom_to_state)
            st_folium(mapa, width=1200, height=700)
        else:
            st.success(f"✅ {len(df_cidade)} distribuidor(es) encontrado(s) em **{st.session_state.cidade_busca}**:")
            st.dataframe(df_cidade[["Distribuidor","Contato","Email"]].reset_index(drop=True), use_container_width=True)

            df_cidade_map = df_cidade.copy()
            if st.session_state.distribuidores_selecionados:
                df_cidade_map = df_cidade_map[df_cidade_map["Distribuidor"].isin(st.session_state.distribuidores_selecionados)]

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
