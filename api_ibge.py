import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
import requests

DATA_FILE = "distribuidores.csv"

# -----------------------------
# Funções IBGE
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

# -----------------------------
# Carregar CSV
# -----------------------------
def carregar_dados():
    try:
        df = pd.read_csv(DATA_FILE)
        for col in ["Distribuidor", "Contato", "Estado", "Cidade", "Latitude", "Longitude"]:
            if col not in df.columns:
                df[col] = ""
        df = df.dropna(subset=["Distribuidor", "Contato", "Estado", "Cidade"], how='all')
        return df[["Distribuidor", "Contato", "Estado", "Cidade", "Latitude", "Longitude"]]
    except FileNotFoundError:
        df = pd.DataFrame(columns=["Distribuidor", "Contato", "Estado", "Cidade", "Latitude", "Longitude"])
        df.to_csv(DATA_FILE, index=False)
        return df

# -----------------------------
# Inicializar session_state
# -----------------------------
if "df" not in st.session_state:
    st.session_state.df = carregar_dados()

# -----------------------------
# Cor fixa por distribuidor
# -----------------------------
def cor_distribuidor(nome):
    h = abs(hash(nome)) % 0xFFFFFF
    return f"#{h:06X}"

# -----------------------------
# Criar mapa
# -----------------------------
def criar_mapa(df):
    df_valid = df[(df["Latitude"].notna()) & (df["Longitude"].notna()) & (df["Latitude"] != "") & (df["Longitude"] != "")]
    if df_valid.empty:
        return folium.Map(location=[-14.2350, -51.9253], zoom_start=4)

    lats = df_valid["Latitude"].astype(float).tolist()
    lons = df_valid["Longitude"].astype(float).tolist()
    centro = [sum(lats)/len(lats), sum(lons)/len(lons)]
    mapa = folium.Map(location=centro, zoom_start=4)

    for _, row in df_valid.iterrows():
        lat = float(row["Latitude"])
        lon = float(row["Longitude"])
        folium.CircleMarker(
            location=[lat, lon],
            radius=6,
            color=cor_distribuidor(row["Distribuidor"]),
            fill=True,
            fill_color=cor_distribuidor(row["Distribuidor"]),
            popup=f"{row['Distribuidor']} ({row['Cidade']})"
        ).add_to(mapa)

    mapa.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]])
    return mapa

# -----------------------------
# Função que atualiza mapa imediatamente
# -----------------------------
def atualizar_mapa():
    mapa = criar_mapa(st.session_state.df)
    st_folium(mapa, width=700, height=500)

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("Sistema de Distribuidores Seguro")
tab1, tab2, tab3 = st.tabs(["Cadastro", "Lista / Editar", "Mapa"])

# --- Aba 1: Cadastro ---
with tab1:
    st.subheader("Cadastrar Novo Distribuidor")
    estados = carregar_estados()
    siglas = [e["sigla"] for e in estados]
    estado_selecionado = st.selectbox("Selecione o Estado", siglas)

    cidades = []
    if estado_selecionado:
        cidades_data = carregar_cidades(estado_selecionado)
        cidades = [c["nome"] for c in cidades_data]
    cidade_selecionada = st.selectbox("Selecione a Cidade", cidades)

    with st.form("novo_distribuidor"):
        nome = st.text_input("Nome do Distribuidor")
        contato = st.text_input("Contato")
        submitted = st.form_submit_button("Adicionar")
        if submitted:
            if not nome.strip() or not contato.strip() or not estado_selecionado or not cidade_selecionada:
                st.error("Preencha todos os campos!")
            else:
                geolocator = Nominatim(user_agent="distribuidores_app")
                location = geolocator.geocode(f"{cidade_selecionada}, {estado_selecionado}, Brasil")
                latitude = location.latitude if location else ""
                longitude = location.longitude if location else ""
                novo = pd.DataFrame([[nome.strip(), contato.strip(), estado_selecionado, cidade_selecionada, latitude, longitude]],
                                    columns=["Distribuidor", "Contato", "Estado", "Cidade", "Latitude", "Longitude"])
                st.session_state.df = pd.concat([st.session_state.df, novo], ignore_index=True)
                st.session_state.df.to_csv(DATA_FILE, index=False)
                st.success(f"Distribuidor {nome} adicionado!")

# --- Aba 2: Lista / Editar / Excluir ---
with tab2:
    st.subheader("Distribuidores Cadastrados")
    st.dataframe(st.session_state.df.drop(columns=["Latitude", "Longitude"]))

    st.markdown("---")
    st.subheader("Editar Distribuidor")
    if not st.session_state.df.empty:
        dist_para_editar = st.selectbox("Selecione distribuidor para editar", st.session_state.df["Distribuidor"].tolist())
        row = st.session_state.df[st.session_state.df["Distribuidor"] == dist_para_editar].iloc[0]

        nome_edit = st.text_input("Nome", value=row["Distribuidor"])
        contato_edit = st.text_input("Contato", value=row["Contato"])
        estado_edit = st.selectbox("Estado", siglas, index=siglas.index(row["Estado"]) if row["Estado"] in siglas else 0)

        cidades_edit = []
        try:
            cidades_data = carregar_cidades(estado_edit)
            cidades_edit = [c["nome"] for c in cidades_data]
        except:
            pass
        cidade_edit = st.selectbox("Cidade", cidades_edit, index=cidades_edit.index(row["Cidade"]) if row["Cidade"] in cidades_edit else 0)

        if st.button("Salvar Alterações"):
            if not nome_edit.strip() or not contato_edit.strip() or not estado_edit or not cidade_edit:
                st.error("Preencha todos os campos!")
            else:
                latitude, longitude = row["Latitude"], row["Longitude"]
                if latitude == "" or longitude == "":
                    geolocator = Nominatim(user_agent="distribuidores_app")
                    location = geolocator.geocode(f"{cidade_edit}, {estado_edit}, Brasil")
                    latitude = location.latitude if location else ""
                    longitude = location.longitude if location else ""
                st.session_state.df.loc[
                    st.session_state.df["Distribuidor"] == dist_para_editar,
                    ["Distribuidor", "Contato", "Estado", "Cidade", "Latitude", "Longitude"]
                ] = [nome_edit.strip(), contato_edit.strip(), estado_edit, cidade_edit, latitude, longitude]
                st.session_state.df.to_csv(DATA_FILE, index=False)
                st.success(f"Distribuidor {nome_edit} atualizado!")

    st.markdown("---")
    st.subheader("Excluir Distribuidor")
    if not st.session_state.df.empty:
        dist_para_excluir = st.selectbox("Selecione distribuidor para excluir", st.session_state.df["Distribuidor"].tolist())
        if st.button("Excluir Distribuidor"):
            st.session_state.df = st.session_state.df[st.session_state.df["Distribuidor"] != dist_para_excluir]
            st.session_state.df.to_csv(DATA_FILE, index=False)
            st.success(f"Distribuidor {dist_para_excluir} excluído!")

# --- Aba 3: Mapa ---
with tab3:
    st.subheader("Mapa de Distribuidores")
    atualizar_mapa()  # chama a função que sempre renderiza o mapa
