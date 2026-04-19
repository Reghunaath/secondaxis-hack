import streamlit as st
import pandas as pd

CSV_FILE = "listings.csv"

st.set_page_config(page_title="NEU Housing Listings", layout="wide")

@st.cache_data
def load_data():
    df = pd.read_csv(CSV_FILE, encoding="utf-8-sig", low_memory=False)
    df["monthly_rent"] = pd.to_numeric(df["monthly_rent"], errors="coerce")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["bathrooms"] = pd.to_numeric(df["bathrooms"], errors="coerce")
    df["walk_time_to_neu_min"] = pd.to_numeric(df["walk_time_to_neu_min"], errors="coerce")
    return df

df = load_data()

# ── Sidebar filters ────────────────────────────────────────────────────────────
st.sidebar.title("Filters")

listing_types = ["All"] + sorted(df["listing_type"].dropna().unique().tolist())
listing_type = st.sidebar.selectbox("Listing type", listing_types)

room_types = ["All"] + sorted(df["room_type"].dropna().unique().tolist())
room_type = st.sidebar.selectbox("Room type", room_types)

gender_opts = ["All"] + sorted(df["gender_preference"].dropna().unique().tolist())
gender = st.sidebar.selectbox("Gender preference", gender_opts)

food_opts = ["All"] + sorted(df["food_preference"].dropna().unique().tolist())
food = st.sidebar.selectbox("Food preference", food_opts)

neighborhood_opts = ["All"] + sorted(df["neighborhood"].dropna().unique().tolist())
neighborhood = st.sidebar.selectbox("Neighborhood", neighborhood_opts)

source_opts = ["All"] + sorted(df["source_group"].dropna().unique().tolist())
source = st.sidebar.selectbox("Source group", source_opts)

rent_min, rent_max = int(df["monthly_rent"].min(skipna=True) or 0), int(df["monthly_rent"].max(skipna=True) or 5000)
rent_range = st.sidebar.slider("Monthly rent ($)", rent_min, rent_max, (rent_min, rent_max))

red_eye = st.sidebar.checkbox("Red Eye accessible only")
no_broker = st.sidebar.checkbox("No broker fee only")

# ── Apply filters ──────────────────────────────────────────────────────────────
mask = pd.Series([True] * len(df), index=df.index)

if listing_type != "All":
    mask &= df["listing_type"] == listing_type
if room_type != "All":
    mask &= df["room_type"] == room_type
if gender != "All":
    mask &= df["gender_preference"] == gender
if food != "All":
    mask &= df["food_preference"] == food
if neighborhood != "All":
    mask &= df["neighborhood"].str.contains(neighborhood, na=False, case=False)
if source != "All":
    mask &= df["source_group"] == source

rent_mask = df["monthly_rent"].isna() | (
    (df["monthly_rent"] >= rent_range[0]) & (df["monthly_rent"] <= rent_range[1])
)
mask &= rent_mask

if red_eye:
    mask &= df["red_eye_accessible"] == True
if no_broker:
    mask &= df["broker_fee"].str.lower().eq("none")

filtered = df[mask].reset_index(drop=True)

# ── Header & KPIs ──────────────────────────────────────────────────────────────
st.title("NEU Housing Listings")
st.caption(f"Scraped from WhatsApp group chats · {len(df):,} total listings")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Showing", f"{len(filtered):,}")
c2.metric("Offering", int((filtered["listing_type"] == "offering").sum()))
c3.metric("Seeking", int((filtered["listing_type"] == "seeking").sum()))
avg_rent = filtered["monthly_rent"].mean()
c4.metric("Avg rent", f"${avg_rent:,.0f}" if not pd.isna(avg_rent) else "—")
med_walk = filtered["walk_time_to_neu_min"].median()
c5.metric("Median walk to NEU", f"{med_walk:.0f} min" if not pd.isna(med_walk) else "—")

st.divider()

# ── Charts ─────────────────────────────────────────────────────────────────────
col_a, col_b, col_c = st.columns(3)

with col_a:
    st.subheader("Room type")
    rt = filtered["room_type"].value_counts()
    if not rt.empty:
        st.bar_chart(rt)

with col_b:
    st.subheader("Rent distribution")
    rent_data = filtered["monthly_rent"].dropna()
    if not rent_data.empty:
        st.bar_chart(rent_data.value_counts(bins=10).sort_index())

with col_c:
    st.subheader("Neighborhood")
    # Explode comma-separated neighborhoods
    nbhd = (
        filtered["neighborhood"]
        .dropna()
        .str.split(", ")
        .explode()
        .str.strip()
        .value_counts()
        .head(10)
    )
    if not nbhd.empty:
        st.bar_chart(nbhd)

st.divider()

# ── Listings table ─────────────────────────────────────────────────────────────
st.subheader(f"Listings ({len(filtered):,})")

TABLE_COLS = [
    "post_date", "poster_name", "source_group",
    "listing_type", "accommodation_type", "room_type",
    "address", "neighborhood",
    "monthly_rent", "bedrooms", "bathrooms",
    "available_from", "lease_end_date",
    "gender_preference", "food_preference",
    "laundry", "furnished", "kitchen_equipped",
    "walk_time_to_neu_min", "transit_lines", "red_eye_accessible",
    "broker_fee", "contact_info",
]
display_cols = [c for c in TABLE_COLS if c in filtered.columns]
st.dataframe(filtered[display_cols], use_container_width=True, height=500)

# ── Raw message viewer ─────────────────────────────────────────────────────────
st.divider()
st.subheader("Raw message viewer")

if len(filtered) == 0:
    st.info("No listings match the current filters.")
else:
    idx = st.number_input(
        f"Select listing (0 – {len(filtered) - 1})", 0, len(filtered) - 1, 0
    )
    row = filtered.iloc[int(idx)]
    left, right = st.columns([1, 2])
    with left:
        for field in display_cols:
            val = row[field]
            if pd.notna(val) and str(val).strip():
                st.markdown(f"**{field}:** {val}")
    with right:
        st.text_area("Raw message", row["raw_message"], height=400)
