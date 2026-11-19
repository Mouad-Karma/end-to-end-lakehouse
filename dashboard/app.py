from pathlib import Path

import altair as alt
import duckdb
import streamlit as st


GOLD = Path(__file__).resolve().parents[1] / "data" / "Gold"
MONTHLY_SALES = str(GOLD / "sales_monthly")
ORDER_ITEMS = str(GOLD / "fact_order_items")


def brl(amount):
    number = f"{amount:,.2f}".replace(",", " ").replace(".", ",")
    return f"{number} R$"


st.set_page_config(page_title="Olist | Ventes", page_icon="📊", layout="wide")
st.title("Ventes Olist")
st.caption("Commandes livrées · Couche Gold · Montants en réals brésiliens")

try:
    with duckdb.connect() as db:
        sales = db.execute(
            """
            SELECT make_date(purchase_year, purchase_month, 1) AS month,
                   total_orders, total_revenue, total_freight
            FROM delta_scan(?)
            WHERE purchase_year IS NOT NULL AND purchase_month IS NOT NULL
            ORDER BY month
            """,
            [MONTHLY_SALES],
        ).df()

        if sales.empty:
            st.info("La table Gold des ventes mensuelles est vide.")
            st.stop()

        years = sorted(sales["month"].dt.year.unique().tolist())
        controls, refresh = st.columns([3, 1], vertical_alignment="bottom")
        year = controls.selectbox("Année d’achat", [None, *years],
                                  format_func=lambda value: "Toutes les années" if value is None else str(value))
        refresh.button("Actualiser")

        if year is not None:
            sales = sales[sales["month"].dt.year == year]

        categories = db.execute(
            """
            SELECT category, SUM(price) AS revenue
            FROM delta_scan(?)
            WHERE order_status = 'delivered'
              AND purchase_year IS NOT NULL AND purchase_month IS NOT NULL
              AND (? IS NULL OR purchase_year = ?)
            GROUP BY category
            ORDER BY revenue DESC NULLS LAST, category
            LIMIT 10
            """,
            [ORDER_ITEMS, year, year],
        ).df()
except duckdb.Error as error:
    st.error("Impossible de lire les tables Gold. Vérifie que le pipeline Spark a créé les tables Delta.")
    st.code(str(error))
    st.stop()

revenue, orders, freight = st.columns(3)
revenue.metric("Chiffre d’affaires hors livraison", brl(sales["total_revenue"].sum()), border=True)
orders.metric("Commandes livrées", f"{sales['total_orders'].sum():,}".replace(",", " "), border=True)
freight.metric("Frais de livraison", brl(sales["total_freight"].sum()), border=True)

monthly_panel, category_panel = st.columns([3, 2], gap="medium")
with monthly_panel:
    st.subheader("Chiffre d’affaires par mois")
    monthly_chart = alt.Chart(sales).mark_line(point=True, color="#0F766E").encode(
        x=alt.X("month:T", title=None, axis=alt.Axis(format="%m/%Y")),
        y=alt.Y("total_revenue:Q", title="Chiffre d’affaires (R$)"),
        tooltip=[
            alt.Tooltip("month:T", title="Mois", format="%m/%Y"),
            alt.Tooltip("total_revenue:Q", title="Chiffre d’affaires (R$)", format=",.2f"),
            alt.Tooltip("total_orders:Q", title="Commandes livrées"),
        ],
    ).properties(height=340)
    st.altair_chart(monthly_chart, width="stretch")

with category_panel:
    st.subheader("Top 10 des catégories")
    category_chart = alt.Chart(categories).mark_bar(color="#2563EB", cornerRadiusEnd=3).encode(
        y=alt.Y("category:N", title=None, sort="-x"),
        x=alt.X("revenue:Q", title="Chiffre d’affaires (R$)"),
        tooltip=[
            alt.Tooltip("category:N", title="Catégorie"),
            alt.Tooltip("revenue:Q", title="Chiffre d’affaires (R$)", format=",.2f"),
        ],
    ).properties(height=340)
    st.altair_chart(category_chart, width="stretch")

with st.expander("Détail mensuel"):
    monthly_table = sales.assign(month=sales["month"].dt.strftime("%Y-%m")).rename(columns={
        "month": "Mois",
        "total_orders": "Commandes livrées",
        "total_revenue": "Chiffre d’affaires (R$)",
        "total_freight": "Livraison (R$)",
    })
    st.dataframe(monthly_table, hide_index=True)
