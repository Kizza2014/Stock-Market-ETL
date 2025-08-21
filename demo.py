import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "zap"

st.title("Trino SQL Query Demo")

# Lưu lịch sử truy vấn trong session
if "history" not in st.session_state:
    st.session_state.history = []

# Kết nối Trino để lấy catalog/schema/table
def get_conn():
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        auth=None,
        catalog="hive",
        schema="default",
        http_scheme="http",
    )

conn = get_conn()
cur = conn.cursor()

# Lấy danh sách catalog
cur.execute("SHOW CATALOGS")
catalogs = [row[0] for row in cur.fetchall()]
catalog = st.selectbox("Chọn catalog", catalogs, index=catalogs.index("hive") if "hive" in catalogs else 0)

# Lấy danh sách schema
cur.execute(f"SHOW SCHEMAS FROM {catalog}")
schemas = [row[0] for row in cur.fetchall()]
schema = st.selectbox("Chọn schema", schemas, index=schemas.index("default") if "default" in schemas else 0)

# Gợi ý truy vấn mẫu
default_query = f"SELECT * FROM {catalog}.{schema}.fact_price LIMIT 10"
query = st.text_area("Nhập truy vấn SQL:", default_query)

if st.button("Thực thi"):
    try:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        st.dataframe(df)
        st.success(f"Số dòng: {len(df)}, Số cột: {len(df.columns)}")
        st.write("Preview 5 dòng đầu:")
        st.dataframe(df.head())
        # Lưu lịch sử
        st.session_state.history.append(query)
    except Exception as e:
        st.error(f"Lỗi: {e}")

# Hiển thị lịch sử truy vấn
if st.session_state.history:
    st.subheader("Lịch sử truy vấn")
    for i, q in enumerate(reversed(st.session_state.history[-5:]), 1):
        st.code(q, language="sql")