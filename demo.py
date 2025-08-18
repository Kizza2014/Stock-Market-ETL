import streamlit as st
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# Cấu hình kết nối Trino
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "zap"
# TRINO_PASSWORD = "your_trino_password"  # Nếu không có auth thì bỏ dòng này và auth=None

st.title("Trino SQL Query Demo")

query = st.text_area("Nhập truy vấn SQL:", "SELECT * FROM delta.default.fact_price LIMIT 10")

if st.button("Thực thi"):
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            auth=None,
            catalog="hive",
            schema="default",
            http_scheme="http",  # Nếu dùng https thì đổi lại
        )
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        st.dataframe([dict(zip(columns, row)) for row in rows])
    except Exception as e:
        st.error(f"Lỗi: {e}")