import pandas as pd
import os
from datetime import date

ACTIVE_QUOTE_PATH = "./data_test/crawl_active_tickers/"

def clean_active_quote(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    csv_path = os.path.join(path, "most_active_quotes_parsed.csv")
    df = pd.read_csv(csv_path)
    
    # Xử lý dữ liệu
    # chỉ lấy thông tin về symbol, name, volume, avg_vol(3m) do mức độ hoạt động tích cực được đo dựa trên volume
    df = df[["symbol", "name", "volume", "avg_vol_(3m)"]]

    # chuyển volume và avg_vol_(3m) sang kiểu số và lưu lại đơn vị đo
    df["volume_value"] = df["volume"].str.extract(r'([\d\.]+)').astype(float)
    df["volume_unit"] = df["volume"].str.extract(r'([A-Za-z]+)')
    df["avg_volume_3m_value"] = df["avg_vol_(3m)"].str.extract(r'([\d\.]+)').astype(float)
    df["avg_volume_3m_unit"] = df["avg_vol_(3m)"].str.extract(r'([A-Za-z]+)')
    ## thêm hệ số scale tương ứng với đơn vị đo
    scale_factors = {
        "USD": 1,  # không có đơn vị
        "K": 1e3,
        "M": 1e6,
        "B": 1e9,
        "T": 1e12
    }
    df["volume_value_scale_factor"] = df["volume_unit"].map(scale_factors).fillna(1)
    df["avg_volume_3m_value_scale_factor"] = df["avg_volume_3m_unit"].map(scale_factors).fillna(1)

    # xoá các cột không cần thiết
    df.drop(columns=["volume", "avg_vol_(3m)"], inplace=True)
    
    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"most_active_quotes_cleaned.csv")
    df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


def clean_5_years_history(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    # đọc toàn bộ file csv đã được parsed
    available_csv = [f for f in os.listdir(path) if f.endswith('parsed.csv')] # chỉ chọn các file đã được parsed
    if not available_csv:
        print(f"Không tìm thấy file CSV nào trong thư mục {path}.")
        exit(1)
    
    dfs = []
    for file in available_csv:
        df = pd.read_csv(os.path.join(path, file))
        # Nếu muốn thêm tên mã vào dữ liệu, lấy từ tên file (giả sử tên file là SYMBOL.csv)
        symbol = os.path.splitext(file)[0].split("_")[0]
        df['symbol'] = symbol
        dfs.append(df)

    # gộp tất cả lại
    merged_df = pd.concat(dfs, ignore_index=True)

    # Xử lý dữ liệu
    ## format lại định dạng ngày tháng
    merged_df['date'] = pd.to_datetime(merged_df['date'], format='%b %d, %Y')

    # loại bỏ các giá trị NaN do ngày đó không có giao dịch (dividend, stock split, ...)
    merged_df.dropna(subset=['date', 'open', 'high', 'low','volume'], inplace=True)

    # chuẩn hoá tên một số trường
    merged_df.rename(
        columns={
            'closeclose_price_adjusted_for_splits.': 'close',
            'adj_closeadjusted_close_price_adjusted_for_splits_and_dividend_and/or_capital_gain_distributions.': 'adjusted_close',
        },
        inplace=True
    )

    # chuẩn hoá giá trị cho volume
    merged_df['volume'] = merged_df['volume'].str.replace(',', '', regex=False)

    # chuẩn hoá kiểu dữ liệu
    numeric_cols = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume']
    for col in numeric_cols:
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"all_5_years_history_cleaned.csv")
    merged_df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


def clean_profile(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    csv_file = os.path.join(path, "all_profiles_parsed.csv")
    if not os.path.exists(csv_file):
        print(f"Không tìm thấy file CSV đã parsed trong thư mục {path}.")
        exit(1)
    
    # đọc file csv và xử lý
    df = pd.read_csv(csv_file)

    # lưu dữ liệu đã xử lý
    cleaned_csv_path = os.path.join(path, f"all_profiles_cleaned.csv")
    df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")
    

def clean_income_statement(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    available_csv = [f for f in os.listdir(path) if f.endswith('parsed.csv')]  # chỉ chọn các file đã được parsed
    if not available_csv:
        print(f"Không tìm thấy file CSV nào trong thư mục {path}.")
        exit(1)
    
    dfs = []
    for file in available_csv:
        df = pd.read_csv(os.path.join(path, file))
        # lấy tên mã từ tên file (ticker_income_statement_parsed.csv)
        symbol = os.path.splitext(file)[0].split("_")[0]
        df['symbol'] = symbol

        # thay thế giá trị "--" bằng NaN
        df.replace("--", pd.NA, inplace=True)

        # bỏ dòng với breakdown = 'ttm'
        df = df[df['breakdown'] != 'ttm']

        dfs.append(df)

    # gộp tất cả dataframe lại
    merged_df = pd.concat(dfs, ignore_index=True)

    # chuẩn hoá kiểu dữ liệu
    ## ngày tháng
    merged_df['date'] = pd.to_datetime(merged_df['breakdown'], format='%m/%d/%Y')
    merged_df.drop(columns=['breakdown'], inplace=True)
    ## các cột số
    numeric_cols = merged_df.columns.difference(['symbol', 'date'])
    for col in numeric_cols:
        if merged_df[col].dtype == 'object':
            # chuyển đổi các giá trị về dạng số
            merged_df[col] = merged_df[col].str.replace(',', '', regex=False)  # loại bỏ dấu phẩy
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    # sắp xếp lại thứ tự các cột cho dễ nhìn
    cols = ['symbol', 'date'] + [col for col in merged_df.columns if col not in ['symbol', 'date']]
    merged_df = merged_df[cols]

    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"income_statement_cleaned.csv")
    merged_df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


def clean_balance_sheet(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    available_csv = [f for f in os.listdir(path) if f.endswith('parsed.csv')]  # chỉ chọn các file đã được parsed
    if not available_csv:
        print(f"Không tìm thấy file CSV nào trong thư mục {path}.")
        exit(1)
    
    dfs = []
    for file in available_csv:
        df = pd.read_csv(os.path.join(path, file))
        # lấy tên mã từ tên file (ticker_balance_sheet_parsed.csv)
        symbol = os.path.splitext(file)[0].split("_")[0]
        df['symbol'] = symbol

        # thay thế giá trị "--" bằng NaN
        df.replace("--", pd.NA, inplace=True)

        # bỏ dòng với breakdown = 'ttm'
        df = df[df['breakdown'] != 'ttm']

        dfs.append(df)

    # gộp tất cả dataframe lại
    merged_df = pd.concat(dfs, ignore_index=True)

    # chuẩn hoá kiểu dữ liệu
    ## ngày tháng
    merged_df['date'] = pd.to_datetime(merged_df['breakdown'], format='%m/%d/%Y')
    merged_df.drop(columns=['breakdown'], inplace=True)
    ## các cột số
    numeric_cols = merged_df.columns.difference(['symbol', 'date'])
    for col in numeric_cols:
        if merged_df[col].dtype == 'object':
            # chuyển đổi các giá trị về dạng số
            merged_df[col] = merged_df[col].str.replace(',', '', regex=False)  # loại bỏ dấu phẩy
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    # sắp xếp lại thứ tự các cột cho dễ nhìn
    cols = ['symbol', 'date'] + [col for col in merged_df.columns if col not in ['symbol', 'date']]
    merged_df = merged_df[cols]

    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"balance_sheet_cleaned.csv")
    merged_df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


def clean_cash_flow(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    available_csv = [f for f in os.listdir(path) if f.endswith('parsed.csv')]  # chỉ chọn các file đã được parsed
    if not available_csv:
        print(f"Không tìm thấy file CSV nào trong thư mục {path}.")
        exit(1)
    
    dfs = []
    for file in available_csv:
        df = pd.read_csv(os.path.join(path, file))
        # lấy tên mã từ tên file (ticker_cash_flow_parsed.csv)
        symbol = os.path.splitext(file)[0].split("_")[0]
        df['symbol'] = symbol

        # thay thế giá trị "--" bằng NaN
        df.replace("--", pd.NA, inplace=True)

        # bỏ dòng với breakdown = 'ttm'
        df = df[df['breakdown'] != 'ttm']

        dfs.append(df)

    # gộp tất cả dataframe lại
    merged_df = pd.concat(dfs, ignore_index=True)

    # chuẩn hoá kiểu dữ liệu
    ## ngày tháng
    merged_df['date'] = pd.to_datetime(merged_df['breakdown'], format='%m/%d/%Y')
    merged_df.drop(columns=['breakdown'], inplace=True)
    ## các cột số
    numeric_cols = merged_df.columns.difference(['symbol', 'date'])
    for col in numeric_cols:
        if merged_df[col].dtype == 'object':
            # chuyển đổi các giá trị về dạng số
            merged_df[col] = merged_df[col].str.replace(',', '', regex=False)  # loại bỏ dấu phẩy
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

    # sắp xếp lại thứ tự các cột cho dễ nhìn
    cols = ['symbol', 'date'] + [col for col in merged_df.columns if col not in ['symbol', 'date']]
    merged_df = merged_df[cols]

    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"cash_flow_cleaned.csv")
    merged_df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


def clean_statistics(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file CSV nào.")
        exit(1)

    available_csv = [f for f in os.listdir(path) if f.endswith('parsed.csv')]  # chỉ chọn các file đã được parsed
    if not available_csv:
        print(f"Không tìm thấy file CSV nào trong thư mục {path}.")
        exit(1)
    
    dfs = []
    for file in available_csv:
        df = pd.read_csv(os.path.join(path, file))
        # lấy tên mã từ tên file (ticker_statistics_parsed.csv)
        symbol = os.path.splitext(file)[0].split("_")[0]
        df['symbol'] = symbol

        # bỏ dòng với breakdown = 'ttm', 'current'
        df = df[~df['breakdown'].isin(['ttm', 'current'])]

        # thay thế giá trị "--" bằng NaN
        df.replace("--", pd.NA, inplace=True)

        dfs.append(df)

    # gộp tất cả dataframe lại
    merged_df = pd.concat(dfs, ignore_index=True)

    # chuẩn hoá kiểu dữ liệu
    ## ngày tháng
    merged_df['date'] = pd.to_datetime(merged_df['breakdown'], format='%m/%d/%Y')
    merged_df.drop(columns=['breakdown'], inplace=True)

    ## các cột số
    merged_df["market_cap_value"] = merged_df["Market Cap"].str.extract(r'([\d\.]+)').astype(float)
    merged_df["market_cap_unit"] = merged_df["Market Cap"].str.extract(r'([A-Za-z]+)')
    merged_df["enterprise_value"] = merged_df["Enterprise Value"].str.extract(r'([\d\.]+)').astype(float)
    merged_df["enterprise_value_unit"] = merged_df["Enterprise Value"].str.extract(r'([A-Za-z]+)')
    ## thêm hệ số scale tương ứng với đơn vị đo
    scale_factors = {
        "USD": 1,  # không có đơn vị
        "K": 1e3,
        "M": 1e6,
        "B": 1e9,
        "T": 1e12
    }
    merged_df["market_cap_value_scale_factor"] = merged_df["market_cap_unit"].map(scale_factors).fillna(1)
    merged_df["enterprise_value_scale_factor"] = merged_df["enterprise_value_unit"].map(scale_factors).fillna(1)

    # bỏ môt số cột không cần thiết
    cols_to_drop = ['Market Cap', 'Enterprise Value']
    merged_df.drop(columns=cols_to_drop, inplace=True)

    # chuẩn hóa tên cột
    merged_df.rename(
        columns={
            "Trailing P/E": "trailing_pe",
            "Forward P/E": "forward_pe",
            "PEG Ratio (5yr expected)": "peg_ratio_5yr_expected",
            "Price/Sales": "ps_ratio",
            "Price/Book": "pb_ratio",
            "Enterprise Value/Revenue": "ev_revenue_ratio",
            "Enterprise Value/EBITDA": "ev_ebitda_ratio",
        },
        inplace=True
    )

    # sắp xếp lại thứ tự các cột cho dễ nhìn
    cols = ['symbol', 'date'] + [col for col in merged_df.columns if col not in ['symbol', 'date']]
    merged_df = merged_df[cols]

    # Lưu lại file đã xử lý
    cleaned_csv_path = os.path.join(path, f"statistics_cleaned.csv")
    merged_df.to_csv(cleaned_csv_path, index=False)
    print(f"Đã lưu file đã xử lý: {cleaned_csv_path}")


if __name__ == "__main__":
    # # # đường dẫn dùng để lưu rawl html và parsed csv
    # crawl_date = "2025_08_02"
    # path = os.path.join(ACTIVE_QUOTE_PATH, f"crawled_on_{crawl_date}")
    # clean_active_quote(path)
    # print("Cleaning completed.")

    # # Đường dẫn đến thư mục chứa dữ liệu 5 năm
    # crawl_date = "2025_08_02"
    # five_years_history_path = os.path.join("./data_test/crawl_5_years_history/", f"crawled_on_{crawl_date}")
    # clean_5_years_history(five_years_history_path)
    # print("5 years history cleaning completed.")

    # # Đường dẫn đến thư mục chứa dữ liệu báo cáo thu nhập
    # crawl_date = "2025_08_02"
    # income_statement_path = f"./data_test/crawl_financials/crawled_on_{crawl_date}/income_statement"
    # clean_income_statement(income_statement_path)
    # print("Income statement cleaning completed.")

    # # Đường dẫn đến thư mục chứa dữ liệu báo cáo bảng cân đối kế toán
    # crawl_date = "2025_08_02"
    # balance_sheet_path = f"./data_test/crawl_financials/crawled_on_{crawl_date}/balance_sheet"
    # clean_balance_sheet(balance_sheet_path)
    # print("Balance sheet cleaning completed.")

    # # Đường dẫn đến thư mục chứa dữ liệu báo cáo lưu chuyển tiền tệ
    # crawl_date = "2025_08_02"
    # cash_flow_path = f"./data_test/crawl_financials/crawled_on_{crawl_date}/cash_flow"
    # clean_cash_flow(cash_flow_path)
    # print("Cash flow cleaning completed.")

    # Đường dẫn đến thư mục chứa dữ liệu thống kê
    crawl_date = "2025_08_02"
    statistics_path = f"./data_test/crawl_statistics/crawled_on_{crawl_date}"
    clean_statistics(statistics_path)
    print("Statistics cleaning completed.")

    # # Đường dẫn đến thư mục chứa dữ liệu hồ sơ công ty
    # crawl_date = "2025_08_02"
    # profile_path = f"./data_test/crawl_profile/crawled_on_{crawl_date}"
    # clean_profile(profile_path)
    # print("Profile cleaning completed.")

