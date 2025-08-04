import csv
import os


def check_valid_folder(path):
    # kiểm tra xem dữ liệu ngày đó đã được crawl chưa
    if not os.path.exists(path):
        print(f"Thư mục {path} không tồn tại")
        exit(1)
    if not os.listdir(path):
        print(f"Thư mục {path} không chứa file nào")
        exit(1)

def create_folder_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Đã tạo thư mục: {path}")
    
def save_html(html, save_path):
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Đã lưu toàn bộ HTML vào {save_path}")

def save_to_csv(rows_data, schema, save_path):
        if not rows_data or not schema:
            print("Không có dữ liệu để ghi ra file CSV.")
            return
        
        with open(save_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(schema)
            for row in rows_data:
                writer.writerow(row)
        print(f"Đã ghi dữ liệu ra file {save_path}")

def state_code_look_up(state_code):
    state_codes = {
        "AL": "Alabama",
        "AK": "Alaska",
        "AZ": "Arizona",
        "AR": "Arkansas",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "IA": "Iowa",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "ME": "Maine",
        "MD": "Maryland",
        "MA": "Massachusetts",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MS": "Mississippi",
        "MO": "Missouri",
        "MT": "Montana",
        "NE": "Nebraska",
        "NV": "Nevada",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NY": "New York",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VT": "Vermont",
        "VA": "Virginia",
        "WA": "Washington",
        "WV": "West Virginia",
        "WI": "Wisconsin",
        "WY": "Wyoming"
    }
     
    return state_codes.get(state_code.upper(), None)
