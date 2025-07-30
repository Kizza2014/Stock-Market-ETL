import csv

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