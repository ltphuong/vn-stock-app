from vnstock import Trading
import os
import pandas as pd
import time
from datetime import datetime
import glob

# 1. Khởi tạo
trading = Trading(source='VCI')
STORAGE_DIR = 'data_history'
list_tickers = [
# Nhóm Ngân hàng
'ABB', 'ACB', 'BID', 'EIB', 'HDB', 'LPB', 'MBB', 'MSB', 'NAB', 'OCB', 'SHB', 'STB', 'TCB', 'TPB', 'VAB', 'VCB', 'VIB', 'VPB',
# Nhóm Chứng khoán
'AGR', 'BSI', 'BVS', 'CTS', 'DSC', 'DSE', 'EVS', 'FTS', 'HCM', 'MBS', 'ORS', 'PSI', 'SHS', 'SSI', 'TCX', 'VCI', 'VDS', 'VIX', 'VND', 'VPX',
# Nhóm Bất động sản
'AGG', 'CEO', 'CKG', 'CSC', 'DIG', 'DPG', 'DXG', 'DXS', 'HDC', 'HDG', 'ITC', 'KDH', 'NBB', 'NHA', 'NLG', 'NTL', 'NVL', 'PDR', 'TCH',
# Nhóm đầu tư công
'CII', 'CTD', 'CTI', 'DTD', 'FCN', 'HHV', 'L14', 'PLC', 'VCG',
# Nhóm Thép
'BMP', 'HPG', 'HSG', 'NKG', 'SMC', 'TLH', 'VGS',
# Nhom dau khi va khoan san
'BSR', 'GAS', 'KSB', 'PAC', 'PLX', 'PVB', 'PVC', 'PVD', 'PVP', 'PVS', 'PVT',
# Nhóm Thuỷ Sản
'ANV', 'CMX', 'FMC', 'IDI', 'VHC',
# Nhóm bất động sản khu công nghiệp
'KBC', 'LHG', 'SZC', 'TIP', 'VGC', 'NTC', 'SIP',
# Nhóm bán lẻ
'DGW', 'FRT', 'HAX', 'MCH', 'MSN', 'PET', 'MWG', 'PNJ', 'VNM',
# Nhóm viễn thông
'CMG', 'CTR', 'ELC', 'FPT', 'SAM', 'VGI', 'VTP',
# Nhóm tài chính
'HHS', 'EVF',
# Nhóm Phân Bón
'AAA', 'BFC', 'CSV', 'DCM', 'DDV', 'DGC', 'DPM', 'LAS',
# Nhóm điện
'GEE', 'GEG', 'GEX', 'IDC', 'NT2', 'PC1', 'POW', 'REE', 'TV2',
# Nhom gao
'NAF', 'PAN',
# Nhóm dược
'DHG', 'TNH',
# Nhóm cảng biển
'GMD', 'HAH', 'SGP', 'SKG', 'VOS',
# Nhóm dệt may
'GIL', 'MSH',
# Nhóm cao su
'DPR', 'DRC', 'GVR', 'PHR','TRC'
# Nhóm Vin
'VHM', 'VRE', 'VPL', 'VIC',
# OTHER
'BAF', 'DBC', 'HAG', 'IPA', 'PTB', 'PVI', 'SAB', 'TIG', 'TTF'
]


def fetch_and_save_data(tickers):
    """Function 1: Lấy dữ liệu và lưu vào file csv theo ngày"""
    results = []
    print(f"--- Đang lấy dữ liệu cho {len(tickers)} mã ---")
        
    for ticker in list_tickers:
        try:
            # Lấy dữ liệu bảng giá chi tiết cho từng mã
            price_board = trading.price_board([ticker])
            
            # Lọc chỉ lấy dữ liệu khối ngoại (Foreign)
            # Khối lượng mua và bán
            foreign_buy_volume = price_board.filter(like='foreign_buy_volume').iloc[0].values[0]
            foreign_sell_volume = price_board.filter(like='foreign_sell_volume').iloc[0].values[0]
            KL_mua_ban = foreign_buy_volume - foreign_sell_volume
            
            # Giá trị mua và bán
            foreign_buy_value = price_board.filter(like='foreign_buy_value').iloc[0].values[0]
            foreign_sell_value = price_board.filter(like='foreign_sell_value').iloc[0].values[0]
            GT_mua_ban = foreign_buy_value - foreign_sell_value
            
            results.append({
                'Ticker': ticker,
                'KL_mua_ban': KL_mua_ban,
                'GT_mua_ban': GT_mua_ban
            })

            # Nghỉ ngắn để tránh bị chặn (Rate limit)
            time.sleep(1)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")

    # 3. Lưu vào file csv theo ngày
    if not os.path.exists(STORAGE_DIR):
        os.makedirs(STORAGE_DIR)

    current_date = datetime.now().strftime('%Y-%m-%d')
    file_name = f"foreign_data_{current_date}.csv"
    file_path = os.path.join(STORAGE_DIR, file_name)
    df_final = pd.DataFrame(results)

    # Lưu file CSV
    df_final.to_csv(file_path, index=False, encoding='utf-8-sig')

    print(f"--- THÀNH CÔNG ---")
    print(f"Đã lưu dữ liệu ngày {current_date} vào hệ thống.")
    print(f"Đường dẫn file: {file_path}")

def analyze_foreign_flow(tickers):
    """Function 2: Đọc dữ liệu từ file csv và phân tích từng mã"""
    # Lấy danh sách tất cả các file có định dạng foreign_data_*.csv trong thư mục storage_dir
    search_pattern = os.path.join(STORAGE_DIR, "foreign_data_*.csv")
    all_files = glob.glob(search_pattern)
    all_files.sort(reverse=True) # Lấy các ngày mới nhất
    recent_files = all_files[:10] # Chỉ lấy 10 ngày gần nhất

    if len(recent_files) < 5:
        print("Chưa đủ dữ liệu 10 ngày để phân tích.")
    else:
        li = [pd.read_csv(f) for f in recent_files]
        full_df = pd.concat(li, axis=0, ignore_index=True)

        # 2. Phân tích từng mã
        for ticker in list_tickers:
            ticker_data = full_df[full_df['Ticker'] == ticker]
            
            # Đếm số phiên mua ròng (Net_Vol > 0)
            positive_sessions = len(ticker_data[ticker_data['Net_Vol'] > 0])
            
            # Kiểm tra điều kiện: Mua ròng > 5 phiên trong 10 phiên
            if positive_sessions > 5:
                print(f"🔥 CẢNH BÁO: Mã {ticker} có {positive_sessions}/10 phiên mua ròng!")
                
            # Kiểm tra mua ròng liên tục (ví dụ 3 phiên gần nhất)
            last_5_sessions = ticker_data.head(5)['Net_Vol'].tolist()
            if all(x > 0 for x in last_5_sessions) and len(last_5_sessions) == 5:
                print(f"🚀 Mã {ticker} ĐANG MUA RÒNG LIÊN TIẾP 5 PHIÊN!")


def cleanup_old_files(keep_count=30):
    """Function 3: Dọn dẹp file cũ, chỉ giữ lại số lượng file quy định"""
    # 1. Lấy danh sách tất cả các file có định dạng foreign_data_*.csv trong thư mục storage_dir
    search_pattern = os.path.join(STORAGE_DIR, "foreign_data_*.csv")
    all_files = glob.glob(search_pattern)
    all_files.sort(reverse=True) # Lấy các ngày mới nhất

    # 3. Giữ lại 30 file đầu tiên, xóa các file từ vị trí 30 trở đi
    files_to_delete = all_files[30:]

    if files_to_delete:
        print(f"--- Đang dọn dẹp thư mục (Chỉ giữ 30 file gần nhất) ---")
        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                print(f"Đã xóa file cũ: {file_path}")
            except Exception as e:
                print(f"Lỗi khi xóa file {file_path}: {e}")
    else:
        print("Số lượng file hiện có chưa quá 30, không cần xóa.")


# ==========================================
# CHƯƠNG TRÌNH CHÍNH
# ==========================================
if __name__ == "__main__":
    fetch_and_save_data(list_tickers)
    analyze_foreign_flow(list_tickers)
    cleanup_old_files(30)

# # Lấy dữ liệu bảng giá chi tiết cho NT2
# price_board = trading.price_board(['FPT'])

# # Lọc chỉ lấy dữ liệu khối ngoại (Foreign)
# # Khối lượng mua và bán
# foreign_buy_volume = price_board.filter(like='foreign_buy_volume').iloc[0].values[0]
# foreign_sell_volume = price_board.filter(like='foreign_sell_volume').iloc[0].values[0]
# KL_mua_ban = foreign_buy_volume - foreign_sell_volume
# print(f"Khối lượng mua ngoại: {foreign_buy_volume:,}")
# print(f"Khối lượng bán ngoại: {foreign_sell_volume:,}")
# print(f"Khối lượng mua bán ròng: {KL_mua_ban:,}")

# # Giá trị mua và bán
# foreign_buy_value = price_board.filter(like='foreign_buy_value').iloc[0].values[0]
# foreign_sell_value = price_board.filter(like='foreign_sell_value').iloc[0].values[0]
# GT_mua_ban = foreign_buy_value - foreign_sell_value
# print("\n" + "="*30)
# print(f"Giá trị mua ngoại: {foreign_buy_value:,} VNĐ")
# print(f"Giá trị bán ngoại: {foreign_sell_value:,} VNĐ")
# print(f"Giá trị mua bán ròng: {GT_mua_ban:,} VNĐ")