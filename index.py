import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from label_config import brand_label, pushsales_label, promotion_label, conversion_label
import os
from dotenv import load_dotenv
import json
load_dotenv()

def getKeys(obj, labels):
    keys = obj.keys()
    label_list = [labels[value] if value in labels else value for value in keys]
    return label_list

def convertData(array, labels):
    # Tạo danh sách mới
    new_list = []
    new_list.append(getKeys(array[0], labels))
# Duyệt qua mỗi đối tượng trong danh sách data
    for obj in array:
    # Tạo một danh sách con để chứa các giá trị của đối tượng
        values = []
        # Duyệt qua mỗi giá trị trong đối tượng
        for key, value in obj.items():
            if isinstance(value, list):
                value = '\n'.join(map(str, value))
            values.append(value)  # Thêm giá trị vào danh sách con
        # Thêm danh sách con vào danh sách mới
        new_list.append(values)

        # In danh sách mới
    return new_list
def sendRequest(url,  query = None):
    response = requests.get(url, params = query)
    # Kiểm tra xem yêu cầu có thành công không (status code 200)
    if response.status_code == 200:
        # Hiển thị nội dung của phản hồi
        return response.json()
    else:
        print('Yêu cầu không thành công:', response.status_code)
    return
def push_to_sheet(data, sheee_id, sheet_num = 1):
# Cấu hình Google Sheets API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)

    # Mở Google Sheet 
    spreadsheet  = client.open_by_key(sheee_id)
    sheets = spreadsheet.worksheets()
    sheet = sheets[sheet_num - 1]

    # Đồng bộ dữ liệu lên Google Sheet
    # data = [["Dữ liệu 1", "Dữ liệu 2", "Dữ liệu 3"], ["Dữ liệu 4", "Dữ liệu 5", "Dữ liệu 6"]]
    sheet.clear()
    sheet.insert_rows(data, 2)  # Thay đổi số hàng và dữ liệu tương ứng với dữ liệu của bạn
    print("Dữ liệu đã được đồng bộ lên Google Sheets!")

def getBrandOffer():
    result = sendRequest(f"{os.getenv('BASE_URL')}/offer/brand?pub_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}")
    convert = convertData(result['data'], brand_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), 1)
    return



def convertPushSaleData(data):
    extracted_data = []
    for item in data:
        info = item.get('info', {})
        extracted_item = {
            'pushsale_brand_id': item['pushsale_brand_id'],
            'pushsale_offer_id': item['pushsale_offer_id'],
            'pushsale_offer_name': item['pushsale_offer_name'],
            'avatar': info.get('avatar', ''),
            'content': info.get('content', ''),
            'commission': info.get('commission', ''),
            'status': item['status'],
            'start_date': item['start_date'],
            'end_date': item['end_date'],
            'link': item['link'],
            'domain': item['domain']
        }
        extracted_data.append(extracted_item)
    return extracted_data


def getPushSaleOffer():
    result = sendRequest(f"{os.getenv('BASE_URL')}/offer/pushsale?pub_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}")
    convert_pushsale = convertPushSaleData(result['data'])
    convert = convertData(convert_pushsale, pushsales_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), 2)
    return


def converPromotionData(data_list):
    converted_list = []
    for item in data_list:
        converted_item = {
            "coupon_code": item.get("coupon_code", ""),
            "offer_id": item.get("offer_id", ""),
            "started_date": item.get("started_date", ""),
            "expired_date": item.get("expired_date", ""),
            "title": item.get("title", ""),
            "content": item.get("content", ""),
            "type": item.get("type", ""),
            "discount_type": item.get("discount_type", ""),
            "status": item.get("status", ""),
            "discount": item.get("discount", ""),
            "external_links": item.get("external_links", []),
            "slug": item.get("slug", ""),
            "id": item.get("id", ""),
            "started_time": item.get("started_time", ""),
            "expired_time": item.get("expired_time", ""),
            "aff_link": item.get("aff_link", "")
        }
        converted_list.append(converted_item)
    return converted_list

def getPromotion():
    result = sendRequest(f"{os.getenv('BASE_URL')}/v1/promotions?publisher_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&limit=200")
    convert_promotion = converPromotionData(result['data'])
    convert = convertData(convert_promotion, promotion_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), 3)


def fillMissingKeys(array):
    # Tạo một bản sao của mảng để tránh ảnh hưởng đến dữ liệu gốc
    new_array = []

    # Tìm tất cả các key trong mảng và lưu trữ vị trí của chúng
    all_keys = set()
    key_index_map = {}
    for item in array:
        for index, key in enumerate(item.keys()):
            all_keys.add(key)
            key_index_map[key] = index

    print(all_keys)
    # Điền giá trị 'null' cho các phần tử thiếu key
    for item in array:
        new_item = item.copy()
        for key in all_keys:
            if key not in new_item:
                insert_index = key_index_map[key]
                new_item[key] = 'null'
        new_array.append(new_item)
    
    return new_array



def getConversion():
    result = sendRequest(f"{os.getenv('BASE_URL')}/v1/conversions?publisher_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&limit=100")
    fillMissing = fillMissingKeys(result['data'])
    print(fillMissing)
    # convert = convertData(fillMissing, conversion_label)
    # push_to_sheet(convert, os.getenv('SHEET_ID'), 4)

def main():
    # push_to_sheet([["Dữ liệu 1", "Dữ liệu 2", "Dữ liệu 3"], ["Dữ liệu 4", "Dữ liệu 5", "Dữ liệu 6"]])
    # getBrandOffer()
    # getPushSaleOffer()
    # getPromotion()
    getConversion()
    return

main()
