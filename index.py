import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from label_config import brand_label, pushsales_label, promotion_label, conversion_label, transaction_label
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
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

def getBrandOffer(number_of_sheet=1, date_to_get = datetime.now()):
    result = sendRequest(f"{os.getenv('BASE_URL')}/offer/brand?pub_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&date={date_to_get}")
    convert = convertData(result['data'], brand_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)
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


def getPushSaleOffer(number_of_sheet=2, date_to_get = datetime.now()):
    result = sendRequest(f"{os.getenv('BASE_URL')}/offer/pushsale?pub_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&date={date_to_get}")
    convert_pushsale = convertPushSaleData(result['data'])
    convert = convertData(convert_pushsale, pushsales_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)
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

def getPromotion(limit=100, number_of_sheet=3):
    result = sendRequest(f"{os.getenv('BASE_URL')}/v1/promotions?publisher_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&limit={limit}")
    convert_promotion = converPromotionData(result['data'])
    convert = convertData(convert_promotion, promotion_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)


def get_complete_keys(data):
    # Tạo một set chứa tất cả các key
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())

    return all_keys

def fillMissingKeys(array, complete_keys):
    for item in array:
        # Tạo một từ điển mới với các key đã kiểm tra và giữ nguyên thứ tự ban đầu
        new_item = {key: item.get(key, None) for key in item.keys() | complete_keys}
        item.clear()
        item.update(new_item)
    
    return array


def getConversion(date_form,limit=100, number_of_sheet=4):
    result = sendRequest(f"{os.getenv('BASE_URL')}/v1/conversions?publisher_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&limit={limit}&{date_form}")
    fillMissing = fillMissingKeys(result['data'], get_complete_keys(result['data']))
    # print(fillMissing)
    convert = convertData(fillMissing, conversion_label)
    push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)

def getTransaction(start_date, end_date, limit=100, number_of_sheet=5):
    result = sendRequest(f"{os.getenv('BASE_URL')}/transaction?pub_id={os.getenv('PUB_ID')}&token={os.getenv('TOKEN')}&date_from={start_date}&date_to={end_date}&limit={limit}")
    if result is not None and 'data' in result:
        if result['data'] is not None and 'transactions' in result['data']:
            if result['data']['transactions'] is not None:
            # Thực hiện các thao tác với result['data'] ở đây
                fillMissing = fillMissingKeys(result['data']['transactions'], get_complete_keys(result['data']['transactions']))
                convert = convertData(fillMissing, transaction_label)
                push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)
                pass
            else:
                print("Không co dữ liệu!")
        else:
            print("Không co dữ liệu!")
    else:
        print("Không có dữ liệu!")
        
        #     fillMissing = fillMissingKeys(result['data']['transactions'], get_complete_keys(result['data']['transactions']))
        #     convert = convertData(fillMissing, transaction_label)
        #     push_to_sheet(convert, os.getenv('SHEET_ID'), number_of_sheet)
def render_12_months(year):
    result = []
    for month in range(1, 13):
        # Tạo ngày đầu tháng
        first_day_of_month = datetime(year, month, 1).strftime("%Y%m%d")
        
        # Tìm ngày cuối của tháng
        if month == 12:
            # Nếu là tháng 12, thì năm kế tiếp sẽ là năm mới
            next_year = year + 1
            last_day_of_month = datetime(next_year, 1, 1) - timedelta(days=1)
        else:
            last_day_of_month = datetime(year, month + 1, 1) - timedelta(days=1)
        last_day_of_month = last_day_of_month.strftime("%Y%m%d")
        
        result.append((first_day_of_month, last_day_of_month))
    
    return result

def main():
    # value = input("Enter 1 to get brand offer, 2 to get pushsale offer, 3 to get promotion, 4 to get conversion, 5 to get transaction: ")
    # print('Your entered value: ', value)
    # if(1 == int(value)):
    #     date_form = input("Enter date to get data(dd-MM-yyyy HH:mm:ss): ")
    #     number_of_sheet = input("Enter number of sheet: ")
    #     if int(number_of_sheet) < 0:
    #         return
    #     getBrandOffer(int(number_of_sheet), date_form)
    # if(2 == int(value)):
    #     date_form = input("Enter date to get data(dd-MM-yyyy HH:mm:ss): ")
    #     number_of_sheet = input("Enter number of sheet: ")
    #     if int(number_of_sheet) < 0:
    #         return
    #     getPushSaleOffer(int(number_of_sheet), date_form)
    # if(3 == int(value)):
    #     limit = input("Enter limit: ")
    #     number_of_sheet = input("Enter number of sheet: ")
    #     if int(number_of_sheet) < 0:
    #         return
    #     getPromotion(limit, int(number_of_sheet))
    # if(4 == int(value)):
    #     date_form = input("Enter date form(dd-MM-yyyy HH:mm:ss): ")
    #     limit = input("Enter limit: ")
    #     number_of_sheet = input("Enter number of sheet: ")
    #     if int(number_of_sheet) < 0:
    #         return
    #     getConversion(date_form, limit, int(number_of_sheet))
    # if(5 == int(value)):
    # limit = input("Nhập giới hạn số lượng dữ liệu muốn lấy (mặc định: 100): ")
    months_2024 = render_12_months(datetime.now().year)
    for index, (start_date, end_date) in enumerate(months_2024, start=1):
        getTransaction(start_date, end_date, 2000, int(index))
    return

main()
