import os
import requests
import time
import json
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = '1aJUkWXhvv75WzZq4CaULUgrqsguxYuEahLzORJKW5VY'

# Авторизация через секрет GitHub
service_account_info = json.loads(os.environ['SERVICE_ACCOUNT_JSON'])
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

def normalize_username(name):
    return re.sub(r'\s+', ' ', name.strip()).lower()

def extract_display_name(u: dict) -> str:
    if not isinstance(u, dict):
        return ""
    for k in ("displayname", "displayName", "name", "username", "nick"):
        v = u.get(k)
        if v:
            return str(v)
    for parent in ("user", "profile", "account", "player"):
        obj = u.get(parent)
        if isinstance(obj, dict):
            for k in ("displayname", "displayName", "name", "username", "nick"):
                v = obj.get(k)
                if v:
                    return str(v)
    return ""

def extract_score(u: dict) -> int:
    if not isinstance(u, dict):
        return 0
    for k in ("score", "value", "points", "doubloons"):
        if k in u:
            try:
                return int(u[k])
            except:
                try:
                    return int(float(u[k]))
                except:
                    pass
    return 0

def get_sheet_id(sheet_name):
    spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in spreadsheet['sheets']:
        if s['properties']['title'] == sheet_name:
            return s['properties']['sheetId']
    return None

def create_sheet(sheet_name):
    requests_body = {
        "requests": [{
            "addSheet": {
                "properties": {
                    "title": sheet_name,
                    "gridProperties": {"rowCount": 1000, "columnCount": 10}
                }
            }
        }]
    }
    try:
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=requests_body).execute()
        return True
    except Exception as e:
        if 'already exists' in str(e):
            return True
        print(f"Ошибка создания листа {sheet_name}: {e}")
        return False

def clear_sheet(sheet_name):
    try:
        sheet.values().clear(spreadsheetId=SPREADSHEET_ID, range=sheet_name).execute()
    except Exception as e:
        print(f"Ошибка очистки листа {sheet_name}: {e}")

def write_data_to_sheet(data, sheet_name, start_row=1):
    range_name = f"{sheet_name}!A{start_row}"
    body = {'values': data}
    try:
        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
    except Exception as e:
        print(f"Ошибка записи в лист {sheet_name}: {e}")

def prepare_data_for_sheet(users):
    rows = [['Rank', 'Nicknames', 'Doubloons', '$']]
    for u in users:
        rows.append([u['rank'], u['displayname'], u['score']])
    return rows

def apply_formatting(sheet_name):
    sheet_id = get_sheet_id(sheet_name)
    if sheet_id is None:
        print(f"Не найден лист {sheet_name} для форматирования")
        return

    requests_body = {
        "requests": [
            # Установка ширины столбцов
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 4},
                "properties": {"pixelSize": 150},
                "fields": "pixelSize"
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 70}, "fields": "pixelSize"
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                "properties": {"pixelSize": 300}, "fields": "pixelSize"
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3},
                "properties": {"pixelSize": 140}, "fields": "pixelSize"
            }},
            {"updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": 4},
                "properties": {"pixelSize": 120}, "fields": "pixelSize"
            }},
            # Заголовки жирные и с серым фоном
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.94, "green": 0.94, "blue": 0.94},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE"
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)"
            }},
            # Выравнивание всех ячеек по вертикали
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1},
                "cell": {"userEnteredFormat": {"verticalAlignment": "MIDDLE"}},
                "fields": "userEnteredFormat.verticalAlignment"
            }},
            # Выравнивание столбцов
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }},
            # Nicknames – слева, Doubloons и $ – справа
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 2},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "RIGHT"}},
                "fields": "userEnteredFormat.horizontalAlignment"
            }},
            # Формат чисел
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}},
                "fields": "userEnteredFormat.numberFormat"
            }},
            {"repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": 3, "endColumnIndex": 4},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
                "fields": "userEnteredFormat.numberFormat"
            }}
        ]
    }

    try:
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=requests_body).execute()
    except Exception as e:
        print(f"Ошибка форматирования листа {sheet_name}: {e}")

def fetch_leaderboard():
    base_url = "https://api.kap.gg/games/leaderboard/doubloons/"
    types = ["piracy", "governance"]
    limit = 50
    all_users_map = {}

    try:
        for lb_type in types:
            offset = 0
            while True:
                params = {"type": lb_type, "limit": limit, "offset": offset}
                response = requests.get(base_url, params=params)
                if response.status_code != 200:
                    print(f"Ошибка {lb_type}: {response.status_code}")
                    break
                data = response.json()
                users = data.get("results", [])
                if not users:
                    break
                for u in users:
                    name = extract_display_name(u)
                    score = extract_score(u)
                    if not name:
                        continue
                    norm_name = normalize_username(name)
                    if norm_name in all_users_map:
                        all_users_map[norm_name]['score'] += score
                        all_users_map[norm_name][lb_type] = score
                    else:
                        all_users_map[norm_name] = {
                            "displayname": name,
                            "score": score,
                            "piracy": score if lb_type=="piracy" else 0,
                            "governance": score if lb_type=="governance" else 0
                        }
                if len(users) < limit:
                    break
                offset += limit
                time.sleep(0.2)

        all_users = sorted(all_users_map.values(), key=lambda x: x['score'], reverse=True)
        prev_score = None
        prev_rank = 0
        for idx, user in enumerate(all_users, start=1):
            if user['score'] != prev_score:
                prev_rank = idx
                prev_score = user['score']
            user['rank'] = prev_rank

        total_score = sum(u['score'] for u in all_users)
        total_prize = 5000

        def create_and_fill_sheet(sheet_name, users_list):
            create_sheet(sheet_name)
            clear_sheet(sheet_name)
            rows = prepare_data_for_sheet(users_list)
            write_data_to_sheet(rows, sheet_name)
            # Добавляем формулу $ для всех пользователей
            formula_range = f"{sheet_name}!D2:D{len(users_list)+1}"
            formula = f'=ARRAYFORMULA(ЕСЛИ(ЕЧИСЛО(C2:C); ОКРУГЛ((C2:C / {total_score}) * {total_prize}; 2); ""))'
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=formula_range,
                valueInputOption='USER_ENTERED',
                body={'values': [[formula]]}
            ).execute()
            apply_formatting(sheet_name)

        # Leaderboard – общее
        create_and_fill_sheet("Leaderboard", all_users)
        # Piracy
        piracy_users = sorted(all_users, key=lambda x: x['piracy'], reverse=True)
        for u in piracy_users:
            u['score'] = u['piracy']
        create_and_fill_sheet("Piracy", piracy_users)
        # Governance
        governance_users = sorted(all_users, key=lambda x: x['governance'], reverse=True)
        for u in governance_users:
            u['score'] = u['governance']
        create_and_fill_sheet("Governance", governance_users)

        print("Leaderboard, Piracy и Governance обновлены и красиво отформатированы!")

    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    fetch_leaderboard()
