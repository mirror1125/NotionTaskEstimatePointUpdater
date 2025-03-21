import time
import requests
from dotenv import load_dotenv # type: ignore
import os
import logging
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor

# .envファイルを読み込む
load_dotenv()

# 環境変数を取得
token = os.getenv("TOKEN")
database_id = os.getenv("DB_ID")
log_file_path = os.getenv("LOG_FILE_PATH")

# ロガーの作成
logger = logging.getLogger("myLogger")
logger.setLevel(logging.INFO)

# 既存のハンドラをクリア（重要）
logger.handlers.clear()

# RotatingFileHandler の設定（例: 1MBごとにローテートし、バックアップを3つ保持）
handler = RotatingFileHandler(log_file_path, maxBytes=1*1024*1024, backupCount=3)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

ESTIMATE_POINT = "見積りポイント"

def main():
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    session = requests.Session()  # Keep-Alive有効化

    def get_database_pages():
        all_pages = []  # 全ページを蓄積するリスト
        url = f"https://api.notion.com/v1/databases/{database_id}/query"
        payload = {}  # 初回は空のペイロード
        cursor = None

        while True:
            # カーソルがある場合はペイロードに追加
            if cursor:
                payload["start_cursor"] = cursor

            # APIリクエスト
            response = session.post(url, headers=headers, json=payload)
            response.raise_for_status()  # エラー時に例外を発生

            # JSONデータを取得
            data = response.json()
            all_pages.extend(data["results"])  # 取得したページを追加

            # ページネーションの確認
            if not data.get("has_more", False):
                break  # さらにページがなければ終了
            cursor = data.get("next_cursor")  # 次のカーソルを更新

            # レート制限対策（必要に応じて）
            time.sleep(0.1)  # 1秒間に約3リクエストの制限を考慮

        return all_pages

    def update_page(page_id, total_points, page_dict):
        page = page_dict[page_id]
        current_value = page["properties"][ESTIMATE_POINT]["number"] or 0

        if current_value == total_points:
            return  # すでに最新の値なら更新不要

        response = session.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            json={"properties": {ESTIMATE_POINT: {"number": total_points}}},
            headers=headers
        )
        if response.status_code == 429:  # Too Many Requests
            time.sleep(1)
            session.patch(f"https://api.notion.com/v1/pages/{page_id}", json={"properties": {ESTIMATE_POINT: {"number": total_points}}}, headers=headers)

    def calculate_all_subitem_points(pages, page_dict):
        cache = {}

        def calculate_page_points(page_id):
            if page_id in cache:
                return cache[page_id]

            page = page_dict.get(page_id)
            if not page:
                return 0

            sub_items = page["properties"]["サブタスク"]["relation"]
            if sub_items:
                # 子タスクの合計のみを計算
                total = sum(calculate_page_points(sub_item["id"]) for sub_item in sub_items)
            else:
                # 子タスクがなければ、タスク自身の見積もりポイントを使用
                total = page["properties"].get(ESTIMATE_POINT, {}).get("number", 0) or 0

            cache[page_id] = total  # 計算結果をキャッシュに保存
            return total

        for page in pages:
            calculate_page_points(page["id"])

        return cache

    # Notionからページデータを取得
    pages = get_database_pages()
    logger.info(f"ページ数: {len(pages)}")

    # IDをキーにした辞書を作成
    page_dict = {page["id"]: page for page in pages}

    # タスクの見積りポイントを計算
    point_cache = calculate_all_subitem_points(pages, page_dict)

    # サブタスクが存在するタスク全てを更新対象とする
    update_pages = [p for p in pages if p["properties"]["サブタスク"]["relation"]]

    # 並列でAPIリクエストを実行
    with ThreadPoolExecutor(max_workers=12) as executor:
        for page in update_pages:
            executor.submit(update_page, page["id"], point_cache[page["id"]], page_dict)

    logger.info("正常終了しました")

if __name__ == "__main__":
    main()
