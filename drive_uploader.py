"""
Google Drive アップローダーモジュール

サービスアカウント認証情報（credentials.json）を使って
ローカルのログファイル（CSV / JSONL）を Google Drive の
指定フォルダにアップロードします。

使用例:
    from drive_uploader import upload_to_drive

    upload_to_drive("logs/board_20250414.csv", folder_id="1AbCdEfG...")
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from loguru import logger

# サービスアカウント認証情報ファイルのデフォルトパス
_DEFAULT_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(os.path.dirname(__file__), "credentials.json"),
)

# アップロード済みファイルを移動する先のサブフォルダ名（ローカル）
_UPLOADED_DIR = "uploaded"

# MIME タイプマッピング
_MIME_MAP = {
    ".csv":   "text/csv",
    ".jsonl": "application/x-ndjson",
    ".json":  "application/json",
    ".log":   "text/plain",
    ".txt":   "text/plain",
}


def _build_service(credentials_path: str):
    """Google Drive API サービスオブジェクトを返す"""
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"認証情報ファイルが見つかりません: {credentials_path}\n"
            "GCP コンソールでサービスアカウントを作成し、JSONキーを配置してください。"
        )
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _get_mime_type(file_path: str) -> str:
    """ファイル拡張子から MIME タイプを返す"""
    ext = Path(file_path).suffix.lower()
    return _MIME_MAP.get(ext, "application/octet-stream")


def _mark_uploaded(file_path: str) -> str:
    """
    アップロード済みファイルを処理します。

    - ファイルと同じディレクトリに `uploaded/` サブフォルダを作成し、
      そこにファイルを移動します。
    - 移動先に同名ファイルが既にある場合はタイムスタンプサフィックスを付与します。

    Returns:
        移動後のファイルパス
    """
    src = Path(file_path).resolve()
    dest_dir = src.parent / _UPLOADED_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / src.name
    if dest.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{src.stem}_{ts}{src.suffix}"

    shutil.move(str(src), str(dest))
    logger.debug(f"アップロード済みフォルダへ移動: {src.name} → {dest}")
    return str(dest)


def upload_to_drive(
    file_path: str,
    folder_id: str,
    credentials_path: str = _DEFAULT_CREDENTIALS,
    rename_after_upload: bool = True,
) -> dict:
    """
    指定したローカルファイルを Google Drive の特定フォルダにアップロードします。

    Args:
        file_path          : アップロードするローカルファイルのパス（CSV / JSONL など）
        folder_id          : アップロード先 Google Drive フォルダの ID
        credentials_path   : サービスアカウント JSON キーのパス
        rename_after_upload: True の場合、アップロード後にファイルを
                             `uploaded/` サブフォルダへ移動する

    Returns:
        アップロードされたファイルのメタデータ dict
            {"id": "...", "name": "...", "webViewLink": "..."}

    Raises:
        FileNotFoundError : ファイルまたは認証情報が見つからない
        HttpError         : Drive API エラー
    """
    file_path = str(Path(file_path).resolve())

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"アップロード対象ファイルが見つかりません: {file_path}")

    file_name = Path(file_path).name
    mime_type = _get_mime_type(file_path)

    logger.info(f"Drive アップロード開始: {file_name} → フォルダ {folder_id}")

    service = _build_service(credentials_path)

    file_metadata = {
        "name": file_name,
        "parents": [folder_id],
    }
    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)

    try:
        uploaded = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id,name,webViewLink")
            .execute()
        )
    except HttpError as e:
        logger.error(f"Drive アップロード失敗 ({file_name}): {e}")
        raise

    logger.success(
        f"アップロード完了: {uploaded['name']} "
        f"(id={uploaded['id']}, url={uploaded.get('webViewLink', 'N/A')})"
    )

    if rename_after_upload:
        _mark_uploaded(file_path)

    return uploaded


def upload_directory(
    dir_path: str,
    folder_id: str,
    pattern: str = "*.csv",
    credentials_path: str = _DEFAULT_CREDENTIALS,
) -> list[dict]:
    """
    ディレクトリ内の特定パターンに一致するファイルを一括アップロードします。

    Args:
        dir_path         : 対象ディレクトリのパス
        folder_id        : アップロード先 Google Drive フォルダ ID
        pattern          : glob パターン（デフォルト: "*.csv"）
        credentials_path : サービスアカウント JSON キーのパス

    Returns:
        アップロードされたファイルメタデータのリスト
    """
    target_dir = Path(dir_path)
    files = sorted(target_dir.glob(pattern))

    if not files:
        logger.info(f"アップロード対象ファイルなし: {target_dir / pattern}")
        return []

    results = []
    for f in files:
        try:
            result = upload_to_drive(str(f), folder_id, credentials_path)
            results.append(result)
        except Exception as e:
            logger.error(f"スキップ ({f.name}): {e}")

    logger.info(f"一括アップロード完了: {len(results)}/{len(files)} ファイル")
    return results
