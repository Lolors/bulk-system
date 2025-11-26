import streamlit as st
import pandas as pd
import os
from datetime import datetime, date
import io
import math

# ==============================
# 사용자 계정 (로그인용)
# ==============================
USER_ACCOUNTS = {
    "ps": {"password": "0000", "display_name": "임필선"},
    "by": {"password": "0000", "display_name": "강봉연"},
    "hn": {"password": "0000", "display_name": "김한나"},
}

# ==============================
# 기본 설정 + CSS
# ==============================
st.set_page_config(page_title="벌크 관리 시스템", layout="wide")

st.markdown(
    """
    <style>
    /* 텍스트 입력 칸은 화면 폭과 상관없이 고정 크기 + 확장 금지 */
    .stTextInput > div {
        flex: 0 0 auto !important;
    }
    .stTextInput > div > div > input {
        width: 160px !important;
        max-width: 160px !important;
        min-width: 160px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

CSV_PATH = "bulk_drums_extended.csv"   # 품목코드~현재위치까지 들어있는 파일
PRODUCTION_FILE = "production.xlsx"    # 자사: 작업번호 → 로트/제조량
MOVE_LOG_CSV = "bulk_move_log.csv"     # 이동 이력
RECEIVE_FILE = "receive.xlsx"          # 사급: 입하번호 기반
STOCK_FILE = "stock.xlsx"              # 전산 재고

# ==============================
# S3 연동 설정
# ==============================
import boto3

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "bulk-system-enc")
S3_PREFIX = os.getenv("S3_PREFIX", "bulk-app/")  # 폴더 경로

def s3_enabled() -> bool:
    return bool(S3_BUCKET_NAME)

@st.cache_resource(show_spinner=False)
def get_s3_client():
    try:
        session = boto3.session.Session()
        client = session.client("s3")
        return client
    except Exception:
        return None

def _s3_key(filename: str) -> str:
    """
    S3에서 저장되는 경로를 결정.
    예: filename="bulk_drums_extended.csv" → "bulk-app/bulk_drums_extended.csv"
    """
    prefix = S3_PREFIX.rstrip("/")
    return f"{prefix}/{filename}" if prefix else filename


# ==============================
# 바코드 인식 (Dynamsoft DBR 전용 - CaptureVisionRouter 사용)
# ==============================
try:
    from PIL import Image, ImageOps, ImageEnhance
except ImportError:
    Image = None
    ImageOps = None
    ImageEnhance = None

# Dynamsoft Barcode Reader Python SDK (v10~)
try:
    from dynamsoft_barcode_reader_bundle import (
        LicenseManager,
        CaptureVisionRouter,
        EnumPresetTemplate,
        EnumErrorCode,
    )
except ImportError:
    LicenseManager = None
    CaptureVisionRouter = None
    EnumPresetTemplate = None
    EnumErrorCode = None


def load_dbr_license():
    """
    DBR 라이선스 키 로드:
    1) st.secrets["DBR_LICENSE"]
    2) 환경변수 DBR_LICENSE
    """
    lic = ""
    try:
        lic = st.secrets.get("DBR_LICENSE", "")
    except Exception:
        lic = ""
    if not lic:
        lic = os.getenv("DBR_LICENSE", "")
    if not lic:
        st.warning("DBR 라이선스 키를 찾을 수 없습니다. st.secrets 또는 환경변수 DBR_LICENSE에 등록해 주세요.")
    return lic


DBR_LICENSE = load_dbr_license()

_DBR_CVR = None
_DBR_LICENSE_INIT = False


def get_dbr_router():
    """LicenseManager + CaptureVisionRouter 초기화해서 전역으로 재사용."""
    global _DBR_CVR, _DBR_LICENSE_INIT

    if CaptureVisionRouter is None or LicenseManager is None or EnumErrorCode is None:
        return None

    if not _DBR_LICENSE_INIT:
        try:
            err_code, err_str = LicenseManager.init_license(DBR_LICENSE)
        except Exception:
            return None

        if err_code not in (
            EnumErrorCode.EC_OK,
            getattr(EnumErrorCode, "EC_LICENSE_CACHE_USED", EnumErrorCode.EC_OK),
            getattr(EnumErrorCode, "EC_LICENSE_WARNING", EnumErrorCode.EC_OK),
        ):
            return None

        _DBR_LICENSE_INIT = True

    if _DBR_CVR is None:
        try:
            _DBR_CVR = CaptureVisionRouter()
        except Exception:
            return None

    return _DBR_CVR


def preprocess_for_barcode(pil_img):
    """흐릿한 라벨용 전처리."""
    if Image is None:
        return pil_img

    if pil_img.mode != "L":
        img = pil_img.convert("L")
    else:
        img = pil_img.copy()

    img = ImageOps.autocontrast(img)
    img = ImageEnhance.Sharpness(img).enhance(2.0)

    min_side = min(img.size)
    if min_side < 800:
        scale = 800.0 / float(min_side)
        new_size = (int(img.width * scale), int(img.height * scale))
        img = img.resize(new_size, Image.LANCZOS)

    return img


def dbr_decode(pil_img):
    """
    Dynamsoft DBR(CaptureVisionRouter)로만 바코드 디코딩.
    성공하면 [(포맷, 텍스트), ...] 리스트를 반환.
    """
    cvr = get_dbr_router()
    if cvr is None or EnumPresetTemplate is None or EnumErrorCode is None:
        return []

    img = preprocess_for_barcode(pil_img)

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()

    try:
        result = cvr.capture(data, EnumPresetTemplate.PT_READ_BARCODES)
    except Exception:
        return []

    err = result.get_error_code()
    if err not in (
        EnumErrorCode.EC_OK,
        getattr(EnumErrorCode, "EC_UNSUPPORTED_JSON_KEY_WARNING", EnumErrorCode.EC_OK),
    ):
        return []

    barcode_result = result.get_decoded_barcodes_result()
    if barcode_result is None or barcode_result.get_items() == 0:
        return []

    items = barcode_result.get_items()
    codes = []
    for item in items:
        text = (item.get_text() or "").strip()
        fmt = (item.get_format_string() or "").strip()
        if text:
            codes.append((fmt, text))

    return codes


# ==============================
# 공통 유틸 (업로드/로컬 겸용)
# ==============================
@st.cache_data(show_spinner=False)
def _load_drums_core(bulk_bytes):
    """bulk_drums_extended.csv 로드 (세션 업로드 우선, 없으면 로컬 파일)."""
    # 1) 세션에 업로드된 파일이 있으면 그걸 우선 사용
    if bulk_bytes is not None:
        try:
            df = pd.read_csv(io.BytesIO(bulk_bytes))
        except Exception as e:
            st.error(f"업로드한 bulk_drums_extended.csv를 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame(
                columns=[
                    "품목코드",
                    "품명",
                    "로트번호",
                    "제품라인",
                    "제조일자",
                    "상태",
                    "통번호",
                    "통용량",
                    "현재위치",
                ]
            )
    # 2) 업로드 파일이 없고, 로컬 CSV가 있으면 그걸 사용
    elif os.path.exists(CSV_PATH):
        try:
            df = pd.read_csv(CSV_PATH)
        except Exception as e:
            st.error(f"CSV 파일을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame(
                columns=[
                    "품목코드",
                    "품명",
                    "로트번호",
                    "제품라인",
                    "제조일자",
                    "상태",
                    "통번호",
                    "통용량",
                    "현재위치",
                ]
            )
    # 3) 둘 다 없으면 빈 DF
    else:
        return pd.DataFrame(
            columns=[
                "품목코드",
                "품명",
                "로트번호",
                "제품라인",
                "제조일자",
                "상태",
                "통번호",
                "통용량",
                "현재위치",
            ]
        )

    required_cols = [
        "품목코드",
        "품명",
        "로트번호",
        "제품라인",
        "제조일자",
        "상태",
        "통번호",
        "통용량",
        "현재위치",
    ]
    for c in required_cols:
        if c not in df.columns:
            st.error(f"CSV에 '{c}' 열이 없습니다. 엑셀에서 다시 추출해 주세요.")
            return pd.DataFrame(columns=required_cols)

    df["통번호"] = pd.to_numeric(df["통번호"], errors="coerce").fillna(0).astype(int)
    df["통용량"] = pd.to_numeric(df["통용량"], errors="coerce").fillna(0.0).astype(float)

    def norm_loc(x: str) -> str:
        if pd.isna(x):
            return ""
        s = str(x).strip()
        if "-" not in s:
            if s in ["2층", "4층", "5층", "6층"]:
                return f"{s}-A1"
        return s

    df["현재위치"] = df["현재위치"].apply(norm_loc)

    return df


def load_drums() -> pd.DataFrame:
    """세션 상태를 감안해서 bulk DF를 가져오는 외부용 함수."""
    ss = st.session_state
    bulk_bytes = ss.get("bulk_csv_bytes", None)
    return _load_drums_core(bulk_bytes)


def save_drums(df: pd.DataFrame):
    """
    현재 DF를 bulk_drums_extended.csv로 저장.
    - 세션 메모리(업로드 방식) 갱신
    - 로컬 파일도 있으면 덮어쓰기 (로컬 실행용)
    """
    # 1) 세션 메모리 갱신
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    st.session_state["bulk_csv_bytes"] = buf.getvalue()

    # 캐시 무효화
    _load_drums_core.clear()

    # 2) 로컬 CSV로도 저장 (있으면)
    try:
        df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    except Exception:
        # Cloud 환경에서는 보통 권한/경로가 없으니 조용히 무시
        pass


@st.cache_data(show_spinner=False)
def _load_production_core(prod_bytes):
    if prod_bytes is not None:
        try:
            df = pd.read_excel(io.BytesIO(prod_bytes))
        except Exception:
            return pd.DataFrame()
    elif os.path.exists(PRODUCTION_FILE):
        try:
            df = pd.read_excel(PRODUCTION_FILE)
        except Exception:
            return pd.DataFrame()
    else:
        return pd.DataFrame()

    required = ["작업번호", "품번", "품명", "LOTNO", "지시수량", "제조량", "작업일자"]
    for c in required:
        if c not in df.columns:
            return pd.DataFrame()
    return df[required].copy()


def load_production():
    ss = st.session_state
    prod_bytes = ss.get("prod_xlsx_bytes", None)
    return _load_production_core(prod_bytes)


@st.cache_data(show_spinner=False)
def _load_receive_core(recv_bytes):
    if recv_bytes is not None:
        try:
            df = pd.read_excel(io.BytesIO(recv_bytes))
        except Exception as e:
            st.error(f"receive.xlsx 파일(업로드)을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame()
    elif os.path.exists(RECEIVE_FILE):
        try:
            df = pd.read_excel(RECEIVE_FILE)
        except Exception as e:
            st.error(f"receive.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()
    return df


def load_receive():
    ss = st.session_state
    recv_bytes = ss.get("recv_xlsx_bytes", None)
    return _load_receive_core(recv_bytes)


@st.cache_data(show_spinner=False)
def _load_stock_core(stock_bytes):
    if stock_bytes is not None:
        try:
            df = pd.read_excel(io.BytesIO(stock_bytes))
        except Exception as e:
            st.error(f"stock.xlsx 파일(업로드)을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame()
    elif os.path.exists(STOCK_FILE):
        try:
            df = pd.read_excel(STOCK_FILE)
        except Exception as e:
            st.error(f"stock.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()
    return df


def load_stock() -> pd.DataFrame:
    ss = st.session_state
    stock_bytes = ss.get("stock_xlsx_bytes", None)
    return _load_stock_core(stock_bytes)


# ==============================
# 자사 품번별 제품라인 자동 분류
# ==============================
NEEDLESHOT_CODES = {
    "3VTCLOS-010",
    "3VTCLOS-006",
    "3VTCLOS-007",
    "3VTCLOS-008",
    "3VTCLOS-011",
    "3VTCLOS-013",
    "3VTCLOS-047",
}

FACIAL_CODES = {
    "3VTCLOS-023",
    "3VTCLOS-024",
    "3VTCLOS-060",
    "3VTCLOS-061",
    "3VTCLOS-062",
    "3VTCLOS-063",
    "3VTCLOS-064",
    "3VTCLOS-065",
}


def classify_product_line(item_code: str) -> str:
    if not isinstance(item_code, str):
        return ""
    code = item_code.strip()
    if code in NEEDLESHOT_CODES:
        return "리들샷"
    if code in FACIAL_CODES:
        return "페이셜"
    return ""
def add_tat_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    df에 'TAT' 컬럼을 추가해서 제조일자로부터 오늘까지 경과 개월 수를 채워준다.
    - 제조일자가 비어있거나 파싱 불가하면 TAT는 <NA>
    """
    if "제조일자" not in df.columns:
        df["TAT"] = pd.NA
        return df

    # 제조일자를 datetime으로 변환 (여러 포맷 허용)
    mfg_dt = pd.to_datetime(df["제조일자"], errors="coerce")

    # 오늘 날짜
    today = date.today()

    # 연/월 차이로 개월 수 계산
    years_diff = today.year - mfg_dt.dt.year
    months_diff = today.month - mfg_dt.dt.month
    tat_months = years_diff * 12 + months_diff

    # 음수 방지
    tat_months = tat_months.clip(lower=0)

    # 날짜 없는 곳은 NA로
    tat_months = tat_months.where(~mfg_dt.isna(), pd.NA)

    # nullable 정수로 저장
    df = df.copy()
    df["TAT"] = tat_months.astype("Int64")

    return df

def generate_drums(prod_qty_kg: float):
    """제조량(kg)을 받아서 통번호/용량을 자동 생성."""
    if prod_qty_kg is None:
        return []

    try:
        qty = float(prod_qty_kg)
    except Exception:
        return []

    if qty <= 0:
        return []

    drums = []
    if qty < 200:
        drums.append({"통번호": 1, "통용량": qty})
        return drums

    full = int(qty // 1000)
    rem = qty % 1000

    for i in range(full):
        drums.append({"통번호": i + 1, "통용량": 1000})

    if rem > 0:
        drums.append({"통번호": full + 1, "통용량": rem})

    return drums


def ensure_lot_in_csv(
    df: pd.DataFrame,
    lot: str,
    item_code: str,
    item_name: str,
    line: str,
    mfg_date: str,
    initial_status: str = "생산대기",
    prod_qty: float = None,
) -> pd.DataFrame:
    """없던 로트면 통 자동 생성해서 CSV에 추가."""
    if (df["로트번호"] == lot).any():
        return df

    drums = generate_drums(prod_qty)
    if not drums:
        return df

    new_rows = []
    for d in drums:
        new_rows.append(
            {
                "품목코드": item_code,
                "품명": item_name,
                "로트번호": lot,
                "제품라인": line or "",
                "제조일자": mfg_date or "",
                "상태": initial_status or "생산대기",
                "통번호": int(d["통번호"]),
                "통용량": float(d["통용량"]),
                "현재위치": "미지정",
            }
        )

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df


# ==============================
# 이동 LOG 유틸 (ID 포함, 업로드/세션 겸용)
# ==============================
@st.cache_data(show_spinner=False)
def _load_move_log_core(move_bytes):
    """이동 이력 CSV 로드."""
    default_cols = [
        "시간",
        "ID",          # 이동 기록 작성자 (표시용 이름)
        "품번",
        "품명",
        "로트번호",
        "통번호",
        "변경 전 용량",
        "변경 후 용량",
        "변화량",
        "변경 전 위치",
        "변경 후 위치",
    ]

    if move_bytes is not None:
        try:
            df = pd.read_csv(io.BytesIO(move_bytes))
        except Exception as e:
            st.error(f"이동 이력 파일(업로드)을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame(columns=default_cols)
    elif os.path.exists(MOVE_LOG_CSV):
        try:
            df = pd.read_csv(MOVE_LOG_CSV)
        except Exception as e:
            st.error(f"이동 이력 파일을 읽는 중 오류가 발생했습니다: {e}")
            return pd.DataFrame(columns=default_cols)
    else:
        return pd.DataFrame(columns=default_cols)

    # 예전 로그에 ID열이 없을 수도 있으니 보정
    for c in default_cols:
        if c not in df.columns:
            if c == "ID":
                df[c] = ""
            else:
                df[c] = pd.NA

    return df[default_cols]


def load_move_log() -> pd.DataFrame:
    ss = st.session_state
    move_bytes = ss.get("move_log_csv_bytes", None)
    return _load_move_log_core(move_bytes)


def write_move_log(item_code: str, item_name: str, lot: str, drum_infos, from_zone: str, to_zone: str):
    """
    이동 이력을 bulk_move_log.csv에 기록.
    drum_infos: [(통번호, moved_qty, old_qty, new_qty), ...]
    ID 열에는 로그인한 사용자의 '표시 이름'을 남긴다.
    """
    if not drum_infos:
        return

    ss = st.session_state
    user_display_name = ss.get("user_name", "")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for drum_no, moved_qty, old_qty, new_qty in drum_infos:
        rows.append(
            {
                "시간": ts,
                "ID": user_display_name,
                "품번": item_code,
                "품명": item_name,
                "로트번호": lot,
                "통번호": drum_no,
                "변경 전 용량": old_qty,
                "변경 후 용량": new_qty,
                "변화량": moved_qty,
                "변경 전 위치": from_zone,
                "변경 후 위치": to_zone,
            }
        )

    new_df = pd.DataFrame(rows)

    # 기존 로그 불러오기 (세션/로컬)
    if "move_log_csv_bytes" in ss:
        try:
            old_df = pd.read_csv(io.BytesIO(ss["move_log_csv_bytes"]))
        except Exception:
            old_df = pd.DataFrame()
    elif os.path.exists(MOVE_LOG_CSV):
        try:
            old_df = pd.read_csv(MOVE_LOG_CSV)
        except Exception:
            old_df = pd.DataFrame()
    else:
        old_df = pd.DataFrame()

    log_df = pd.concat([old_df, new_df], ignore_index=True)

    # 1) 세션에 다시 저장
    buf = io.BytesIO()
    log_df.to_csv(buf, index=False, encoding="utf-8-sig")
    ss["move_log_csv_bytes"] = buf.getvalue()

    _load_move_log_core.clear()

    # 2) 로컬 CSV에도 저장 (로컬 실행용)
    try:
        log_df.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
    except Exception:
        pass

# ==============================
# 업로드 시간 표시 유틸  (S3 → 로컬 순으로 확인)
# ==============================
from datetime import datetime

def last_upload_caption(filename: str) -> str:
    """
    1) S3 객체가 있으면 그 객체의 LastModified 시간을 표시
    2) 없으면 로컬 파일 수정시간을 표시
    3) 둘 다 없으면 '업로드된 파일 없음'
    """
    # 1) S3 LastModified -----------------------------------------
    try:
        if s3_enabled():
            client = get_s3_client()
            if client:
                s3_path = _s3_key(filename)
                resp = client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_path)
                lm = resp["LastModified"]  # timezone aware datetime
                ts_str = lm.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                return f"S3 마지막 수정: {ts_str}"
    except Exception:
        pass

    # 2) 로컬 파일 mtime -----------------------------------------
    if os.path.exists(filename):
        try:
            ts = os.path.getmtime(filename)
            dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            return f"로컬 마지막 수정: {dt}"
        except Exception:
            return "로컬 파일 시간 읽기 오류"

    # 3) 둘 다 없음 ----------------------------------------------
    return "업로드된 파일이 없습니다."



# ==============================
# 데이터 파일 업로드 화면 (최초 1회용)
# ==============================
def render_file_loader():
    ss = st.session_state

    st.title("📁 데이터 파일 업로드")
    st.markdown(
        """
        Streamlit Cloud 또는 초기 설정 시, GitHub에 올리기 어려운 CSV/엑셀 파일들을
        여기에서 직접 업로드해서 사용합니다.

        아래 4개 파일은 **필수**이고, 이동 이력(`bulk_move_log.csv`)은 **있으면 업로드, 없으면 생략**해도 됩니다.
        """
    )

    col_left, col_right = st.columns(2)

    with col_left:
        bulk_file = st.file_uploader(
            "1) bulk_drums_extended.csv (필수)",
            type=["csv"],
            key="first_up_bulk",
        )
        st.caption(last_upload_caption(CSV_PATH))

        prod_file = st.file_uploader(
            "2) production.xlsx (필수)",
            type=["xlsx"],
            key="first_up_prod",
        )
        st.caption(last_upload_caption(PRODUCTION_FILE))

        recv_file = st.file_uploader(
            "3) receive.xlsx (필수)",
            type=["xlsx"],
            key="first_up_recv",
        )
        st.caption(last_upload_caption(RECEIVE_FILE))

        stock_file = st.file_uploader(
            "4) stock.xlsx (필수)",
            type=["xlsx"],
            key="first_up_stock",
        )
        st.caption(last_upload_caption(STOCK_FILE))

    with col_right:
        move_file = st.file_uploader(
            "5) bulk_move_log.csv (선택)",
            type=["csv"],
            key="first_up_move",
        )
        st.caption(last_upload_caption(MOVE_LOG_CSV))
        st.caption("※ 없으면 업로드 안 해도 됩니다. 새 로그로 시작해요.")

    if st.button("업로드 완료", key="first_upload_done"):
        missing = []
        if bulk_file is None:
            missing.append("bulk_drums_extended.csv")
        if prod_file is None:
            missing.append("production.xlsx")
        if recv_file is None:
            missing.append("receive.xlsx")
        if stock_file is None:
            missing.append("stock.xlsx")

        if missing:
            st.error("다음 필수 파일을 모두 업로드해 주세요: " + ", ".join(missing))
            return

        # ---------- 1) 업로드 파일을 바이트로 읽어서 세션에 저장 ----------
        bulk_bytes = bulk_file.read()
        prod_bytes = prod_file.read()
        recv_bytes = recv_file.read()
        stock_bytes = stock_file.read()
        move_bytes = move_file.read() if move_file is not None else None

        ss["bulk_csv_bytes"] = bulk_bytes
        ss["prod_xlsx_bytes"] = prod_bytes
        ss["recv_xlsx_bytes"] = recv_bytes
        ss["stock_xlsx_bytes"] = stock_bytes
        if move_bytes is not None:
            ss["move_log_csv_bytes"] = move_bytes

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.caption(last_upload_caption(CSV_PATH))
        st.caption(last_upload_caption(PRODUCTION_FILE))
        st.caption(last_upload_caption(RECEIVE_FILE))
        st.caption(last_upload_caption(STOCK_FILE))
        st.caption(last_upload_caption(MOVE_LOG_CSV))


        # ---------- 2) 서버 로컬 파일로도 저장 (이후 세션에서 재사용) ----------
        try:
            _load_drums_core.clear()
            df_bulk = _load_drums_core(bulk_bytes)
            df_bulk.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
        except Exception:
            pass

        try:
            _load_production_core.clear()
            df_prod = _load_production_core(prod_bytes)
            df_prod.to_excel(PRODUCTION_FILE, index=False)
        except Exception:
            pass

        try:
            _load_receive_core.clear()
            df_recv = _load_receive_core(recv_bytes)
            df_recv.to_excel(RECEIVE_FILE, index=False)
        except Exception:
            pass

        try:
            _load_stock_core.clear()
            df_stock = _load_stock_core(stock_bytes)
            df_stock.to_excel(STOCK_FILE, index=False)
        except Exception:
            pass

        if move_bytes is not None:
            try:
                _load_move_log_core.clear()
                df_move = _load_move_log_core(move_bytes)
                df_move.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
            except Exception:
                pass

        # ---------- 3) 플래그 세팅 후 메인으로 ----------
        ss["data_initialized"] = True

        st.success("파일 업로드가 완료되었습니다. 메인 화면으로 이동합니다.")
        st.rerun()


# ==============================
# 로그인 화면
# ==============================
def render_login():
    ss = st.session_state
    st.title("🏭 벌크 관리 시스템 - 로그인")

    st.markdown("작업 전 ID와 비밀번호를 입력해 주세요.")

    login_id = st.text_input("ID", key="login_id")
    login_pw = st.text_input("비밀번호", type="password", key="login_pw")

    if st.button("로그인", key="login_btn"):
        user = USER_ACCOUNTS.get((login_id or "").strip())
        if user and login_pw == user["password"]:
            ss["user_id"] = (login_id or "").strip()
            ss["user_name"] = user["display_name"]
            st.success(f"{user['display_name']}님, 환영합니다.")
            st.rerun()
        else:
            st.error("ID 또는 비밀번호가 올바르지 않습니다.")


# ==============================
# (생략됐던) get_stock_summary 더미 정의
# ==============================
def get_stock_summary(item_code: str, lot: str):
    """
    원래 코드에 있던 get_stock_summary가 질문 코드에는 없어서
    최소한의 더미로 넣어 둡니다.
    실제 전산 재고 연동 로직이 있다면 이 부분을 교체해 주세요.
    """
    return None, ""


# ==============================
# 탭 1: 이동
# ==============================
def clear_move_inputs():
    """조회/초기화 버튼 옆에서 사용할 입력값 초기화 콜백."""
    ss = st.session_state
    ss["mv_barcode"] = ""
    ss["mv_lot"] = ""
    ss["mv_scanned_barcode"] = ""


def render_tab_move():
    st.markdown("### 📦 벌크 이동")

    ss = st.session_state
    ss.setdefault("mv_scanned_barcode", "")
    ss.setdefault("mv_searched_csv", False)
    ss.setdefault("mv_search_by_lot", False)
    ss.setdefault("mv_last_lot", "")
    ss.setdefault("mv_last_barcode", "")
    ss.setdefault("mv_show_stock_detail", False)
    ss.setdefault("mv_show_move_history_here", False)

    bulk_type = st.radio(
        "벌크 구분을 선택해 주세요.",
        ["자사", "사급"],
        horizontal=True,
        key="mv_bulk_type_csv",
    )
    barcode_label = "작업번호를 입력해 주세요." if bulk_type == "자사" else "입하번호를 입력해 주세요."

    # ================== 상단 입력 ==================

    # 1줄: 작업번호/입하번호 + 로트번호 (기존 그대로)
    col_in1, col_in2, _sp = st.columns([0.49, 0.49, 2.5])

    with col_in1:
        barcode = st.text_input(
            barcode_label,
            key="mv_barcode",
            placeholder="예: W24012345",
        )

    with col_in2:
        lot_input = st.text_input(
            "로트번호",
            key="mv_lot",
            placeholder="예: 2E075K",
        )

    # ================== 2줄: 바코드 스캔 업로드 (절반 너비) ==================
    st.write("")

    scan_col, empty_col = st.columns([1.2, 3])   # ← 여기서 너비가 결정된다!

    with scan_col:
        st.caption("라벨 사진 업로드 (선택)")
        scan_file = st.file_uploader(
            "바코드 인식",
            type=["png", "jpg", "jpeg"],
            key="mv_barcode_image",
        )

        if scan_file is not None:
            if Image is None or CaptureVisionRouter is None or LicenseManager is None:
                st.error("바코드 인식 라이브러리가 없습니다.")
            else:
                try:
                    img = Image.open(io.BytesIO(scan_file.read()))
                    st.image(img, caption=scan_file.name, width=220)
                    codes = dbr_decode(img)
                    if codes:
                        _, text_code = codes[0]
                        ss["mv_scanned_barcode"] = text_code.strip()
                        st.success(f"인식됨: {text_code}")
                    else:
                        st.warning("바코드를 인식하지 못했습니다.")
                except Exception as e:
                    st.error(f"이미지를 처리하는 중 오류: {e}")

    # ================== 3줄: 조회 / 초기화 버튼 ==================
    st.write("")
    btn_col1, btn_col2, _ = st.columns([0.5, 0.5, 3])

    search_clicked = False
    with btn_col1:
        if st.button("조회하기", key="mv_search_btn_csv"):
            search_clicked = True

    with btn_col2:
        st.button("초기화", key="mv_clear_btn", on_click=clear_move_inputs)


    # 조회 버튼 처리
    if "search_clicked" in locals() and search_clicked:
        barcode_val = (barcode or "").strip()
        lot_val = (lot_input or "").strip()
        scanned_val = ss.get("mv_scanned_barcode", "").strip()

        if not lot_val and not barcode_val and not scanned_val:
            st.warning("먼저 작업번호/입하번호 또는 로트번호를 입력(또는 바코드를 스캔)해 주세요.")
            ss["mv_searched_csv"] = False
            return

        search_by_lot = bool(lot_val)

        if not search_by_lot:
            if not barcode_val and scanned_val:
                barcode_val = scanned_val

        ss["mv_last_lot"] = lot_val
        ss["mv_last_barcode"] = barcode_val
        ss["mv_search_by_lot"] = search_by_lot
        ss["mv_searched_csv"] = True
        ss["mv_scanned_barcode"] = ""
        ss["mv_show_move_history_here"] = False

    if not ss["mv_searched_csv"]:
        return

    # ===================== 검색 후 로직 =====================
    df = load_drums()
    prod_df = load_production()
    recv_df = load_receive()

    search_by_lot = ss.get("mv_search_by_lot", False)
    lot = ""
    item_code = ""
    item_name = ""
    prod_date = ""
    prod_qty = None
    line = ""
    barcode_used = ""

    if search_by_lot:
        lot = (ss.get("mv_last_lot") or "").strip()
        if not lot:
            st.warning("로트번호가 비어 있습니다.")
            ss["mv_searched_csv"] = False
            return
        barcode_used = lot
    else:
        barcode_query = (ss.get("mv_last_barcode") or "").strip()
        if not barcode_query:
            st.warning("작업번호/입하번호가 비어 있습니다.")
            ss["mv_searched_csv"] = False
            return

        if bulk_type == "자사":
            if prod_df.empty:
                st.error("production.xlsx 파일을 읽을 수 없어서 작업번호 기반 조회를 할 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            hit = prod_df[prod_df["작업번호"].astype(str) == barcode_query]
            if hit.empty:
                st.warning("해당 작업번호를 찾을 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            r = hit.iloc[0]
            lot = str(r["LOTNO"])
            item_code = str(r["품번"])
            item_name = str(r["품명"])
            prod_qty = float(r["제조량"]) if not pd.isna(r["제조량"]) else None
            prod_date = str(r["작업일자"])
            line = classify_product_line(item_code)

            df = ensure_lot_in_csv(
                df,
                lot=lot,
                item_code=item_code,
                item_name=item_name,
                line=line,
                mfg_date=prod_date,
                initial_status="생산대기",
                prod_qty=prod_qty,
            )
            save_drums(df)

        else:  # 사급
            if recv_df.empty:
                st.error("receive.xlsx 파일을 찾을 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            if "입하번호" not in recv_df.columns:
                st.error("receive.xlsx에 '입하번호' 열이 없습니다.")
                ss["mv_searched_csv"] = False
                return

            hit = recv_df[recv_df["입하번호"].astype(str) == barcode_query]
            if hit.empty:
                st.warning("해당 입하번호를 receive.xlsx에서 찾을 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            r = hit.iloc[0]
            if "품번" not in recv_df.columns or "품명" not in recv_df.columns or "로트번호" not in recv_df.columns:
                st.error("receive.xlsx에 품번/품명/로트번호 관련 열이 없습니다.")
                ss["mv_searched_csv"] = False
                return

            item_code = str(r["품번"])
            item_name = str(r["품명"])
            lot = str(r["로트번호"])

            if "입하량" in recv_df.columns:
                prod_qty = float(r["입하량"]) if not pd.isna(r["입하량"]) else None
            else:
                prod_qty = None

            if "제조일자" in recv_df.columns:
                prod_date = "" if pd.isna(r["제조일자"]) else str(r["제조일자"])
            elif "제조년월일" in recv_df.columns:
                prod_date = "" if pd.isna(r["제조년월일"]) else str(r["제조년월일"])
            else:
                prod_date = ""

            trade_type = str(r.get("유/무상", "")).strip()
            if trade_type == "유상":
                line = "사급(유상)"
            elif trade_type == "무상":
                line = "사급(무상)"
            else:
                line = "사급"

            df = ensure_lot_in_csv(
                df,
                lot=lot,
                item_code=item_code,
                item_name=item_name,
                line=line,
                mfg_date=prod_date,
                initial_status="생산대기",
                prod_qty=prod_qty,
            )
            save_drums(df)

        barcode_used = barcode_query

    df = load_drums()
    lot_df = df[df["로트번호"].astype(str) == lot].copy()
    if lot_df.empty:
        st.warning("CSV에서 해당 로트번호의 통 정보를 찾을 수 없습니다.")
        ss["mv_searched_csv"] = False
        return

    combos = lot_df[["품목코드", "품명"]].drop_duplicates().reset_index(drop=True)
    if len(combos) == 1:
        item_code = str(combos.at[0, "품목코드"])
        item_name = str(combos.at[0, "품명"])
    elif len(combos) > 1 and ss.get("mv_search_by_lot", False):
        st.info("해당 로트번호에 여러 품명이 연결되어 있습니다. 하나를 선택해 주세요.")
        options = [
            f"{row['품목코드']} / {row['품명']}"
            for _, row in combos.iterrows()
        ]
        selected_label = st.selectbox(
            "품명을 선택해 주세요.",
            options,
            key=f"mv_lot_item_select_{lot}",
        )
        sel_idx = options.index(selected_label)
        item_code = str(combos.at[sel_idx, "품목코드"])
        item_name = str(combos.at[sel_idx, "품명"])
        lot_df = lot_df[
            (lot_df["품목코드"].astype(str) == item_code)
            & (lot_df["품명"].astype(str) == item_name)
        ].copy()
    else:
        item_code = str(combos.at[0, "품목코드"])
        item_name = str(combos.at[0, "품명"])

    if not prod_date:
        dates = (
            lot_df["제조일자"]
            .dropna()
            .astype(str)
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        if dates:
            prod_date = dates[0]

    if not line:
        lines = (
            lot_df["제품라인"]
            .dropna()
            .astype(str)
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        if lines:
            line = lines[0]

    lot_df = lot_df.sort_values("통번호")

    loc_unique = lot_df["현재위치"].dropna().unique().tolist()
    if len(loc_unique) == 1:
        current_zone = loc_unique[0]
    elif len(loc_unique) == 0:
        current_zone = "미지정"
    else:
        current_zone = "혼합"

    stock_summary_df, stock_summary_text = get_stock_summary(item_code, lot)
    if stock_summary_df is not None and not stock_summary_df.empty:
        top = stock_summary_df.iloc[0]
        stock_loc_display = f"{top['대분류']}({top['창고명']})"
    else:
        stock_loc_display = current_zone

    col_left2, col_right2 = st.columns(2)

    # ===== 왼쪽: 조회 정보 + 통 선택 =====
    with col_left2:
        st.markdown("### 🧾 조회 정보")
        st.success("조회가 완료되었습니다.")

        st.markdown(
            f"""
            **벌크 구분:** {bulk_type}  
            **식별값:** {barcode_used}  
            **품목코드:** {item_code}  
            **품명:** {item_name}  
            **로트번호:** {lot}  
            **제조일자:** {prod_date}  
            """
        )

        # 현재 위치 + [상세보기] + [이동이력] 버튼
        loc_col1, loc_col2 = st.columns([3, 2])
        with loc_col1:
            st.markdown(f"**현재 위치(전산 기준):** {stock_loc_display}")
        with loc_col2:
            b1_col, b_sp, b2_col = st.columns([1, 0.05, 1])
            # ✅ 항상 보이는 상세보기 버튼
            with b1_col:
                if st.button("상세보기", key=f"stock_detail_btn_{lot}"):
                    ss["mv_show_stock_detail"] = not ss.get("mv_show_stock_detail", False)
            with b2_col:
                if st.button("이동이력", key=f"move_hist_btn_{lot}"):
                    ss["mv_show_move_history_here"] = not ss.get("mv_show_move_history_here", False)

        # ✅ 전산 재고 상세 토글
        if ss.get("mv_show_stock_detail", False):
            if stock_summary_df is not None and not stock_summary_df.empty:
                st.markdown("#### 🔎 전산 재고 상세")
                st.dataframe(stock_summary_df, use_container_width=True, height=240)
            else:
                st.info("전산 재고 데이터가 없습니다.")

        st.markdown("### 🛢 통 선택 및 잔량 입력")


        selected_drums = []
        drum_new_qty = {}

        drum_list = lot_df["통번호"].tolist()
        # 모두 선택 / 모두 해제  - 버튼 폭을 조금만 사용하는 좁은 컬럼
        c1, c_sp, c2, _c_gap = st.columns([1.5, 0.5, 1.5, 7])
        with c1:
            if st.button("모두 선택", key=f"mv_select_all_{lot}", use_container_width=False):
                for dn in drum_list:
                    st.session_state[f"mv_sel_{lot}_{dn}"] = True
        with c2:
            if st.button("모두 해제", key=f"mv_select_none_{lot}", use_container_width=False):
                for dn in drum_list:
                    st.session_state[f"mv_sel_{lot}_{dn}"] = False


        for _, row in lot_df.iterrows():
            drum_no = int(row["통번호"])
            old_qty = float(row["통용량"])
            drum_loc = str(row.get("현재위치", "") or "").strip()

            if drum_loc:
                label = f"{drum_no}번 통 — 기존 {old_qty:.0f}kg (위치: {drum_loc})"
            else:
                label = f"{drum_no}번 통 — 기존 {old_qty:.0f}kg"

            cb_key = f"mv_sel_{lot}_{drum_no}"
            checked = st.checkbox(label, key=cb_key)
            if checked:
                selected_drums.append(drum_no)
                new_val = st.number_input(
                    f"통 {drum_no}의 현재 용량(kg)",
                    min_value=0.0,
                    max_value=old_qty,
                    value=old_qty,
                    step=10.0,
                    format="%.0f",
                    key=f"mv_qty_{lot}_{drum_no}",
                )
                drum_new_qty[drum_no] = float(new_val)

    # ===== 오른쪽: 이동 위치 + 상태 + 비고 + 저장 =====
    with col_right2:
        st.markdown("### 🚚 이동 위치 선택")

        col1, col2 = st.columns(2)
        with col1:
            from_zone = st.text_input(
                "현재 위치",
                value=current_zone if current_zone != "혼합" else "",
                help="예: 4층-A1, 외주 등",
                key="mv_from_zone_csv",
            )
        with col2:
            floor_list = ["2층", "4층", "5층", "6층", "창고", "소진", "미지정", "폐기", "외주"]
            sel_floor = st.selectbox(
                "이동하실 층/구역을 선택해 주세요.", floor_list, key="mv_floor_csv"
            )
            if sel_floor in ["창고", "소진", "미지정", "폐기", "외주"]:
                sel_zone = ""
            else:
                zone_list = ["A1", "A2", "A3", "B1", "B2", "B3", "C1", "C2", "C3"]
                sel_zone = st.selectbox(
                    "이동하실 구역을 선택해 주세요.", zone_list, key="mv_zone_csv"
                )

            if sel_floor in ["창고", "소진", "미지정", "폐기", "외주"]:
                to_zone = sel_floor
            else:
                to_zone = f"{sel_floor}-{sel_zone}"

        if to_zone == "외주":
            move_status = "외주"
            st.info("이동 위치가 '외주'이므로 상태는 자동으로 '외주'로 설정됩니다.")
        else:
            move_status = st.radio(
                "이동 후 상태를 선택해 주세요.",
                ["잔량", "생산종료"],
                horizontal=True,
                key="mv_status_csv",
            )

        note = st.text_area("비고(선택 입력)", height=80, key="mv_note_csv")

        if st.button("이동 내용 저장 (CSV 반영)", key="mv_save_csv"):
            if not selected_drums:
                st.warning("이동하실 통을 한 개 이상 선택해 주세요.")
                return

            df_all = load_drums()
            lot_mask = df_all["로트번호"].astype(str) == lot

            drum_logs = []

            for dn in selected_drums:
                idx = df_all.index[lot_mask & (df_all["통번호"] == dn)]
                if len(idx) == 0:
                    continue
                i = idx[0]
                old_qty = float(df_all.at[i, "통용량"])
                new_qty = drum_new_qty.get(dn, old_qty)
                moved = old_qty - new_qty

                df_all.at[i, "통용량"] = new_qty
                df_all.at[i, "현재위치"] = to_zone

                if to_zone == "외주":
                    df_all.at[i, "상태"] = "외주"
                else:
                    df_all.at[i, "상태"] = move_status

                drum_logs.append((dn, moved, old_qty, new_qty))

            save_drums(df_all)

            # CSV + 이동 이력 로그 저장
            write_move_log(
                item_code=item_code,
                item_name=item_name,
                lot=lot,
                drum_infos=drum_logs,
                from_zone=from_zone,
                to_zone=to_zone,
            )

            st.success(f"총 {len(drum_logs)}개의 통 정보가 CSV 및 이동 이력에 반영되었습니다.")

    # 이동 탭 내부에서 현재 LOT 이동 이력 표시
    if ss.get("mv_show_move_history_here", False):
        log_df = load_move_log()
        if log_df.empty:
            st.info("이동 이력이 없습니다.")
        else:
            sub = log_df[log_df["로트번호"].astype(str) == str(lot)].copy()
            if sub.empty:
                st.info("해당 로트번호의 이동 이력이 없습니다.")
            else:
                st.markdown("### 📜 해당 로트번호 이동 이력")
                sub = sub.sort_values("시간", ascending=False).head(50)
                st.dataframe(sub, use_container_width=True)



# ==============================
# 탭 2: 조회
# ==============================
def render_tab_lookup():
    st.markdown("### 🔍 벌크 조회")

    df = load_drums()
    if df.empty:
        st.info("CSV에 등록된 벌크 정보가 없습니다.")
        return
        
    # ✅ 제조일자 기준 TAT(개월) 컬럼 추가
    df = add_tat_column(df)
    
    query = st.text_input("로트번호, 품목코드 또는 현재위치를 입력해 주세요.")
    if query:
        q = query.strip()
        mask = (
            df["로트번호"].astype(str).str.contains(q, na=False)
            | df["품목코드"].astype(str).str.contains(q, na=False)
            | df["현재위치"].astype(str).str.contains(q, na=False)
        )
        df_view = df[mask]
    else:
        df_view = df

    if df_view.empty:
        st.warning("검색 결과가 없습니다.")
        return

    st.markdown("#### 📄 행별 상세")
    st.dataframe(df_view, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 📊 현재위치별 용량 요약")

    summary = (
        df_view.groupby("현재위치", dropna=False)
        .agg(
            통개수=("통번호", "count"),
            총용량_kg=("통용량", "sum"),
        )
        .reset_index()
        .sort_values("현재위치")
    )

    # 행 개수에 맞춰 높이 자동 조정
    row_height = 35
    header_height = 40
    dynamic_height = header_height + row_height * (len(summary) + 1)

    st.dataframe(summary, width=300, height=dynamic_height)

    st.markdown("---")
    if st.button("현재 CSV를 그대로 백업 저장하기"):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"bulk_drums_extended_backup_{ts}.csv"
        df.to_csv(backup_name, index=False, encoding="utf-8-sig")
        st.success(f"백업 파일로 저장되었습니다: {backup_name}")


# ==============================
# 탭 3: 지도
# ==============================
def render_tab_map():
    st.markdown("### 🗺 벌크 위치 지도 (CSV 기준)")

    df = load_drums()
    if df.empty:
        st.info("CSV에 등록된 벌크 정보가 없습니다.")
        return

    def get_floor(loc: str) -> str:
        if pd.isna(loc):
            return ""
        s = str(loc).strip()
        if "-" in s:
            return s.split("-")[0]
        return s

    df["층"] = df["현재위치"].apply(get_floor)

    floors = (
        df["층"]
        .dropna()
        .astype(str)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    floors = sorted(floors)

    if not floors:
        st.info("층 정보가 없습니다.")
        return

    sel_floor = st.selectbox("확인하실 층/구역을 선택해 주세요.", floors, key="map_floor_csv")

    fdf = df[df["층"] == sel_floor].copy()
    if fdf.empty:
        st.info("해당 층/구역에 등록된 벌크가 없습니다.")
        return

    # 소진 / 미지정 / 폐기 / 외주 / 창고 는 단일 구역으로 처리
    special_floors = {"소진", "미지정", "폐기", "외주", "창고"}
    if sel_floor in special_floors:
        st.markdown(f"#### {sel_floor} 구역 현황")

        drums = len(fdf)
        vol = fdf["통용량"].sum()

        st.write(f"**통 개수:** {drums}통")
        st.write(f"**총 용량:** {int(vol)}kg")

        st.markdown("---")
        st.markdown("### 🔍 상세 목록")

        show_cols = [
            "품목코드",
            "품명",
            "로트번호",
            "제품라인",
            "제조일자",
            "상태",
            "현재위치",
            "통번호",
            "통용량",
        ]
        st.dataframe(
            fdf[show_cols].sort_values(["로트번호", "통번호"]),
            use_container_width=True,
        )
        return

    def get_zone_label(loc: str) -> str:
        if pd.isna(loc):
            return ""
        s = str(loc).strip()
        if "-" in s:
            return s.split("-")[1]
        if s in ["2층", "4층", "5층", "6층"]:
            return "A1"
        return s

    fdf["zone_label"] = fdf["현재위치"].apply(get_zone_label)

    labels_all = [f"{r}{c}" for r in ["A", "B", "C"] for c in [1, 2, 3]]

    zone_stats = {}
    max_vol = 0.0
    for label in labels_all:
        sub = fdf[fdf["zone_label"] == label]
        drums = len(sub)
        vol = sub["통용량"].sum()
        zone_stats[label] = {"drums": drums, "volume": vol}
        max_vol = max(max_vol, vol)

    def badge(volume):
        if volume <= 0:
            return "⚪"
        if max_vol <= 0:
            return "🟡"
        ratio = volume / max_vol
        if ratio > 0.7:
            return "🔴"
        elif ratio > 0.3:
            return "🟠"
        else:
            return "🟡"

    st.markdown(f"#### {sel_floor} Zone별 현황 (통 개수 / 총 용량)")

    for row in ["A", "B", "C"]:
        cols = st.columns(3)
        for i, col in enumerate(cols):
            label = f"{row}{i+1}"
            info = zone_stats.get(label, {"drums": 0, "volume": 0})
            txt = (
                f"{label} {badge(info['volume'])}\n"
                f"{info['drums']}통 / {int(info['volume'])}kg"
            )
            if col.button(txt, key=f"map_btn_{sel_floor}_{label}"):
                st.session_state["clicked_zone_csv"] = f"{sel_floor}-{label}"

    st.markdown("---")
    st.markdown("### 🔍 Zone 상세 보기")

    clicked = st.session_state.get("clicked_zone_csv", None)
    if not clicked:
        st.info("확인하실 Zone 버튼을 눌러 주세요.")
        return

    st.success(f"선택된 Zone: {clicked}")
    _, cz_label = clicked.split("-")

    ddf = fdf[fdf["zone_label"] == cz_label].copy()
    if ddf.empty:
        st.info("해당 Zone에는 벌크가 없습니다.")
        return

    show_cols = [
        "품목코드",
        "품명",
        "로트번호",
        "제품라인",
        "제조일자",
        "상태",
        "현재위치",
        "통번호",
        "통용량",
    ]
    st.dataframe(
        ddf[show_cols].sort_values(["로트번호", "통번호"]),
        use_container_width=True,
    )


# ==============================
# 탭 4: 이동 이력 (수정 + 행 삭제 가능)
# ==============================
def render_tab_move_log():
    st.markdown("### 📜 이동 이력 (수정 / 삭제 가능)")

    df = load_move_log()
    if df.empty:
        st.info("이동 이력이 없습니다.")
        return

    ss = st.session_state
    ss.setdefault("log_lot_filter", "")
    ss.setdefault("log_page", 1)

    # ▶ 검색 초기화 콜백 (여기서만 state 수정)
    def reset_log_filter():
        ss["log_lot_filter"] = ""
        ss["log_page"] = 1

    col1, col2 = st.columns([3, 1])
    with col1:
        lot_filter = st.text_input(
            "로트번호로 검색 (부분 일치)",
            key="log_lot_filter",
            placeholder="예: 2E075K",
        )
    with col2:
        st.button("검색 초기화", key="log_reset", on_click=reset_log_filter)

    # 필터 적용
    if lot_filter:
        mask = df["로트번호"].astype(str).str.contains(lot_filter.strip(), na=False)
        df_view = df[mask].copy()
    else:
        df_view = df.copy()

    if df_view.empty:
        st.info("검색 조건에 해당하는 이동 이력이 없습니다.")
        return

    # 시간 내림차순 정렬
    df_view = df_view.sort_values("시간", ascending=False)

    # --- 페이지네이션 ---
    page_size = 50
    total_rows = len(df_view)
    total_pages = max(1, math.ceil(total_rows / page_size))

    ss["log_page"] = min(max(1, ss.get("log_page", 1)), total_pages)

    colp1, colp2, colp3 = st.columns([1, 2, 1])
    with colp1:
        if st.button("◀ 이전", key="log_prev") and ss["log_page"] > 1:
            ss["log_page"] -= 1
    with colp2:
        st.write(f"페이지 {ss['log_page']} / {total_pages} (총 {total_rows}건)")
    with colp3:
        if st.button("다음 ▶", key="log_next") and ss["log_page"] < total_pages:
            ss["log_page"] += 1

    start = (ss["log_page"] - 1) * page_size
    end = start + page_size
    page_df = df_view.iloc[start:end].copy()

    # 표시/편집할 컬럼 + 삭제 체크박스 컬럼 추가
    cols_order = [
        "시간",
        "ID",
        "품번",
        "품명",
        "로트번호",
        "통번호",
        "변경 전 용량",
        "변경 후 용량",
        "변화량",
        "변경 전 위치",
        "변경 후 위치",
    ]
    page_df = page_df[cols_order]

    delete_col = "삭제"
    if delete_col not in page_df.columns:
        page_df[delete_col] = False

    st.caption(
        "※ '시간'과 'ID'는 수정할 수 없습니다. "
        "나머지 칼럼은 수정 가능하며, '삭제' 체크 후 '선택 행 삭제'를 누르면 해당 행이 삭제됩니다."
    )

    edited_page = st.data_editor(
        page_df,
        use_container_width=True,
        disabled=["시간", "ID"],   # 이 두 컬럼은 수정 불가
        column_config={
            delete_col: st.column_config.CheckboxColumn("삭제", help="삭제할 행에 체크"),
        },
        key=f"move_log_editor_page_{ss['log_page']}",
    )

    # 공통 저장 함수
    def _save_full_log(df_updated: pd.DataFrame):
        buf = io.BytesIO()
        df_updated.to_csv(buf, index=False, encoding="utf-8-sig")
        ss["move_log_csv_bytes"] = buf.getvalue()
        _load_move_log_core.clear()
        try:
            df_updated.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
        except Exception:
            pass

    col_save, col_delete = st.columns(2)

    # ✅ 내용 수정 저장
    with col_save:
        if st.button("변경 내용 저장", key="log_save_changes"):
            try:
                df_updated = df.copy()

                if delete_col in edited_page.columns:
                    edited_for_update = edited_page.drop(columns=[delete_col])
                else:
                    edited_for_update = edited_page

                df_updated.update(edited_for_update)
                _save_full_log(df_updated)
                st.success("이동 이력 변경 내용이 저장되었습니다.")
            except Exception as e:
                st.error(f"변경 내용을 저장하는 중 오류가 발생했습니다: {e}")

    # 🗑 선택 행 삭제
    with col_delete:
        if st.button("선택 행 삭제", key="log_delete_rows"):
            try:
                if delete_col in edited_page.columns:
                    to_del_idx = edited_page[edited_page[delete_col] == True].index
                else:
                    to_del_idx = []

                if len(to_del_idx) == 0:
                    st.warning("삭제할 행을 먼저 '삭제' 칼럼에 체크해 주세요.")
                else:
                    df_updated = df.drop(index=to_del_idx)
                    _save_full_log(df_updated)
                    st.success(f"총 {len(to_del_idx)}개 행이 삭제되었습니다.")
                    st.rerun()
            except Exception as e:
                st.error(f"행을 삭제하는 중 오류가 발생했습니다: {e}")


# ==============================
# 탭 5: 데이터 파일 관리 (메인 탭 중 데이터 탭)
# ==============================
def file_status(sess_key: str, path: str) -> str:
    ss = st.session_state
    if sess_key in ss:
        return "세션에 업로드된 파일 사용 중"
    if os.path.exists(path):
        return f"로컬 파일 사용 중 ({path})"
    return "파일 없음"


def render_tab_data():
    ss = st.session_state
    st.markdown("### 📁 데이터 파일 관리")
    st.write(
        "필요할 때마다 아래에서 CSV/엑셀 파일을 다시 업로드해서 교체할 수 있습니다. "
        "업로드하면 **현재 세션에서 바로 반영**됩니다."
    )

    # --- bulk_drums_extended.csv ---
    with st.expander("1) bulk_drums_extended.csv (메인 벌크 CSV)", expanded=True):
        st.write("현재 상태:", file_status("bulk_csv_bytes", CSV_PATH))
        bulk_file = st.file_uploader(
            "새 bulk_drums_extended.csv 업로드 (csv)",
            type=["csv"],
            key="data_up_bulk",
        )
        # 🔽 실제 파일 수정 시간 기준 캡션
        st.caption(last_upload_caption(CSV_PATH))

        if st.button("이 파일로 bulk CSV 교체", key="apply_bulk"):
            if bulk_file is None:
                st.warning("먼저 파일을 선택해 주세요.")
            else:
                data = bulk_file.read()
                ss["bulk_csv_bytes"] = data
                _load_drums_core.clear()
                try:
                    df_tmp = _load_drums_core(data)
                    df_tmp.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
                except Exception:
                    pass
                st.success("bulk_drums_extended.csv가 교체되었습니다.")

    # --- production.xlsx ---
    with st.expander("2) production.xlsx (자사 작업번호)", expanded=False):
        st.write("현재 상태:", file_status("prod_xlsx_bytes", PRODUCTION_FILE))
        prod_file = st.file_uploader(
            "새 production.xlsx 업로드",
            type=["xlsx"],
            key="data_up_prod",
        )
        st.caption(last_upload_caption(PRODUCTION_FILE))

        if st.button("이 파일로 production 교체", key="apply_prod"):
            if prod_file is None:
                st.warning("먼저 파일을 선택해 주세요.")
            else:
                data = prod_file.read()
                ss["prod_xlsx_bytes"] = data
                _load_production_core.clear()
                try:
                    df_tmp = _load_production_core(data)
                    df_tmp.to_excel(PRODUCTION_FILE, index=False)
                except Exception:
                    pass
                st.success("production.xlsx가 교체되었습니다.")

    # --- receive.xlsx ---
    with st.expander("3) receive.xlsx (사급 입하번호)", expanded=False):
        st.write("현재 상태:", file_status("recv_xlsx_bytes", RECEIVE_FILE))
        recv_file = st.file_uploader(
            "새 receive.xlsx 업로드",
            type=["xlsx"],
            key="data_up_recv",
        )
        st.caption(last_upload_caption(RECEIVE_FILE))

        if st.button("이 파일로 receive 교체", key="apply_recv"):
            if recv_file is None:
                st.warning("먼저 파일을 선택해 주세요.")
            else:
                data = recv_file.read()
                ss["recv_xlsx_bytes"] = data
                _load_receive_core.clear()
                try:
                    df_tmp = _load_receive_core(data)
                    df_tmp.to_excel(RECEIVE_FILE, index=False)
                except Exception:
                    pass
                st.success("receive.xlsx가 교체되었습니다.")

    # --- stock.xlsx ---
    with st.expander("4) stock.xlsx (전산 재고)", expanded=False):
        st.write("현재 상태:", file_status("stock_xlsx_bytes", STOCK_FILE))
        stock_file = st.file_uploader(
            "새 stock.xlsx 업로드",
            type=["xlsx"],
            key="data_up_stock",
        )
        st.caption(last_upload_caption(STOCK_FILE))

        if st.button("이 파일로 stock 교체", key="apply_stock"):
            if stock_file is None:
                st.warning("먼저 파일을 선택해 주세요.")
            else:
                data = stock_file.read()
                ss["stock_xlsx_bytes"] = data
                _load_stock_core.clear()
                try:
                    df_tmp = _load_stock_core(data)
                    df_tmp.to_excel(STOCK_FILE, index=False)
                except Exception:
                    pass
                st.success("stock.xlsx가 교체되었습니다.")

    # --- bulk_move_log.csv ---
    with st.expander("5) bulk_move_log.csv (이동 이력, 선택)", expanded=False):
        st.write("현재 상태:", file_status("move_log_csv_bytes", MOVE_LOG_CSV))
        move_file = st.file_uploader(
            "새 bulk_move_log.csv 업로드 (csv)",
            type=["csv"],
            key="data_up_move",
        )
        st.caption(last_upload_caption(MOVE_LOG_CSV))

        if st.button("이 파일로 이동 이력 교체", key="apply_move"):
            if move_file is None:
                st.warning("먼저 파일을 선택해 주세요.")
            else:
                data = move_file.read()
                ss["move_log_csv_bytes"] = data
                _load_move_log_core.clear()
                try:
                    df_tmp = _load_move_log_core(data)
                    df_tmp.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
                except Exception:
                    pass
                st.success("bulk_move_log.csv가 교체되었습니다.")

    st.markdown("---")
    st.caption(
        "※ Cloud에서는 세션이 초기화되면 다시 업로드해야 합니다. "
        "중요한 변경 내용은 사이드바의 다운로드 버튼으로 CSV를 저장해 두세요."
    )



# ==============================
# 메인
# ==============================
def main():
    ss = st.session_state

    # 1) 로그인 안 되어 있으면 로그인 화면만 표시
    if "user_id" not in ss or "user_name" not in ss:
        render_login()
        return

    # 2) 필수 데이터 파일 준비 여부 확인
    files_ready = (
        ("bulk_csv_bytes" in ss or os.path.exists(CSV_PATH))
        and ("prod_xlsx_bytes" in ss or os.path.exists(PRODUCTION_FILE))
        and ("recv_xlsx_bytes" in ss or os.path.exists(RECEIVE_FILE))
        and ("stock_xlsx_bytes" in ss or os.path.exists(STOCK_FILE))
    )

    # data_initialized 플래그가 없고, 필수 파일도 없으면 최초 업로드 화면
    if not ss.get("data_initialized", False) and not files_ready:
        bulk_file = st.file_uploader("1) bulk_drums_extended.csv (필수)", type=["csv"])
        st.caption(last_upload_caption(CSV_PATH))

        prod_file = st.file_uploader("2) production.xlsx (필수)", type=["xlsx"])
        st.caption(last_upload_caption(PRODUCTION_FILE))

        recv_file = st.file_uploader("3) receive.xlsx (필수)", type=["xlsx"])
        st.caption(last_upload_caption(RECEIVE_FILE))

        stock_file = st.file_uploader("4) stock.xlsx (필수)", type=["xlsx"])
        st.caption(last_upload_caption(STOCK_FILE))

        move_file = st.file_uploader("5) bulk_move_log.csv (선택)", type=["csv"])
        st.caption(last_upload_caption(MOVE_LOG_CSV))

        return

    # 3) 사이드바: 사용자 정보 + 로그아웃 + (선택) CSV 다운로드 버튼
    with st.sidebar:
        st.markdown(f"**사용자:** {ss['user_name']} ({ss['user_id']})")
        if st.button("로그아웃", key="logout_btn"):
            for k in ["user_id", "user_name"]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

        # 현재 세션의 bulk/move_log를 다운로드할 수 있게
        if "bulk_csv_bytes" in ss:
            st.download_button(
                "현재 bulk CSV 다운로드",
                data=ss["bulk_csv_bytes"],
                file_name="bulk_drums_extended_current.csv",
                mime="text/csv",
            )
        if "move_log_csv_bytes" in ss:
            st.download_button(
                "이동 이력 CSV 다운로드",
                data=ss["move_log_csv_bytes"],
                file_name="bulk_move_log_current.csv",
                mime="text/csv",
            )

    st.title("🏭 벌크 관리 시스템")

    tab_move, tab_lookup, tab_map, tab_log, tab_data = st.tabs(
        ["📦 이동", "🔍 조회", "🗺 지도", "📜 이동 이력", "📁 데이터"]
    )

    with tab_move:
        render_tab_move()
    with tab_lookup:
        render_tab_lookup()
    with tab_map:
        render_tab_map()
    with tab_log:
        render_tab_move_log()
    with tab_data:
        render_tab_data()


if __name__ == "__main__":
    main()
