import streamlit as st
import pandas as pd
import os
from datetime import datetime, date, timezone, timedelta
import io
import math
import boto3

KST = timezone(timedelta(hours=9))

def now_kst_str() -> str:
    """한국 시간(KST) 현재 시각을 'YYYY-MM-DD HH:MM:SS' 문자열로 반환."""
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    
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

    /* 🔹 st.form 테두리/배경 제거 */
    .stForm {
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
        background-color: transparent !important;
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

# ======
# 이동기록 버튼 크기
# ======
# HTML 버튼 클릭 처리
query_params = st.query_params

if "log_prev" in query_params:
    if ss["log_page"] > 1:
        ss["log_page"] -= 1
    st.query_params.clear()   # 클린
    st.rerun()

if "log_next" in query_params:
    if ss["log_page"] < total_pages:
        ss["log_page"] += 1
    st.query_params.clear()
    st.rerun()

st.markdown(
    """
    <style>
    /* 이동 이력 페이지 네비게이션 버튼만 약간 작게 */
    div.stButton > button[kind="secondary"] {
        font-size: 0.8rem !important;
        padding: 0.2rem 0.6rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ==============================
# S3 연동 설정
# ==============================
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


def s3_upload_bytes(filename: str, data: bytes):
    """
    업로드된 파일 바이트를 S3에 저장.
    filename: 로컬에서 사용하는 파일명을 그대로 넘기면 _s3_key로 S3 경로 변환.
    """
    if not s3_enabled():
        return
    client = get_s3_client()
    if not client:
        return
    try:
        client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=_s3_key(filename),
            Body=data,
        )
    except Exception:
        # S3 오류가 나더라도 앱 전체는 죽지 않게 조용히 무시
        pass


def s3_download_bytes(filename: str):
    """
    S3에서 파일을 읽어와서 bytes로 반환.
    없거나 오류면 None 반환.
    """
    if not s3_enabled():
        return None
    client = get_s3_client()
    if not client:
        return None
    try:
        resp = client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=_s3_key(filename),
        )
        return resp["Body"].read()
    except Exception:
        return None


# ==============================
# 공통 유틸 (업로드/로컬/S3 겸용)
# ==============================
@st.cache_data(show_spinner=False)
def _load_drums_core(bulk_bytes):
    """bulk_drums_extended.csv 로드 (세션 업로드 > 로컬 > S3 순서)."""
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
    # 3) 로컬도 없으면 S3에서 시도
    else:
        s3_bytes = s3_download_bytes(CSV_PATH)
        if s3_bytes is not None:
            try:
                df = pd.read_csv(io.BytesIO(s3_bytes))
            except Exception as e:
                st.error(f"S3의 bulk_drums_extended.csv를 읽는 중 오류가 발생했습니다: {e}")
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
    - S3에도 업로드
    """
    # 1) 세션 메모리 갱신
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    data = buf.getvalue()
    st.session_state["bulk_csv_bytes"] = data

    # 캐시 무효화
    _load_drums_core.clear()

    # 2) 로컬 CSV로도 저장 (있으면)
    try:
        df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    except Exception:
        # Cloud 환경에서는 보통 권한/경로가 없으니 조용히 무시
        pass

    # 3) S3 업로드
    s3_upload_bytes(CSV_PATH, data)


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
        # 로컬도 없으면 S3 시도
        s3_bytes = s3_download_bytes(PRODUCTION_FILE)
        if s3_bytes is not None:
            try:
                df = pd.read_excel(io.BytesIO(s3_bytes))
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
        s3_bytes = s3_download_bytes(RECEIVE_FILE)
        if s3_bytes is not None:
            try:
                df = pd.read_excel(io.BytesIO(s3_bytes))
            except Exception as e:
                st.error(f"S3의 receive.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
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
        s3_bytes = s3_download_bytes(STOCK_FILE)
        if s3_bytes is not None:
            try:
                df = pd.read_excel(io.BytesIO(s3_bytes))
            except Exception as e:
                st.error(f"S3의 stock.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
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
# 이동 LOG 유틸 (ID 포함, 업로드/세션/S3 겸용)
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
        s3_bytes = s3_download_bytes(MOVE_LOG_CSV)
        if s3_bytes is not None:
            try:
                df = pd.read_csv(io.BytesIO(s3_bytes))
            except Exception as e:
                st.error(f"S3의 이동 이력 파일을 읽는 중 오류가 발생했습니다: {e}")
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

def save_move_log(df: pd.DataFrame):
    """
    이동 이력 DataFrame 전체를 bulk_move_log.csv 및 세션/S3에 저장.
    (기존 내용을 유지한 채 덮어쓰기 방식으로 전체 저장)
    """
    ss = st.session_state

    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    data = buf.getvalue()

    # 세션에 반영
    ss["move_log_csv_bytes"] = data

    # 캐시 클리어
    _load_move_log_core.clear()

    # 로컬 CSV 저장
    try:
        df.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
    except Exception:
        pass

    # S3 업로드
    s3_upload_bytes(MOVE_LOG_CSV, data)



def write_move_log(item_code: str, item_name: str, lot: str, drum_infos, from_zone: str, to_zone: str):
    """
    이동 이력을 bulk_move_log.csv에 기록.
    drum_infos:
      - 옛 형식: (통번호, moved_qty, old_qty, new_qty)
      - 새 형식: (통번호, moved_qty, old_qty, new_qty, old_loc)
    ID 열에는 로그인한 사용자의 '표시 이름'을 남긴다.
    """
    if not drum_infos:
        return

    ss = st.session_state
    user_display_name = ss.get("user_name", "")

    ts = now_kst_str()  # 🔹 한국 시간 기준

    rows = []
    for info in drum_infos:
        # 🔹 튜플 길이에 따라 분기 (옛 데이터와 호환)
        if len(info) == 4:
            drum_no, moved_qty, old_qty, new_qty = info
            old_loc = from_zone
        else:
            drum_no, moved_qty, old_qty, new_qty, old_loc = info

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
                "변경 전 위치": old_loc,
                "변경 후 위치": to_zone,
            }
        )

    new_df = pd.DataFrame(rows)

    # 기존 로그 불러오기 (세션/로컬/S3)
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
        s3_bytes = s3_download_bytes(MOVE_LOG_CSV)
        if s3_bytes is not None:
            try:
                old_df = pd.read_csv(io.BytesIO(s3_bytes))
            except Exception:
                old_df = pd.DataFrame()
        else:
            old_df = pd.DataFrame()

    log_df = pd.concat([old_df, new_df], ignore_index=True)

    # 1) 세션에 다시 저장
    buf = io.BytesIO()
    log_df.to_csv(buf, index=False, encoding="utf-8-sig")
    data = buf.getvalue()
    ss["move_log_csv_bytes"] = data

    _load_move_log_core.clear()

    # 2) 로컬 CSV에도 저장 (로컬 실행용)
    try:
        log_df.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
    except Exception:
        pass

    # 3) S3 업로드
    s3_upload_bytes(MOVE_LOG_CSV, data)


# ==============================
# 업로드 시간 표시 유틸  (S3 → 로컬 순으로 확인)
# ==============================
from datetime import datetime as dt_for_caption


@st.cache_data(show_spinner=False, ttl=60)
def last_upload_caption(filename: str) -> str:
    """
    파일의 마지막 업로드 시간을 KST(UTC+9) 시간으로 표시
    1) S3 → 2) 로컬 파일 → 3) 없으면 표시 없음
    """
    from datetime import timezone, timedelta, datetime as dt

    # KST timezone
    KST = timezone(timedelta(hours=9))

    # ------------------------
    # 1) S3 timestamp
    # ------------------------
    try:
        if s3_enabled():
            client = get_s3_client()
            if client:
                s3_path = _s3_key(filename)
                resp = client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_path)

                lm = resp["LastModified"]     # timezone-aware datetime
                lm_kst = lm.astimezone(KST)   # 👉 KST 로 변환

                return f"S3 마지막 수정: {lm_kst.strftime('%Y-%m-%d %H:%M:%S')}"
    except Exception:
        pass

    # ------------------------
    # 2) Local file timestamp
    # ------------------------
    if os.path.exists(filename):
        try:
            ts = os.path.getmtime(filename)        # float (UTC 기준 timestamp)
            lm_kst = dt.fromtimestamp(ts, KST)     # 👉 timestamp 를 KST 로 변환
            return f"로컬 마지막 수정: {lm_kst.strftime('%Y-%m-%d %H:%M:%S')}"
        except Exception:
            return "로컬 파일 시간 읽기 오류"

    # ------------------------
    # 3) No file
    # ------------------------
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

        # 🔹 S3 업로드 (원본 바이트 그대로 보관)
        s3_upload_bytes(CSV_PATH, bulk_bytes)
        s3_upload_bytes(PRODUCTION_FILE, prod_bytes)
        s3_upload_bytes(RECEIVE_FILE, recv_bytes)
        s3_upload_bytes(STOCK_FILE, stock_bytes)
        if move_bytes is not None:
            s3_upload_bytes(MOVE_LOG_CSV, move_bytes)

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

    # 🔹 이전에 로그인했던 ID가 있으면 기본값으로 넣어주기
    #    (단, 이번 세션에서 login_id가 아직 안 만들어졌을 때만)
    if "last_login_id" in ss and "login_id" not in ss:
        ss["login_id"] = ss["last_login_id"]

    st.title("🏭 벌크 관리 시스템 - 로그인")
    st.markdown("작업 전 ID와 비밀번호를 입력해 주세요.")

    # ✅ form 사용: 엔터로도 로그인, 버튼으로도 로그인
    with st.form("login_form"):
        login_id = st.text_input("ID", key="login_id")
        login_pw = st.text_input("비밀번호", type="password", key="login_pw")

        login_submitted = st.form_submit_button("로그인")

    # 폼 제출(엔터 또는 버튼 클릭) 시 로그인 처리
    if login_submitted:
        user = USER_ACCOUNTS.get((login_id or "").strip())

        if user and login_pw == user["password"]:
            ss["user_id"] = (login_id or "").strip()
            ss["user_name"] = user["display_name"]

            # 🔹 마지막에 성공적으로 로그인한 ID 기억
            ss["last_login_id"] = (login_id or "").strip()

            # 혹시 예전에 쓰던 로그인 유지 관련 키가 있다면 정리 (선택 사항)
            for k in ["remember_me", "login_remember_checkbox"]:
                if k in ss:
                    del ss[k]

            st.success(f"{user['display_name']}님, 환영합니다.")
            st.rerun()
        else:
            st.error("ID 또는 비밀번호가 올바르지 않습니다.")

# ==============================
# stock.xlsx 기반 전산 재고 요약
# ==============================
def get_stock_summary(item_code: str, lot: str):
    """
    stock.xlsx에서
      - 품번(C열) == item_code
      - 로트번호(G열) == lot
      - 실재고수량(K열) != 0
    조건을 만족하는 전산재고를 찾아
      '대분류(창고명) 실재고수량kg'
    형식으로 변환하여 반환한다.
    """

    stock_df = load_stock()
    if stock_df is None or stock_df.empty:
        return None, ""

    if not item_code or not lot:
        return None, ""

    cols = list(stock_df.columns)
    # 열 이름 설정 (한글이 없으면 위치(index) 기준)
    try:
        col_code = "창고코드" if "창고코드" in cols else cols[0]      # A열
        col_name = "창고명"   if "창고명"   in cols else cols[1]      # B열
        col_item = "품번"     if "품번"     in cols else cols[2]      # C열
        col_lot  = "로트번호" if "로트번호" in cols else cols[6]      # G열
        col_qty  = "실재고수량" if "실재고수량" in cols else cols[10] # K열
    except Exception:
        return None, ""

    df = stock_df.copy()

    # 비교용으로 문자열 정리
    df[col_item] = df[col_item].astype(str).str.strip().str.lower()
    df[col_lot]  = df[col_lot].astype(str).str.strip().str.lower()

    item_key = str(item_code).strip().lower()
    lot_key  = str(lot).strip().lower()

    # 실재고수량 numeric 변환
    df[col_qty] = pd.to_numeric(df[col_qty], errors="coerce").fillna(0)

    # 조건 필터
    mask = (
        (df[col_item] == item_key) &
        (df[col_lot] == lot_key) &
        (df[col_qty] != 0)
    )
    sub = df[mask].copy()
    if sub.empty:
        return None, ""

    # ----- A열 코드 분류 -----
    JASA = {"WC301", "WC501", "WC502", "WC503", "WC504"}
    WAREHOUSE = {"WH201", "WH701", "WH301", "WH601", "WH401", "WH506"}
    BAD = {"WH001", "WH102", "WH202"}

    def classify(code):
        c = str(code).strip()
        if c in JASA:
            return "자사"
        if c in WAREHOUSE:
            return "창고"
        if c in BAD:
            return "불량"
        return "외주"

    sub["창고코드"] = sub[col_code].astype(str).str.strip()
    sub["창고명"] = sub[col_name].astype(str).str.strip()
    sub["실재고수량"] = pd.to_numeric(sub[col_qty], errors="coerce").fillna(0.0)
    sub["대분류"] = sub["창고코드"].apply(classify)

    # 대분류 + 창고명 기준 합산
    grouped = (
        sub.groupby(["대분류", "창고코드", "창고명"], as_index=False)["실재고수량"]
        .sum()
    )

    # 수량 포맷
    def fmt(v):
        f = float(v)
        return str(int(f)) if f.is_integer() else str(f)

    # 최종 표시 문구 생성: 대분류(창고명) 실재고수량kg
    grouped["표시"] = grouped.apply(
        lambda r: f"{r['대분류']}({r['창고명']}) {fmt(r['실재고수량'])}kg",
        axis=1,
    )

    detail_df = grouped[["대분류", "창고코드", "창고명", "실재고수량"]].copy()
    detail_df = detail_df.sort_values(["대분류", "창고코드"])

    summary_text = " / ".join(grouped["표시"].tolist())

    return detail_df, summary_text


# ==============================
# 탭 1: 이동 - 입력값 초기화
# ==============================
def clear_move_inputs():
    """이동 탭 입력값 초기화 콜백."""
    ss = st.session_state

    for k in [
        "mv_barcode",
        "mv_lot",
        "mv_last_lot",
        "mv_last_barcode",
        "mv_search_by_lot",
        "mv_searched_csv",
        "mv_show_stock_detail",
        "mv_show_move_history_here",
        "clicked_zone_csv",
        "mv_just_searched",
    ]:
        if k in ss:
            del ss[k]

# ==============================
# 탭 1: 이동
# ==============================
def render_tab_move():
    st.markdown("### 📦 벌크 이동")

    ss = st.session_state
    ss.setdefault("mv_searched_csv", False)
    ss.setdefault("mv_search_by_lot", False)
    ss.setdefault("mv_show_stock_detail", False)
    ss.setdefault("mv_show_move_history_here", False)

    # ================== 검색 폼 (엔터 + 버튼 둘 다 가능) ==================
    with st.form("move_search_form"):
        bulk_type = st.radio(
            "벌크 구분을 선택해 주세요.",
            ["자사", "사급"],
            horizontal=True,
            key="mv_bulk_type_csv",
        )

        barcode_label = "작업번호를 입력해 주세요." if bulk_type == "자사" else "입하번호를 입력해 주세요."

        # 🔹 입력칸 두 개 나란히 (예전 너비 느낌 유지)
        col_in1, col_in2, _sp = st.columns([0.45, 0.45, 2.5])
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

        # 🔹 조회하기 / 초기화 버튼 한 줄
        col_b1, col_b2, _sp2 = st.columns([1, 1, 6])
        with col_b1:
            search_submit = st.form_submit_button("조회하기", use_container_width=True)
        with col_b2:
            reset_submit = st.form_submit_button("초기화", use_container_width=True)

    # ----- 초기화 버튼 -----
    if reset_submit:
        clear_move_inputs()
        st.rerun()

    # ----- 조회 버튼: 이번 입력을 "마지막 조회 조건"으로 저장 -----
    if search_submit:
        lot_val = (lot_input or "").strip()
        barcode_val = (barcode or "").strip()

        ss["mv_last_lot"] = lot_val
        ss["mv_last_barcode"] = barcode_val
        ss["mv_search_by_lot"] = bool(lot_val)  # 로트가 있으면 로트 기준 조회
        ss["mv_searched_csv"] = True
        ss["mv_just_searched"] = True  # 이번에 막 조회함 표시

    # 🔹 한 번도 조회한 적 없으면 아래는 안 그림
    if not ss.get("mv_searched_csv", False):
        return

    # 여기부터는 "마지막 조회 조건" 기반으로 항상 화면 그림
    bulk_type = ss.get("mv_bulk_type_csv", "자사")
    df = load_drums()
    prod_df = load_production()
    recv_df = load_receive()

    lot = ""
    item_code = ""
    item_name = ""
    prod_date = ""
    prod_qty = None
    line = ""
    barcode_used = ""
    lot_lower = ""

    search_by_lot = ss.get("mv_search_by_lot", False)

    # ================== 로트 / 작업번호 / 입하번호 해석 ==================
    if search_by_lot:
        lot_input = (ss.get("mv_last_lot") or "").strip()
        if not lot_input:
            st.warning("로트번호가 비어 있습니다.")
            ss["mv_searched_csv"] = False
            return

        lot = lot_input
        lot_lower = lot_input.lower()
        barcode_used = lot_input

    else:
        barcode_query = (ss.get("mv_last_barcode") or "").strip()
        if not barcode_query:
            st.warning("작업번호/입하번호가 비어 있습니다.")
            ss["mv_searched_csv"] = False
            return

        barcode_used = barcode_query
        q = barcode_query.lower()

        if bulk_type == "자사":
            # 🟡 자사: 작업번호 (대소문자 무시)
            if prod_df.empty:
                st.error("production.xlsx 파일을 읽을 수 없어서 작업번호 기반 조회를 할 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            hit = prod_df[prod_df["작업번호"].astype(str).str.lower() == q]
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

        else:
            # 🟡 사급: 입하번호 (대소문자 무시)
            if recv_df.empty:
                st.error("receive.xlsx 파일을 찾을 수 없습니다.")
                ss["mv_searched_csv"] = False
                return

            if "입하번호" not in recv_df.columns:
                st.error("receive.xlsx에 '입하번호' 열이 없습니다.")
                ss["mv_searched_csv"] = False
                return

            hit = recv_df[recv_df["입하번호"].astype(str).str.lower() == q]
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

        lot_lower = (lot or "").lower()

    # ---------- LOT 기준으로 CSV 조회 (대소문자 무시) ----------
    df = load_drums()
    df["lot_lower"] = df["로트번호"].astype(str).str.lower()
    lot_df = df[df["lot_lower"] == lot_lower].copy()

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

    if stock_summary_text:  # 요약 문자열 있으면 그걸 그대로 사용
        # 예: "창고(부자재창고) 10kg / 외주(위드맘) 20kg"
        stock_loc_display = stock_summary_text
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

        # 현재 위치 + [상세보기] + [이동이력]
        loc_col1, loc_col2 = st.columns([3, 2])
        with loc_col1:
            st.markdown(f"**현재 위치(전산 기준):** {stock_loc_display}")
        with loc_col2:
            b1_col, b_sp, b2_col = st.columns([1, 0.05, 1])
            with b1_col:
                if st.button("상세보기", key=f"stock_detail_btn_{lot}"):
                    ss["mv_show_stock_detail"] = not ss.get("mv_show_stock_detail", False)
            with b2_col:
                if st.button("이동이력", key=f"move_hist_btn_{lot}"):
                    ss["mv_show_move_history_here"] = not ss.get("mv_show_move_history_here", False)

        if ss.get("mv_show_stock_detail", False):
            if stock_summary_df is not None and not stock_summary_df.empty:
                st.markdown("#### 🔎 전산 재고 상세")

                # 행 수 기반 높이 자동 조정 (행당 약 35px + 헤더 40px)
                n_rows = len(stock_summary_df)
                row_h = 35
                header_h = 40
                height = header_h + row_h * (n_rows + 1)

                st.dataframe(
                    stock_summary_df,
                    use_container_width=True,
                    height=height
                )
            else:
                st.info("전산 재고 데이터가 없습니다.")


        st.markdown("### 🛢 통 선택 및 잔량 입력")

        selected_drums = []
        drum_new_qty = {}

        drum_list = lot_df["통번호"].tolist()
        c1, c_sp, c2, _c_gap = st.columns([2, 0.5, 2, 7])
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

        # ================== 이동 내용 저장 버튼 ==================
        if st.button("이동 내용 저장 (CSV 반영)", key="mv_save_csv"):
            if not selected_drums:
                st.warning("이동하실 통을 한 개 이상 선택해 주세요.")
                return

            df_all = load_drums()
            df_all["lot_lower"] = df_all["로트번호"].astype(str).str.lower()
            lot_mask = df_all["lot_lower"] == lot_lower

            drum_logs = []

            for dn in selected_drums:
                idx = df_all.index[lot_mask & (df_all["통번호"] == dn)]
                if len(idx) == 0:
                    continue
                i = idx[0]

                # 🔹 변경 전 위치(통마다) 먼저 확보
                old_loc = str(df_all.at[i, "현재위치"] or "")

                old_qty = float(df_all.at[i, "통용량"])
                new_qty = drum_new_qty.get(dn, old_qty)
                moved = old_qty - new_qty

                # CSV 업데이트
                df_all.at[i, "통용량"] = new_qty
                df_all.at[i, "현재위치"] = to_zone

                if to_zone == "외주":
                    df_all.at[i, "상태"] = "외주"
                else:
                    df_all.at[i, "상태"] = move_status

                # 🔹 통번호, 변화량, 전/후 용량, 변경 전 위치까지 담아서 로그로 보냄
                drum_logs.append((dn, moved, old_qty, new_qty, old_loc))

            save_drums(df_all)

            write_move_log(
                item_code=item_code,
                item_name=item_name,
                lot=lot,
                drum_infos=drum_logs,
                from_zone=from_zone,  # 없던 옛 형식과도 호환용으로 유지
                to_zone=to_zone,
            )

            st.success(f"총 {len(drum_logs)}개의 통 정보가 CSV 및 이동 이력에 반영되었습니다.")


    # ================== 이동 탭 내부 LOT 이동 이력 ==================
    if ss.get("mv_show_move_history_here", False):
        log_df = load_move_log()
        if log_df.empty:
            st.info("이동 이력이 없습니다.")
        else:
            sub = log_df[log_df["로트번호"].astype(str).str.lower() == lot_lower].copy()
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

    # 제조일자 기준 TAT(개월) 컬럼 추가
    df = add_tat_column(df)

    query = st.text_input("로트번호, 품목코드 또는 품명을 입력해 주세요.")
    if query:
        q = query.strip()
        mask = (
            df["로트번호"].astype(str).str.contains(q, case=False, na=False)
            | df["품목코드"].astype(str).str.contains(q, case=False, na=False)
            | df["품명"].astype(str).str.contains(q, case=False, na=False)
        )
        df_view = df[mask]
    else:
        df_view = df

    # 용량 0 포함 여부 (기본: 미포함)
    include_zero = st.checkbox("용량 0 포함", value=False)

    if not include_zero:
        df_view = df_view[df_view["통용량"] > 0]

    if df_view.empty:
        st.warning("검색 결과가 없습니다.")
        return

    st.markdown("#### 📄 행별 상세")
    st.dataframe(df_view, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 📊 현재위치별 용량 요약")

    def show_summary_table(df_part: pd.DataFrame, title: str, width: int = 400):
        st.markdown(f"##### {title}")
        if df_part.empty:
            st.info("데이터가 없습니다.")
            return

        summary = (
            df_part.groupby("현재위치", dropna=False)
            .agg(
                통개수=("통번호", "count"),
                총용량_kg=("통용량", "sum"),
            )
            .reset_index()
            .sort_values("현재위치")
        )

        # 합계 행 추가
        total_row = pd.DataFrame({
            "현재위치": ["합계"],
            "통개수": [summary["통개수"].sum()],
            "총용량_kg": [summary["총용량_kg"].sum()],
        })
        summary = pd.concat([summary, total_row], ignore_index=True)

        row_height = 35
        header_height = 40
        dynamic_height = header_height + row_height * (len(summary) + 1)

        st.dataframe(summary, width=width, height=dynamic_height)

    # 층(또는 구역) 기준으로 분류용 컬럼
    tmp = df_view.copy()
    tmp["층"] = tmp["현재위치"].astype(str).str.split("-").str[0]

    # 1) 자사 위치: 2층, 4층, 5층, 6층
    df_onsite = tmp[tmp["층"].isin(["2층", "4층", "5층", "6층"])]

    # 2) 외주
    df_outsourcing = tmp[tmp["층"] == "외주"]

    # 3) 창고
    df_warehouse = tmp[tmp["층"] == "창고"]

    # 4) 소진 + 폐기
    df_consumed = tmp[tmp["층"].isin(["소진", "폐기"])]

    # 표 4개 출력
    show_summary_table(df_onsite, "1) 자사 위치 (2층 / 4층 / 5층 / 6층)")
    show_summary_table(df_outsourcing, "2) 외주")
    show_summary_table(df_warehouse, "3) 창고")
    show_summary_table(df_consumed, "4) 소진 / 폐기")

    st.markdown("---")
    if st.button("현재 CSV를 그대로 백업 저장하기"):
        KST = timezone(timedelta(hours=9))
        ts = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        backup_name = f"bulk_drums_extended_backup_{ts}.csv"

        df.to_csv(backup_name, index=False, encoding="utf-8-sig")
        st.success(f"백업 파일로 저장되었습니다: {backup_name}")



# ==============================
# 탭 3: 지도 (A1~C3 버튼)
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
    st.markdown("### 📜 이동 이력 (롤백 전용 / 삭제만 가능)")

    df = load_move_log()
    if df.empty:
        st.info("이동 이력이 없습니다.")
        return

    ss = st.session_state
    ss.setdefault("log_lot_filter", "")
    ss.setdefault("log_page", 1)

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

    if lot_filter:
        q = lot_filter.strip().lower()
        df["lot_lower"] = df["로트번호"].astype(str).str.lower()
        mask = df["lot_lower"].str.contains(q, na=False)
        df_view = df[mask].copy()
    else:
        df_view = df.copy()

    if df_view.empty:
        st.info("검색 조건에 해당하는 이동 이력이 없습니다.")
        return

    df_view = df_view.sort_values("시간", ascending=False)

    page_size = 50
    total_rows = len(df_view)
    total_pages = max(1, math.ceil(total_rows / page_size))

    # 현재 페이지 번호 보정
    ss["log_page"] = min(max(1, ss.get("log_page", 1)), total_pages)

    # 🔹 페이지 네비게이션 (이전 / 페이지 / 다음) – 한 줄 배치 시도
    colp1, colp2, colp3 = st.columns([1, 2, 1])

    with colp1:
        prev_clicked = st.button("◀ 이전", key="log_prev")

    with colp2:
        st.markdown(
            f"<div style='text-align:center; font-size:0.85rem;'>"
            f"페이지 {ss['log_page']} / {total_pages}<br>(총 {total_rows}건)"
            f"</div>",
            unsafe_allow_html=True,
        )

    with colp3:
        next_clicked = st.button("다음 ▶", key="log_next")

    if prev_clicked and ss["log_page"] > 1:
        ss["log_page"] -= 1
        st.experimental_rerun()
    if next_clicked and ss["log_page"] < total_pages:
        ss["log_page"] += 1
        st.experimental_rerun()

    # 🔹 현재 페이지에 해당하는 구간만 잘라서 표시
    start = (ss["log_page"] - 1) * page_size
    end = start + page_size
    page_df = df_view.iloc[start:end].copy()
    
    start = (ss["log_page"] - 1) * page_size
    end = start + page_size
    page_df = df_view.iloc[start:end].copy()

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
        "※ LOG는 수정할 수 없습니다. "
        "조회만 가능하며, '삭제'에 체크 후 '선택 행 삭제(롤백)'을 누르면 "
        "해당 이동 이력은 삭제되고, 통 정보 CSV는 변경 전 상태로 롤백됩니다.\n"
        "※ 안전을 위해 각 통의 '가장 최근 이동 이력'만 삭제할 수 있습니다."
    )

    # 🔹 모든 칼럼은 읽기 전용, '삭제'만 체크 가능
    edited_page = st.data_editor(
        page_df,
        use_container_width=True,
        disabled=cols_order,  # 시간~변경 후 위치까지 전부 읽기 전용
        column_config={
            delete_col: st.column_config.CheckboxColumn("삭제", help="롤백할 행에 체크"),
        },
        key=f"move_log_editor_page_{ss['log_page']}",
    )

    def _save_full_log(df_updated: pd.DataFrame):
        buf = io.BytesIO()
        df_updated.to_csv(buf, index=False, encoding="utf-8-sig")
        data = buf.getvalue()
        ss["move_log_csv_bytes"] = data
        _load_move_log_core.clear()
        try:
            df_updated.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")
        except Exception:
            pass
        s3_upload_bytes(MOVE_LOG_CSV, data)

    # 🔹 이제는 삭제(롤백) 버튼만 존재
    _, col_delete = st.columns([3, 1])

    with col_delete:
        if st.button("선택 행 삭제 (롤백)", key="log_delete_rows"):
            try:
                if delete_col in edited_page.columns:
                    to_del_idx = edited_page[edited_page[delete_col] == True].index
                else:
                    to_del_idx = []

                if len(to_del_idx) == 0:
                    st.warning("먼저 롤백할 행을 '삭제' 칼럼에 체크해 주세요.")
                    return

                # 원본 전체 로그에서 삭제 대상 행 추출
                rows_to_delete = df.loc[to_del_idx].copy()

                # 1) 각 통(로트번호+통번호)의 '가장 최신 이력'인지 확인
                log_all = df.copy()
                log_all["__dt"] = pd.to_datetime(log_all["시간"], errors="coerce")

                not_latest = []
                for idx, row in rows_to_delete.iterrows():
                    lot = str(row["로트번호"])
                    drum_no = int(row["통번호"])

                    mask = (
                        log_all["로트번호"].astype(str) == lot
                    ) & (log_all["통번호"] == drum_no)
                    sub = log_all[mask]

                    if sub.empty:
                        continue

                    sub_valid = sub.dropna(subset=["__dt"])
                    if not sub_valid.empty:
                        last_idx = sub_valid["__dt"].idxmax()
                    else:
                        # 시간 파싱이 안 되면, 인덱스 기준으로 가장 큰 값 = 마지막
                        last_idx = sub.index.max()

                    if idx != last_idx:
                        not_latest.append(f"{lot} / 통 {drum_no}")

                if not_latest:
                    st.error(
                        "롤백은 각 통의 '가장 최근 이동 이력'만 삭제할 수 있습니다.\n"
                        "다음 항목은 더 새로운 이력이 있어 롤백할 수 없습니다:\n"
                        + ", ".join(not_latest)
                    )
                    return

                # 2) 통 정보 CSV 롤백
                drums_df = load_drums()
                drums_df["lot_lower"] = drums_df["로트번호"].astype(str).str.lower()

                for _, row in rows_to_delete.iterrows():
                    lot = str(row["로트번호"])
                    lot_lower = lot.lower()
                    drum_no = int(row["통번호"])

                    old_qty = float(row["변경 전 용량"])
                    from_loc = str(row["변경 전 위치"]) if not pd.isna(row["변경 전 위치"]) else ""

                    mask_drum = (drums_df["lot_lower"] == lot_lower) & (drums_df["통번호"] == drum_no)
                    drum_idxs = drums_df.index[mask_drum]

                    if len(drum_idxs) == 0:
                        # 해당 통 정보가 CSV에 없으면 스킵
                        continue

                    i = drum_idxs[0]
                    drums_df.at[i, "통용량"] = old_qty
                    if from_loc:
                        drums_df.at[i, "현재위치"] = from_loc
                    # 상태까지 완벽히 복원하려면 로그에 상태를 추가로 기록해야 함.
                    # 지금은 통용량/현재위치만 롤백.

                if "lot_lower" in drums_df.columns:
                    drums_df = drums_df.drop(columns=["lot_lower"])
                save_drums(drums_df)

                # 3) 이동 로그에서 행 삭제 + 저장
                df_updated = df.drop(index=to_del_idx)
                _save_full_log(df_updated)

                st.success(f"총 {len(to_del_idx)}개 이동 이력이 삭제되고, 관련 통 정보가 롤백되었습니다.")
                st.rerun()

            except Exception as e:
                st.error(f"행을 삭제(롤백)하는 중 오류가 발생했습니다: {e}")

# ==============================
# 탭 5: 데이터 파일 관리
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
                s3_upload_bytes(CSV_PATH, data)
                st.success("bulk_drums_extended.csv가 교체되었습니다.")

    # --- production.xlsx ---
    with st.expander("2) production.xlsx (제조작업실적현황)", expanded=False):
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
                s3_upload_bytes(PRODUCTION_FILE, data)
                st.success("production.xlsx가 교체되었습니다.")

    # --- receive.xlsx ---
    with st.expander("3) receive.xlsx (입하현황)", expanded=False):
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
                s3_upload_bytes(RECEIVE_FILE, data)
                st.success("receive.xlsx가 교체되었습니다.")

    # --- stock.xlsx ---
    with st.expander("4) stock.xlsx (일자별통합재고현황)", expanded=False):
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
                s3_upload_bytes(STOCK_FILE, data)
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
                s3_upload_bytes(MOVE_LOG_CSV, data)
                st.success("bulk_move_log.csv가 교체되었습니다.")

    st.markdown("---")
    st.caption(
        "※ Cloud에서는 세션이 초기화되면 다시 업로드해야 합니다. "
        "중요한 변경 내용은 사이드바의 다운로드 버튼으로 CSV를 저장해 두세요."
    )


# ==============================
# 메인
# ==============================
def has_data(sess_key: str, path: str) -> bool:
    """
    세션, 로컬 파일, S3 중 하나라도 있으면 True.
    """
    ss = st.session_state
    if sess_key in ss:
        return True
    if os.path.exists(path):
        return True
    b = s3_download_bytes(path)
    if b is not None:
        return True
    return False


def main():
    ss = st.session_state

    # 1) 로그인 안 되어 있으면 로그인 화면만 표시
    if "user_id" not in ss or "user_name" not in ss:
        render_login()
        return

    # 2) 필수 데이터 파일 준비 여부 확인
    files_ready = (
        has_data("bulk_csv_bytes", CSV_PATH)
        and has_data("prod_xlsx_bytes", PRODUCTION_FILE)
        and has_data("recv_xlsx_bytes", RECEIVE_FILE)
        and has_data("stock_xlsx_bytes", STOCK_FILE)
    )

    if not ss.get("data_initialized", False) and not files_ready:
        render_file_loader()
        return

    # 3) 사이드바
    with st.sidebar:
        st.markdown(f"**사용자:** {ss['user_name']} ({ss['user_id']})")
        if st.button("로그아웃", key="logout_btn"):
            for k in ["user_id", "user_name"]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

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
