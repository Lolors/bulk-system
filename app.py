import streamlit as st
import pandas as pd
import os
from datetime import datetime
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

# 라이선스 키 (지금 쓰는 그대로)
DBR_LICENSE = st.secrets["DBR_LICENSE"]


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
# 공통 유틸
# ==============================
@st.cache_data(show_spinner=False)
def load_drums() -> pd.DataFrame:
    """bulk_drums_extended.csv 로드."""
    if not os.path.exists(CSV_PATH):
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


def save_drums(df: pd.DataFrame):
    """현재 DF를 bulk_drums_extended.csv에 그대로 저장"""
    load_drums.clear()  # 캐시 무효화
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")


@st.cache_data(show_spinner=False)
def load_production():
    """production.xlsx 로드 (자사 작업번호용)"""
    if not os.path.exists(PRODUCTION_FILE):
        return pd.DataFrame()

    try:
        df = pd.read_excel(PRODUCTION_FILE)
    except Exception:
        return pd.DataFrame()

    required = ["작업번호", "품번", "품명", "LOTNO", "지시수량", "제조량", "작업일자"]
    for c in required:
        if c not in df.columns:
            return pd.DataFrame()

    return df[required].copy()


@st.cache_data(show_spinner=False)
def load_receive():
    """receive.xlsx 로드 (사급 입하번호용)"""
    if not os.path.exists(RECEIVE_FILE):
        return pd.DataFrame()
    try:
        df = pd.read_excel(RECEIVE_FILE)
    except Exception as e:
        st.error(f"receive.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()
    return df


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
# 이동 LOG 유틸 (ID 포함)
# ==============================
@st.cache_data(show_spinner=False)
def load_move_log() -> pd.DataFrame:
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

    if not os.path.exists(MOVE_LOG_CSV):
        return pd.DataFrame(columns=default_cols)

    try:
        df = pd.read_csv(MOVE_LOG_CSV)
    except Exception as e:
        st.error(f"이동 이력 파일을 읽는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame(columns=default_cols)

    # 예전 로그에 ID열이 없을 수도 있으니 보정
    for c in default_cols:
        if c not in df.columns:
            if c == "ID":
                df[c] = ""
            else:
                df[c] = pd.NA

    return df[default_cols]


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

    if os.path.exists(MOVE_LOG_CSV):
        try:
            old_df = pd.read_csv(MOVE_LOG_CSV)
        except Exception:
            old_df = pd.DataFrame()
        log_df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        log_df = new_df

    # 캐시 무효화 후 저장
    load_move_log.clear()
    log_df.to_csv(MOVE_LOG_CSV, index=False, encoding="utf-8-sig")


# ==============================
# stock.xlsx 관련 유틸
# ==============================
@st.cache_data(show_spinner=False)
def load_stock() -> pd.DataFrame:
    if not os.path.exists(STOCK_FILE):
        return pd.DataFrame()

    try:
        df = pd.read_excel(STOCK_FILE)
    except Exception as e:
        st.error(f"stock.xlsx 파일을 읽는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

    return df


def map_warehouse_category(code: str) -> str:
    if not isinstance(code, str):
        return "외주"

    c = code.strip().upper()

    if c in {"WC301", "WC501", "WC502", "WC503", "WC504"}:
        return "자사"

    if c in {"WH001", "WH102", "WH201", "WH701", "WH301", "WH601", "WH401", "WH506"}:
        return "창고"

    if c in {"WH202", "WH302"}:
        return "불량"

    return "외주"


def get_stock_summary(item_code: str, lot: str):
    df = load_stock()
    if df.empty:
        return None, None

    required_cols = ["창고/작업장", "창고/작업장명", "품번", "로트번호", "실재고수량"]
    for c in required_cols:
        if c not in df.columns:
            return None, None

    sub = df[
        (df["품번"].astype(str) == str(item_code))
        & (df["로트번호"].astype(str) == str(lot))
    ].copy()

    if sub.empty:
        return None, None

    sub["실재고수량"] = pd.to_numeric(sub["실재고수량"], errors="coerce").fillna(0.0)
    sub = sub[sub["실재고수량"] > 0]
    if sub.empty:
        return None, None

    sub["대분류"] = sub["창고/작업장"].apply(map_warehouse_category)

    grp = (
        sub.groupby(["대분류", "창고/작업장", "창고/작업장명"], as_index=False)["실재고수량"]
        .sum()
    )
    grp = grp.sort_values("실재고수량", ascending=False)

    grp = grp.rename(
        columns={
            "창고/작업장": "창고코드",
            "창고/작업장명": "창고명",
            "실재고수량": "총용량_kg",
        }
    )

    parts = []
    for _, r in grp.iterrows():
        parts.append(f"{r['대분류']}({r['창고명']} {r['창고코드']}): {int(r['총용량_kg'])}kg")
    summary_text = ", ".join(parts)

    return grp, summary_text


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
# 탭 1: 이동
# ==============================
def clear_move_inputs():
    """조회/초기화 버튼 옆에서 사용할 입력값 초기화 콜백."""
    ss = st.session_state
    ss["mv_barcode"] = ""
    ss["mv_lot"] = ""
    ss["mv_scanned_barcode"] = ""


def render_tab_move():
    st.markdown("### 📦 벌크 이동 (CSV 직접 수정)")

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

    # 상단 레이아웃: 4칼럼 (1: 입력칸, 2: 바코드 스캔, 3/4: 여유공간)
    col1, col2, col3, col4 = st.columns([2.5, 1.2, 0.8, 0.5])

    # 1번 칼럼: 작업번호/입하번호 + 로트번호 + 조회/초기화 버튼
    with col1:
        barcode = st.text_input(
            barcode_label,
            key="mv_barcode",
            placeholder="예: W24012345",
        )
        lot_input = st.text_input(
            "로트번호를 입력해 주세요.",
            key="mv_lot",
            placeholder="예: 2E075K",
        )

        # 버튼 두 개가 들어갈 영역을 넓게 확보
        btn_col1, btn_sp, btn_col2 = st.columns([1, 0.2, 1])
        with btn_col1:
            search_clicked = st.button("조회하기", key="mv_search_btn_csv")
        with btn_col2:
            st.button("초기화", key="mv_clear_btn", on_click=clear_move_inputs)

    # 2번 칼럼: 바코드 스캔 영역
    with col2:
        st.caption("또는 라벨 사진을 업로드해 바코드를 인식할 수 있습니다.")
        scan_file = st.file_uploader(
            "바코드 라벨 사진 업로드 (선택)",
            type=["png", "jpg", "jpeg"],
            key="mv_barcode_image",
        )
        if scan_file is not None:
            if Image is None or CaptureVisionRouter is None or LicenseManager is None:
                st.error("바코드 인식에 필요한 라이브러리가 설치되어 있지 않습니다.")
            else:
                try:
                    img = Image.open(io.BytesIO(scan_file.read()))
                    st.image(img, caption=scan_file.name, width=260)
                    codes = dbr_decode(img)
                    if codes:
                        _, text_code = codes[0]
                        text_code = (text_code or "").strip()
                        ss["mv_scanned_barcode"] = text_code
                        st.success(f"바코드 인식 결과: {text_code}")
                    else:
                        st.warning("바코드를 인식하지 못했습니다.")
                except Exception as e:
                    st.error(f"이미지를 처리하는 중 오류가 발생했습니다: {e}")

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

        # 현재 위치 + 상세보기/이동이력 버튼
        loc_col1, loc_col2 = st.columns([3, 2])
        with loc_col1:
            st.markdown(f"**현재 위치(전산 기준):** {stock_loc_display}")
        with loc_col2:
            b1_col, b_sp, b2_col = st.columns([1, 0.2, 1])
            with b1_col:
                if stock_summary_df is not None and not stock_summary_df.empty:
                    if st.button("상세보기", key=f"stock_detail_btn_{lot}"):
                        ss["mv_show_stock_detail"] = not ss.get("mv_show_stock_detail", False)
            with b2_col:
                if st.button("이동이력", key=f"move_hist_btn_{lot}"):
                    ss["mv_show_move_history_here"] = not ss.get("mv_show_move_history_here", False)

        if ss.get("mv_show_stock_detail", False) and stock_summary_df is not None:
            st.dataframe(stock_summary_df, use_container_width=True, height=240)

        st.markdown("### 🛢 통 선택 및 잔량 입력")

        selected_drums = []
        drum_new_qty = {}

        drum_list = lot_df["통번호"].tolist()

        # 모두 선택 / 모두 해제  → 버튼 칼럼 폭을 넉넉하게 확보
        c1, c_sp, c2 = st.columns([1, 0.2, 1])
        with c1:
            if st.button("모두 선택", key=f"mv_select_all_{lot}"):
                for dn in drum_list:
                    st.session_state[f"mv_sel_{lot}_{dn}"] = True
        with c2:
            if st.button("모두 해제", key=f"mv_select_none_{lot}"):
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
                "현재 위치(CSV 기준)",
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
    st.markdown("### 🔍 벌크 조회 (CSV 기준)")

    df = load_drums()
    if df.empty:
        st.info("CSV에 등록된 벌크 정보가 없습니다.")
        return

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

    st.markdown("#### 📄 행별 상세 (bulk_drums_extended와 동일 구조)")
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

    # 요약 테이블 높이를 행 개수에 자동 맞춤
    row_height = 35
    header_height = 40
    dynamic_height = header_height + row_height * (len(summary) + 1)

    st.dataframe(
        summary,
        width=340,
        height=dynamic_height,
    )


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
# 탭 4: 이동 이력
# ==============================
def render_tab_move_log():
    st.markdown("### 📜 이동 이력")

    df = load_move_log()
    if df.empty:
        st.info("이동 이력이 없습니다.")
        return

    ss = st.session_state
    ss.setdefault("log_lot_filter", "")
    ss.setdefault("log_page", 1)

    col1, col2 = st.columns([3, 1])
    with col1:
        lot_filter = st.text_input(
            "로트번호로 검색 (부분 일치)",
            key="log_lot_filter",
            placeholder="예: 2E075K",
        )
    with col2:
        if st.button("검색 초기화", key="log_reset"):
            ss["log_lot_filter"] = ""
            ss["log_page"] = 1
            lot_filter = ""

    if lot_filter:
        mask = df["로트번호"].astype(str).str.contains(lot_filter.strip(), na=False)
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

    cols_order = [
        "시간",
        "ID",       # 작성자
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

    st.dataframe(page_df, use_container_width=True)


# ==============================
# 메인
# ==============================
def main():
    ss = st.session_state

    # 로그인 안 되어 있으면 로그인 화면만 표시
    if "user_id" not in ss or "user_name" not in ss:
        render_login()
        return

    # 사이드바: 사용자 정보 + 로그아웃
    with st.sidebar:
        st.markdown(f"**사용자:** {ss['user_name']} ({ss['user_id']})")
        if st.button("로그아웃", key="logout_btn"):
            for k in ["user_id", "user_name"]:
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()

    st.title("🏭 벌크 관리 시스템")

    tab_move, tab_lookup, tab_map, tab_log = st.tabs(
        ["📦 이동(CSV)", "🔍 조회(CSV)", "🗺 지도(CSV)", "📜 이동 이력"]
    )

    with tab_move:
        render_tab_move()
    with tab_lookup:
        render_tab_lookup()
    with tab_map:
        render_tab_map()
    with tab_log:
        render_tab_move_log()


if __name__ == "__main__":
    main()
