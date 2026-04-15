# # -*- coding: utf-8 -*-
# """
# 주말(BTC) 수익률 패널 생성기 (2020-01-01 ~ 오늘)
# - 주말 창: KST 금 16:00 → KST 월 09:00 (= UTC 금 07:00 → 월 00:00)
# - 데이터 출처
#   * 거시/시장: FRED API (환경변수 FRED_API_KEY 필요)
#   * 시간봉 시세: Coinbase Exchange 공개 REST (무인증)
# 출력: weekend_coin.xlsx

# 업데이트:
# - (fix) 실업률/분기 GDP 열 누락 보완: unemployment_rate_pct, gdp_qoq_saar_pct 추가
# - (fix) CPI/PCE YoY 2020년 값 계산을 위해 거시 데이터 수집 시작일을 2019-01-01로 분리
# - CPI/PCE는 '발표 주'에만 값 기입(그 외 NaN)
#   · CPI: 다음 달 둘째 금요일
#   · PCE: 다음 달 마지막 금요일
# - drop 시 존재하지 않는 열은 무시(errors="ignore")
# - 발표 주 매핑 시 날짜를 UTC tz-aware로 맞춤
# """

# import os
# import time
# from datetime import datetime, timedelta, timezone

# import numpy as np
# import pandas as pd
# import pytz
# import requests
# from pandas import DatetimeTZDtype
# # tqdm은 선택적 의존성입니다. 설치되어 있지 않으면 무소음 진행 바로 대체합니다.
# try:
#     from tqdm import tqdm
# except Exception:
#     def tqdm(iterable, **kwargs):
#         return iterable

# # ---------------------
# # 설정
# # ---------------------
# # 거시지표는 2019년부터 불러와 2020년의 YoY 계산이 가능하도록 함
# MACRO_START_DATE = "2019-01-01"
# # 주말 수익률 분석은 2020-01-01부터
# WEEKEND_START_DATE = "2020-01-01"

# OUTPUT_XLSX = "weekend_coin.xlsx"

# UTC = timezone.utc
# KST = pytz.timezone("Asia/Seoul")

# FRED_API_KEY = os.getenv("FRED_API_KEY")
# FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# # FRED 시리즈(공식 ID)
# FRED_SERIES = {
#     "SP500": "D",                # S&P 500 (일)
#     "DGS10": "D",                # 10Y 국채수익률(%)
#     "DFEDTARU": "D",             # 연준 기준금리 상단(%)
#     "CPIAUCSL": "M",             # CPI 지수(월)
#     "PCEPI": "M",                # PCE 물가지수(월)
#     "UNRATE": "M",               # 실업률(%)
#     "A191RL1Q225SBEA": "Q",      # 실질 GDP QoQ SAAR(%)
#     "DCOILWTICO": "D",           # WTI ($/bbl)
# }

# # ---------------------
# # 유틸
# # ---------------------

# def require_fred_key():
#     if not FRED_API_KEY:
#         raise RuntimeError(
#             "환경변수 FRED_API_KEY가 설정되지 않았습니다. "
#             "FRED에서 API Key 발급 후 FRED_API_KEY로 등록하세요."
#         )


# def _tz_to_utc(s: pd.Series) -> pd.Series:
#     """인덱스가 tz-naive면 UTC로 localize, tz-aware면 UTC로 convert"""
#     return s.tz_localize("UTC") if s.index.tz is None else s.tz_convert("UTC")


# def get_fred_series(series_id: str, start: str) -> pd.DataFrame:
#     """FRED 단일 시리즈를 DataFrame(index=date, col=series_id)으로 반환"""
#     require_fred_key()
#     params = {
#         "api_key": FRED_API_KEY,
#         "series_id": series_id,
#         "file_type": "json",
#         "observation_start": start,
#     }
#     r = requests.get(FRED_BASE, params=params, timeout=30)
#     r.raise_for_status()
#     js = r.json()
#     obs = js.get("observations", [])
#     df = pd.DataFrame(obs)[["date", "value"]]
#     df["date"] = pd.to_datetime(df["date"])
#     df["value"] = pd.to_numeric(df["value"], errors="coerce")
#     df = df.set_index("date").sort_index()
#     return df.rename(columns={"value": series_id})


# def load_fred_panel(start: str) -> pd.DataFrame:
#     """필요한 모든 FRED 시리즈 결합 (start 인자 사용)"""
#     frames = []
#     for sid in FRED_SERIES.keys():
#         frames.append(get_fred_series(sid, start))
#         time.sleep(0.2)  # API courtesy
#     panel = pd.concat(frames, axis=1)
#     return panel

# # ---------- 발표 주 계산 헬퍼 ----------

# def _second_friday(dt: pd.Timestamp) -> pd.Timestamp:
#     """dt가 속한 달의 둘째 금요일 날짜(자정, tz-naive)"""
#     month_start = dt.replace(day=1)
#     month_end = (month_start + pd.offsets.MonthEnd(0))
#     fridays = pd.date_range(month_start, month_end, freq="W-FRI")
#     return pd.Timestamp(fridays[1]).normalize()


# def _last_friday(dt: pd.Timestamp) -> pd.Timestamp:
#     """dt가 속한 달의 마지막 금요일 날짜(자정, tz-naive)"""
#     month_start = dt.replace(day=1)
#     month_end = (month_start + pd.offsets.MonthEnd(0))
#     fridays = pd.date_range(month_start, month_end, freq="W-FRI")
#     return pd.Timestamp(fridays[-1]).normalize()


# def _map_monthly_to_release_weeks(
#     monthly_yoy: pd.Series,
#     wk_index_utc: pd.DatetimeIndex,
#     rule: str,
# ) -> pd.Series:
#     """
#     월간 YoY 시리즈(인덱스=그 달 1일)를 주간(W-FRI, tz=UTC) 인덱스에 매핑.
#     rule:
#       - "CPI": 해당 달의 '다음 달' 둘째 금요일 주
#       - "PCE": 해당 달의 '다음 달' 마지막 금요일 주
#     나머지 주는 NaN.
#     """
#     out = pd.Series(index=wk_index_utc, dtype="float64")

#     for mdate, val in monthly_yoy.dropna().items():
#         next_month = (pd.Timestamp(mdate) + pd.offsets.MonthBegin(1))
#         if rule == "CPI":
#             release_naive = _second_friday(next_month)
#         elif rule == "PCE":
#             release_naive = _last_friday(next_month)
#         else:
#             continue
#         release_utc = release_naive.tz_localize("UTC")
#         if release_utc in out.index:
#             out.loc[release_utc] = float(val)

#     return out


# def make_weekly_features(panel: pd.DataFrame) -> pd.DataFrame:
#     """주간(금요일, UTC) 기준 피처 구성"""
#     end_dt = pd.Timestamp.now(tz="UTC").normalize()
#     start_dt = pd.Timestamp(WEEKEND_START_DATE).tz_localize("UTC")
#     wk_index = pd.date_range(start=start_dt, end=end_dt, freq="W-FRI")
#     weekly = pd.DataFrame(index=wk_index)

#     # 일간 시리즈 → 주간 금요일 수준
#     for sid in ["SP500", "DGS10", "DFEDTARU", "DCOILWTICO"]:
#         s = panel[sid].dropna()
#         s = _tz_to_utc(s)
#         weekly[sid] = s.resample("W-FRI").last()

#     # 월/분기 레벨(UNRATE, GDP)은 주간으로 forward-fill 유지
#     for sid in ["UNRATE", "A191RL1Q225SBEA"]:
#         s = panel[sid].dropna()
#         s = _tz_to_utc(s)
#         weekly[sid] = s.reindex(weekly.index, method="ffill")

#     # ---- CPI/PCE: 발표 주에만 값 기입 (그 외 NaN) ----
#     cpi_month = panel["CPIAUCSL"].dropna()
#     pce_month = panel["PCEPI"].dropna()
#     cpi_yoy_m = cpi_month.pct_change(12) * 100.0
#     pce_yoy_m = pce_month.pct_change(12) * 100.0

#     cpi_week = _map_monthly_to_release_weeks(cpi_yoy_m, weekly.index, rule="CPI")
#     pce_week = _map_monthly_to_release_weeks(pce_yoy_m, weekly.index, rule="PCE")

#     weekly["cpi_yoy_pct"] = cpi_week        # 발표 주만 값, 나머지 NaN
#     weekly["pce_yoy_pct"] = pce_week        # 발표 주만 값, 나머지 NaN

#     # 파생 지표
#     weekly["sp500_close"] = weekly["SP500"]
#     weekly["sp500_weekly_ret_pct"] = weekly["sp500_close"].pct_change() * 100.0
#     weekly["sp500_updown"] = np.where(weekly["sp500_weekly_ret_pct"] >= 0, "UP", "DOWN")

#     weekly["us10y_pct"] = weekly["DGS10"]
#     weekly["dgs10_weekly_change_bp"] = weekly["us10y_pct"].diff() * 100.0

#     weekly["fed_target_upper_pct"] = weekly["DFEDTARU"]
#     weekly["fed_change_bp"] = weekly["fed_target_upper_pct"].diff() * 100.0

#     weekly["wti_usd"] = weekly["DCOILWTICO"]
#     weekly["wti_weekly_ret_pct"] = weekly["wti_usd"].pct_change() * 100.0

#     # (fix) 실업률/분기 GDP 열 노출
#     weekly["unemployment_rate_pct"] = weekly["UNRATE"]
#     weekly["gdp_qoq_saar_pct"] = weekly["A191RL1Q225SBEA"]

#     # 원시열 정리 (존재하는 열만 삭제)
#     weekly = weekly.drop(
#         columns=[
#             "SP500",
#             "DGS10",
#             "DFEDTARU",
#             "DCOILWTICO",
#             "CPIAUCSL",
#             "PCEPI",
#             # 원시 지표 컬럼 숨김(필드 노출은 파생 열 사용)
#             "UNRATE",
#             "A191RL1Q225SBEA",
#         ],
#         errors="ignore",
#     )

#     weekly.index.name = "week_friday_utc"
#     return weekly.reset_index()

# # ---------------------
# # Coinbase (무인증) 시간봉
# # ---------------------

# def coinbase_candles(
#     product_id: str,
#     start: pd.Timestamp,
#     end: pd.Timestamp,
#     granularity: int = 3600,
#     max_retries: int = 3,
# ) -> pd.DataFrame:
#     """
#     /products/{product_id}/candles → [time, low, high, open, close, volume]
#     granularity: 60/300/900/3600/21600/86400
#     """
#     url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
#     params = {
#         "start": start.isoformat().replace("+00:00", "Z"),
#         "end": (end + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),  # 끝 봉 포함 보정
#         "granularity": granularity,
#     }
#     for i in range(max_retries):
#         r = requests.get(url, params=params, timeout=30)
#         if r.status_code == 429:
#             time.sleep(1.0 * (i + 1))
#             continue
#         r.raise_for_status()
#         data = r.json()
#         if not isinstance(data, list):
#             raise ValueError(f"Unexpected response: {data}")
#         df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
#         df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
#         df = df.sort_values("time").set_index("time")
#         return df
#     raise RuntimeError("Coinbase API rate limited / failed.")


# def generate_weekend_intervals(
#     start_date: str,
#     end_dt_utc: pd.Timestamp,
# ) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
#     """
#     완료된 주말 구간 리스트(UTC)
#     월요일 00:00 UTC들을 end로 사용 → start = end - 2일 17시간(= 금 07:00 UTC)
#     """
#     start_dt = pd.to_datetime(start_date).tz_localize("UTC")
#     first_monday = (start_dt + pd.offsets.Week(weekday=0)).replace(hour=0, minute=0, second=0, microsecond=0)
#     mondays = pd.date_range(first_monday, end_dt_utc, freq="W-MON")

#     intervals = []
#     for mend in mondays:
#         wstart = mend - timedelta(days=2, hours=17)  # Fri 07:00 UTC
#         if wstart < start_dt:
#             continue
#         intervals.append((wstart, mend))
#     return intervals


# def compute_coin_weekend_returns(
#     product_id: str,
#     intervals: list[tuple[pd.Timestamp, pd.Timestamp]],
# ) -> pd.DataFrame:
#     """코인(예: BTC-USD) 주말 수익률 계산"""
#     rows = []
#     for start, end in tqdm(intervals, desc=f"{product_id} weekend"):
#         df = coinbase_candles(product_id, start=start, end=end, granularity=3600)
#         start_idx = df.index.searchsorted(start)
#         end_idx = df.index.searchsorted(end)
#         if start_idx >= len(df.index) or end_idx >= len(df.index):
#             continue
#         start_ts = df.index[start_idx]
#         end_ts = df.index[end_idx]
#         start_px = float(df.loc[start_ts, "open"])
#         end_px = float(df.loc[end_ts, "open"])
#         ret_pct = (end_px / start_px - 1.0) * 100.0
#         rows.append(
#             {
#                 "weekend_start_utc": start_ts,
#                 "weekend_end_utc": end_ts,
#                 "btc_start_price": start_px,
#                 "btc_end_price": end_px,
#                 "btc_weekend_return_pct": ret_pct,
#             }
#         )
#         time.sleep(0.15)
#     btc = pd.DataFrame(rows)
#     if btc.empty:
#         raise RuntimeError("BTC 데이터가 비어있습니다. 네트워크/엔드포인트 확인 필요.")
#     btc["weekend_start_kst"] = btc["weekend_start_utc"].dt.tz_convert("Asia/Seoul")
#     btc["weekend_end_kst"] = btc["weekend_end_utc"].dt.tz_convert("Asia/Seoul")
#     # 라벨용 금요일(UTC)
#     btc["week_friday_utc"] = (btc["weekend_end_utc"] - pd.Timedelta(days=3)).dt.normalize()
#     return btc

# # ---------------------
# # 엑셀 저장용: tz 제거 헬퍼
# # ---------------------

# def strip_tz_for_excel(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     엑셀은 tz-aware datetime을 지원하지 않음 → 모든 datetime 컬럼의 tz 제거
#     - *_kst 는 Asia/Seoul 시각으로 변환 후 tz 제거
#     - 그 외는 UTC로 변환 후 tz 제거
#     """
#     out = df.copy()
#     for col in out.columns:
#         if isinstance(out[col].dtype, DatetimeTZDtype):
#             if "kst" in col.lower():
#                 out[col] = out[col].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
#             else:
#                 out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
#     return out

# # ---------------------
# # 메인 빌드 & 저장
# # ---------------------

# def build_and_save():
#     # 1) 거시/시장 패널(주간) — 2019부터 로딩해 2020년 YoY 계산 가능
#     fred_panel = load_fred_panel(MACRO_START_DATE)
#     weekly_feats = make_weekly_features(fred_panel)

#     # 2) 완료된 주말 구간 — 2020부터 분석
#     now_utc = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
#     intervals = generate_weekend_intervals(WEEKEND_START_DATE, now_utc)

#     # 3) BTC 주말 수익률
#     btc_df = compute_coin_weekend_returns("BTC-USD", intervals)

#     # 4) 병합 (키: week_friday_utc)
#     data = pd.merge(btc_df, weekly_feats, on="week_friday_utc", how="left")

#     # 5) O/X 플래그 추가
#     data["fed_change_ox"] = np.where(data["fed_change_bp"].fillna(0).round(6) != 0, "O", "X")
#     data["btc_up_ox"] = np.where((data["btc_weekend_return_pct"].fillna(0) > 0), "O", "X")

#     # 6) 컬럼 정렬
#     ordered = [
#         "weekend_start_utc",
#         "weekend_end_utc",
#         "weekend_start_kst",
#         "weekend_end_kst",
#         "btc_start_price",
#         "btc_end_price",
#         "btc_weekend_return_pct",
#         "btc_up_ox",
#         "week_friday_utc",
#         "sp500_close",
#         "sp500_weekly_ret_pct",
#         "sp500_updown",
#         "fed_target_upper_pct",
#         "fed_change_bp",
#         "fed_change_ox",
#         "us10y_pct",
#         "dgs10_weekly_change_bp",
#         "wti_usd",
#         "wti_weekly_ret_pct",
#         "cpi_yoy_pct",
#         "pce_yoy_pct",
#         "unemployment_rate_pct",
#         "gdp_qoq_saar_pct",
#     ]
#     # 존재하는 열만 선택
#     data = data[[c for c in ordered if c in data.columns]].sort_values("weekend_end_utc").reset_index(drop=True)

#     # 7) 메타시트
#     meta = pd.DataFrame(
#         {
#             "field": [c for c in ordered if c in data.columns],
#             "description_ko": [
#                 "주말 시작(UTC)",
#                 "주말 종료(UTC)",
#                 "주말 시작(KST)",
#                 "주말 종료(KST)",
#                 "BTC-USD 시작가(주말창)",
#                 "BTC-USD 종료가(주말창)",
#                 "BTC 주말 수익률(%)",
#                 "BTC 주말 상승 여부(O/X)",
#                 "라벨용 금요일(UTC)",
#                 "S&P500 종가(금요일)",
#                 "S&P500 주간 수익률(%)",
#                 "S&P500 상승/하락",
#                 "연준 기준금리 상단(%)",
#                 "기준금리 주간 변화(bp)",
#                 "해당 주 기준금리 변동 여부(O/X)",
#                 "미10년물 금리(%)",
#                 "미10년물 주간 변화(bp)",
#                 "WTI 가격($/bbl)",
#                 "WTI 주간 수익률(%)",
#                 "CPI 전년동월비(%, 발표 주만)",
#                 "PCE 전년동월비(%, 발표 주만)",
#                 "실업률(%)",
#                 "실질GDP QoQ SAAR(%)",
#             ][: len([c for c in ordered if c in data.columns])],
#         }
#     )

#     # 8) 엑셀 저장 (tz 제거 후 기록)
# # tz 변환이 혹시 실패해도 data_x는 항상 정의되도록 방어
# try:
#     data_x = strip_tz_for_excel(data)
# except Exception as e:
#     print("[경고] tz 변환 중 오류 발생 — 원본으로 저장합니다:", e)
#     data_x = data.copy()
#     # 남아있는 tz-aware 컬럼이 있으면 수동으로 tz 제거
#     for col in data_x.columns:
#         if isinstance(data_x[col].dtype, DatetimeTZDtype):
#             if "kst" in col.lower():
#                 data_x[col] = data_x[col].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
#             else:
#                 data_x[col] = data_x[col].dt.tz_convert("UTC").dt.tz_localize(None)

# # 엑셀 저장 엔진 자동 선택 (openpyxl → xlsxwriter → CSV 폴백)
# try:
#     import openpyxl  # noqa: F401
#     _engine = "openpyxl"
# except Exception:
#     try:
#         import xlsxwriter  # noqa: F401
#         _engine = "xlsxwriter"
#     except Exception:
#         _engine = None

# if _engine is not None:
#     with pd.ExcelWriter(OUTPUT_XLSX, engine=_engine) as xw:
#         data_x.to_excel(xw, index=False, sheet_name="data")
#         meta.to_excel(xw, index=False, sheet_name="meta")
# else:
#     # 여기는 엔진이 전혀 없을 때 — CSV로 저장
#     dx = data_x  # 가독성
#     dx.to_csv("weekend_coin_data.csv", index=False)
#     meta.to_csv("weekend_coin_meta.csv", index=False)
#     print("[경고] openpyxl/xlsxwriter 미설치로 CSV로 저장했습니다. pip install openpyxl 권장")

# print(f"[완료] {OUTPUT_XLSX} 저장(또는 CSV 폴백)")
# print(data_x.tail(5))


# if __name__ == "__main__":
#     build_and_save()


# -*- coding: utf-8 -*-
"""
주말(BTC) 수익률 패널 생성기 (2020-01-01 ~ 오늘)
- 주말 창: KST 금 16:00 → KST 월 09:00 (= UTC 금 07:00 → 월 00:00)
- 데이터 출처
  * 거시/시장: FRED API (환경변수 FRED_API_KEY 필요)
  * 시간봉 시세: Coinbase Exchange 공개 REST (무인증)
출력: weekend_coin.xlsx

업데이트:
- (fix) 실업률/분기 GDP 열 누락 보완: unemployment_rate_pct, gdp_qoq_saar_pct 추가
- (fix) CPI/PCE YoY 2020년 값 계산을 위해 거시 데이터 수집 시작일을 2019-01-01로 분리
- CPI/PCE는 '발표 주'에만 값 기입(그 외 NaN)
  · CPI: 다음 달 둘째 금요일
  · PCE: 다음 달 마지막 금요일
- drop 시 존재하지 않는 열은 무시(errors="ignore")
- 발표 주 매핑 시 날짜를 UTC tz-aware로 맞춤
"""

import os
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytz
import requests
from pandas import DatetimeTZDtype
# tqdm은 선택적 의존성입니다. 설치되어 있지 않으면 무소음 진행 바로 대체합니다.
try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

# ---------------------
# 설정
# ---------------------
# 거시지표는 2019년부터 불러와 2020년의 YoY 계산이 가능하도록 함
MACRO_START_DATE = "2019-01-01"
# 주말 수익률 분석은 2020-01-01부터
WEEKEND_START_DATE = "2020-01-01"

OUTPUT_XLSX = "weekend_coin.xlsx"

UTC = timezone.utc
KST = pytz.timezone("Asia/Seoul")

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED 시리즈(공식 ID)
FRED_SERIES = {
    "SP500": "D",                # S&P 500 (일)
    "DGS10": "D",                # 10Y 국채수익률(%)
    "DFEDTARU": "D",             # 연준 기준금리 상단(%)
    "CPIAUCSL": "M",             # CPI 지수(월)
    "PCEPI": "M",                # PCE 물가지수(월)
    "UNRATE": "M",               # 실업률(%)
    "A191RL1Q225SBEA": "Q",      # 실질 GDP QoQ SAAR(%)
    "DCOILWTICO": "D",           # WTI ($/bbl)
}

# ---------------------
# 유틸
# ---------------------

def require_fred_key():
    if not FRED_API_KEY:
        raise RuntimeError(
            "환경변수 FRED_API_KEY가 설정되지 않았습니다. "
            "FRED에서 API Key 발급 후 FRED_API_KEY로 등록하세요."
        )


def _tz_to_utc(s: pd.Series) -> pd.Series:
    """인덱스가 tz-naive면 UTC로 localize, tz-aware면 UTC로 convert"""
    return s.tz_localize("UTC") if s.index.tz is None else s.tz_convert("UTC")


def get_fred_series(series_id: str, start: str) -> pd.DataFrame:
    """FRED 단일 시리즈를 DataFrame(index=date, col=series_id)으로 반환"""
    require_fred_key()
    params = {
        "api_key": FRED_API_KEY,
        "series_id": series_id,
        "file_type": "json",
        "observation_start": start,
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    js = r.json()
    obs = js.get("observations", [])
    df = pd.DataFrame(obs)[["date", "value"]]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.set_index("date").sort_index()
    return df.rename(columns={"value": series_id})


def load_fred_panel(start: str) -> pd.DataFrame:
    """필요한 모든 FRED 시리즈 결합 (start 인자 사용)"""
    frames = []
    for sid in FRED_SERIES.keys():
        frames.append(get_fred_series(sid, start))
        time.sleep(0.2)  # API courtesy
    panel = pd.concat(frames, axis=1)
    return panel

# ---------- 발표 주 계산 헬퍼 ----------

def _second_friday(dt: pd.Timestamp) -> pd.Timestamp:
    """dt가 속한 달의 둘째 금요일 날짜(자정, tz-naive)"""
    month_start = dt.replace(day=1)
    month_end = (month_start + pd.offsets.MonthEnd(0))
    fridays = pd.date_range(month_start, month_end, freq="W-FRI")
    return pd.Timestamp(fridays[1]).normalize()


def _last_friday(dt: pd.Timestamp) -> pd.Timestamp:
    """dt가 속한 달의 마지막 금요일 날짜(자정, tz-naive)"""
    month_start = dt.replace(day=1)
    month_end = (month_start + pd.offsets.MonthEnd(0))
    fridays = pd.date_range(month_start, month_end, freq="W-FRI")
    return pd.Timestamp(fridays[-1]).normalize()


def _map_monthly_to_release_weeks(
    monthly_yoy: pd.Series,
    wk_index_utc: pd.DatetimeIndex,
    rule: str,
) -> pd.Series:
    """
    월간 YoY 시리즈(인덱스=그 달 1일)를 주간(W-FRI, tz=UTC) 인덱스에 매핑.
    rule:
      - "CPI": 해당 달의 '다음 달' 둘째 금요일 주
      - "PCE": 해당 달의 '다음 달' 마지막 금요일 주
    나머지 주는 NaN.
    """
    out = pd.Series(index=wk_index_utc, dtype="float64")

    for mdate, val in monthly_yoy.dropna().items():
        next_month = (pd.Timestamp(mdate) + pd.offsets.MonthBegin(1))
        if rule == "CPI":
            release_naive = _second_friday(next_month)
        elif rule == "PCE":
            release_naive = _last_friday(next_month)
        else:
            continue
        release_utc = release_naive.tz_localize("UTC")
        if release_utc in out.index:
            out.loc[release_utc] = float(val)

    return out


def make_weekly_features(panel: pd.DataFrame) -> pd.DataFrame:
    """주간(금요일, UTC) 기준 피처 구성"""
    end_dt = pd.Timestamp.now(tz="UTC").normalize()
    start_dt = pd.Timestamp(WEEKEND_START_DATE).tz_localize("UTC")
    wk_index = pd.date_range(start=start_dt, end=end_dt, freq="W-FRI")
    weekly = pd.DataFrame(index=wk_index)

    # 일간 시리즈 → 주간 금요일 수준
    for sid in ["SP500", "DGS10", "DFEDTARU", "DCOILWTICO"]:
        s = panel[sid].dropna()
        s = _tz_to_utc(s)
        weekly[sid] = s.resample("W-FRI").last()

    # 월/분기 레벨(UNRATE, GDP)은 주간으로 forward-fill 유지
    for sid in ["UNRATE", "A191RL1Q225SBEA"]:
        s = panel[sid].dropna()
        s = _tz_to_utc(s)
        weekly[sid] = s.reindex(weekly.index, method="ffill")

    # ---- CPI/PCE: 발표 주에만 값 기입 (그 외 NaN) ----
    cpi_month = panel["CPIAUCSL"].dropna()
    pce_month = panel["PCEPI"].dropna()
    cpi_yoy_m = cpi_month.pct_change(12) * 100.0
    pce_yoy_m = pce_month.pct_change(12) * 100.0

    cpi_week = _map_monthly_to_release_weeks(cpi_yoy_m, weekly.index, rule="CPI")
    pce_week = _map_monthly_to_release_weeks(pce_yoy_m, weekly.index, rule="PCE")

    weekly["cpi_yoy_pct"] = cpi_week        # 발표 주만 값, 나머지 NaN
    weekly["pce_yoy_pct"] = pce_week        # 발표 주만 값, 나머지 NaN

    # 파생 지표
    weekly["sp500_close"] = weekly["SP500"]
    weekly["sp500_weekly_ret_pct"] = weekly["sp500_close"].pct_change() * 100.0
    weekly["sp500_updown"] = np.where(weekly["sp500_weekly_ret_pct"] >= 0, "UP", "DOWN")

    weekly["us10y_pct"] = weekly["DGS10"]
    weekly["dgs10_weekly_change_bp"] = weekly["us10y_pct"].diff() * 100.0

    weekly["fed_target_upper_pct"] = weekly["DFEDTARU"]
    weekly["fed_change_bp"] = weekly["fed_target_upper_pct"].diff() * 100.0

    weekly["wti_usd"] = weekly["DCOILWTICO"]
    weekly["wti_weekly_ret_pct"] = weekly["wti_usd"].pct_change() * 100.0

    # (fix) 실업률/분기 GDP 열 노출
    weekly["unemployment_rate_pct"] = weekly["UNRATE"]
    weekly["gdp_qoq_saar_pct"] = weekly["A191RL1Q225SBEA"]

    # 원시열 정리 (존재하는 열만 삭제)
    weekly = weekly.drop(
        columns=[
            "SP500",
            "DGS10",
            "DFEDTARU",
            "DCOILWTICO",
            "CPIAUCSL",
            "PCEPI",
            # 원시 지표 컬럼 숨김(필드 노출은 파생 열 사용)
            "UNRATE",
            "A191RL1Q225SBEA",
        ],
        errors="ignore",
    )

    weekly.index.name = "week_friday_utc"
    return weekly.reset_index()

# ---------------------
# Coinbase (무인증) 시간봉
# ---------------------

def coinbase_candles(
    product_id: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    granularity: int = 3600,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    /products/{product_id}/candles → [time, low, high, open, close, volume]
    granularity: 60/300/900/3600/21600/86400
    """
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    params = {
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": (end + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),  # 끝 봉 포함 보정
        "granularity": granularity,
    }
    for i in range(max_retries):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(1.0 * (i + 1))
            continue
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            raise ValueError(f"Unexpected response: {data}")
        df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.sort_values("time").set_index("time")
        return df
    raise RuntimeError("Coinbase API rate limited / failed.")


def generate_weekend_intervals(
    start_date: str,
    end_dt_utc: pd.Timestamp,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """
    완료된 주말 구간 리스트(UTC)
    월요일 00:00 UTC들을 end로 사용 → start = end - 2일 17시간(= 금 07:00 UTC)
    """
    start_dt = pd.to_datetime(start_date).tz_localize("UTC")
    first_monday = (start_dt + pd.offsets.Week(weekday=0)).replace(hour=0, minute=0, second=0, microsecond=0)
    mondays = pd.date_range(first_monday, end_dt_utc, freq="W-MON")

    intervals = []
    for mend in mondays:
        wstart = mend - timedelta(days=2, hours=17)  # Fri 07:00 UTC
        if wstart < start_dt:
            continue
        intervals.append((wstart, mend))
    return intervals


def compute_coin_weekend_returns(
    product_id: str,
    intervals: list[tuple[pd.Timestamp, pd.Timestamp]],
) -> pd.DataFrame:
    """코인(예: BTC-USD) 주말 수익률 계산"""
    rows = []
    for start, end in tqdm(intervals, desc=f"{product_id} weekend"):
        df = coinbase_candles(product_id, start=start, end=end, granularity=3600)
        start_idx = df.index.searchsorted(start)
        end_idx = df.index.searchsorted(end)
        if start_idx >= len(df.index) or end_idx >= len(df.index):
            continue
        start_ts = df.index[start_idx]
        end_ts = df.index[end_idx]
        start_px = float(df.loc[start_ts, "open"])
        end_px = float(df.loc[end_ts, "open"])
        ret_pct = (end_px / start_px - 1.0) * 100.0
        rows.append(
            {
                "weekend_start_utc": start_ts,
                "weekend_end_utc": end_ts,
                "btc_start_price": start_px,
                "btc_end_price": end_px,
                "btc_weekend_return_pct": ret_pct,
            }
        )
        time.sleep(0.15)
    btc = pd.DataFrame(rows)
    if btc.empty:
        raise RuntimeError("BTC 데이터가 비어있습니다. 네트워크/엔드포인트 확인 필요.")
    btc["weekend_start_kst"] = btc["weekend_start_utc"].dt.tz_convert("Asia/Seoul")
    btc["weekend_end_kst"] = btc["weekend_end_utc"].dt.tz_convert("Asia/Seoul")
    # 라벨용 금요일(UTC)
    btc["week_friday_utc"] = (btc["weekend_end_utc"] - pd.Timedelta(days=3)).dt.normalize()
    return btc

# ---------------------
# 엑셀 저장용: tz 제거 헬퍼
# ---------------------

def strip_tz_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    엑셀은 tz-aware datetime을 지원하지 않음 → 모든 datetime 컬럼의 tz 제거
    - *_kst 는 Asia/Seoul 시각으로 변환 후 tz 제거
    - 그 외는 UTC로 변환 후 tz 제거
    """
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, DatetimeTZDtype):
            if "kst" in col.lower():
                out[col] = out[col].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
            else:
                out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out

# ---------------------
# 메인 빌드 & 저장
# ---------------------

def build_and_save():
    """전체 파이프라인을 순차 실행하고 저장까지 수행 (가드/폴백 포함)"""
    # 1) 거시/시장 패널(주간) — 2019부터 로딩해 2020년 YoY 계산 가능
    fred_panel = load_fred_panel(MACRO_START_DATE)
    weekly_feats = make_weekly_features(fred_panel)

    # 2) 완료된 주말 구간 — 2020부터 분석
    now_utc = datetime.now(tz=UTC).replace(minute=0, second=0, microsecond=0)
    intervals = generate_weekend_intervals(WEEKEND_START_DATE, now_utc)

    # 3) BTC 주말 수익률
    btc_df = compute_coin_weekend_returns("BTC-USD", intervals)

    # 4) 병합 (키: week_friday_utc)
    data = pd.merge(btc_df, weekly_feats, on="week_friday_utc", how="left")

    # 5) O/X 플래그 추가
    data["fed_change_ox"] = np.where(data["fed_change_bp"].fillna(0).round(6) != 0, "O", "X")
    data["btc_up_ox"] = np.where((data["btc_weekend_return_pct"].fillna(0) > 0), "O", "X")

    # 6) 컬럼 정렬
    ordered = [
        "weekend_start_utc",
        "weekend_end_utc",
        "weekend_start_kst",
        "weekend_end_kst",
        "btc_start_price",
        "btc_end_price",
        "btc_weekend_return_pct",
        "btc_up_ox",
        "week_friday_utc",
        "sp500_close",
        "sp500_weekly_ret_pct",
        "sp500_updown",
        "fed_target_upper_pct",
        "fed_change_bp",
        "fed_change_ox",
        "us10y_pct",
        "dgs10_weekly_change_bp",
        "wti_usd",
        "wti_weekly_ret_pct",
        "cpi_yoy_pct",
        "pce_yoy_pct",
        "unemployment_rate_pct",
        "gdp_qoq_saar_pct",
    ]
    data = data[[c for c in ordered if c in data.columns]].sort_values("weekend_end_utc").reset_index(drop=True)

    # 7) 메타시트 (존재 컬럼만 설명 매핑)
    meta = pd.DataFrame(
        {
            "field": [c for c in ordered if c in data.columns],
            "description_ko": [
                "주말 시작(UTC)",
                "주말 종료(UTC)",
                "주말 시작(KST)",
                "주말 종료(KST)",
                "BTC-USD 시작가(주말창)",
                "BTC-USD 종료가(주말창)",
                "BTC 주말 수익률(%)",
                "BTC 주말 상승 여부(O/X)",
                "라벨용 금요일(UTC)",
                "S&P500 종가(금요일)",
                "S&P500 주간 수익률(%)",
                "S&P500 상승/하락",
                "연준 기준금리 상단(%)",
                "기준금리 주간 변화(bp)",
                "해당 주 기준금리 변동 여부(O/X)",
                "미10년물 금리(%)",
                "미10년물 주간 변화(bp)",
                "WTI 가격($/bbl)",
                "WTI 주간 수익률(%)",
                "CPI 전년동월비(%, 발표 주만)",
                "PCE 전년동월비(%, 발표 주만)",
                "실업률(%)",
                "실질GDP QoQ SAAR(%)",
            ][: len([c for c in ordered if c in data.columns])],
        }
    )

    # 8) 엑셀 저장 (tz 제거 후 기록)
    # tz 변환 — 실패해도 data_x는 항상 정의되도록 방어
    try:
        data_x = strip_tz_for_excel(data)
    except Exception as e:
        print("[경고] tz 변환 중 오류 발생 — 원본으로 저장합니다:", e)
        data_x = data.copy()
        for col in data_x.columns:
            if isinstance(data_x[col].dtype, DatetimeTZDtype):
                if "kst" in col.lower():
                    data_x[col] = data_x[col].dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
                else:
                    data_x[col] = data_x[col].dt.tz_convert("UTC").dt.tz_localize(None)

    # 엑셀 저장 엔진 자동 선택 (openpyxl → xlsxwriter → CSV 폴백)
    try:
        import openpyxl  # noqa: F401
        _engine = "openpyxl"
    except Exception:
        try:
            import xlsxwriter  # noqa: F401
            _engine = "xlsxwriter"
        except Exception:
            _engine = None

    if _engine is not None:
        with pd.ExcelWriter(OUTPUT_XLSX, engine=_engine) as xw:
            data_x.to_excel(xw, index=False, sheet_name="data")
            meta.to_excel(xw, index=False, sheet_name="meta")
        print(f"[완료] {OUTPUT_XLSX} 저장")
    else:
        data_x.to_csv("weekend_coin_data.csv", index=False)
        meta.to_csv("weekend_coin_meta.csv", index=False)
        print("[경고] openpyxl/xlsxwriter 미설치로 CSV로 저장했습니다. pip install openpyxl 권장")

    print(data_x.tail(5))


if __name__ == "__main__":
    build_and_save()

