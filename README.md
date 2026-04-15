# 📈 BTC Weekend Return Analysis & Prediction Model

이 프로젝트는 비트코인(BTC)의 주말 수익률과 거시경제 지표 간의 상관관계를 분석하고, 머신러닝 모델을 통해 주말 변동성을 예측하는 도구를 제공합니다. 단순한 데이터 수집을 넘어, 최적화된 학습 범위를 찾아 예측 모델의 유효성을 높였습니다.

## 🚀 주요 업데이트 및 특징
- **학습 범위 최적화**: 전체 데이터를 학습시켰을 때보다 **최근 150주의 데이터**를 사용할 때 가장 높은 예측 정확도(Accuracy)를 보임을 확인하여, 모델의 학습 윈도우를 최적화했습니다.
- **특징 선택 (Feature Selection)**: 모든 지표를 무분별하게 사용하지 않고, 비트코인 수익률과 상관관계가 높은 상위 Feature들만을 선별하여 모델링함으로써 노이즈를 줄이고 예측 성능을 개선했습니다.
- **예측 알고리즘**: 로지스틱 회귀(Logistic Regression) 등을 활용하여 해당 주말의 코인 상승/하락 여부를 예측하며, 최신 데이터로 재학습하는 파이프라인을 포함합니다.

## 🛠 사용 기술 및 라이브러리
- **Language**: Python 3.x
- **Machine Learning**: `Scikit-learn`, `StandardScaler`
- **Data Analysis**: `Pandas`, `NumPy`, `Seaborn`
- **Data Sources**: FRED API, Coinbase Exchange API

## 📂 파일 구성
- `weekend_coin.py`: FRED 및 Coinbase API를 통해 최신 거시경제/코인 데이터를 수집하고 패널 데이터를 생성합니다.
- `predict_coin.py`: 최근 150주 데이터를 기반으로 모델을 학습하고, 해당 주말의 비트코인 방향성을 예측합니다.
- `weekend_coin_data.csv`: 분석 및 학습에 사용되는 실제 수치 데이터셋입니다.
- `weekend_coin_meta.csv`: 데이터셋의 각 변수명에 대한 한글 상세 설명서입니다.

## 📊 분석 지표 (Selected Features)
- `sp500_weekly_ret_pct`: S&P 500 주간 수익률
- `fed_target_upper_pct`: 미국 기준금리 상단
- `us10y_pct`: 미 10년물 국채 금리
- `wti_weekly_ret_pct`: WTI 유가 변동률
- 기타 실업률, GDP 등 상관관계 상위 지표 활용

---

## 🔗 Contact & Blog
개발 과정에서의 시행착오와 상세한 분석 방법론은 아래 블로그에서 확인하실 수 있습니다.

- **Blog**: [https://blog.naver.com/hsjhh33](https://blog.naver.com/hsjhh33)
- **Author**: memoryhong (SeongJin Hong)
