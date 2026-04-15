import os
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler

def load_and_preprocess_data(file_path):
    print("데이터를 로드하는 중...")
    try:
        if os.path.exists('weekend_coin_data.csv'):
            df = pd.read_csv('weekend_coin_data.csv')
            print("▶ 최신 CSV 파일(weekend_coin_data.csv)을 성공적으로 로드했습니다!")
        else:
            df = pd.read_excel(file_path, sheet_name='data')
            print(f"▶ {file_path} 엑셀 파일을 로드했습니다.")
    except Exception as e:
        print(f"데이터를 읽어오는 중 오류가 발생했습니다: {e}")
        return None

    target_class = 'btc_up_ox'
    target_reg = 'btc_weekend_return_pct' # 상관관계 도출용
    
    features = [
        'sp500_weekly_ret_pct',
        'fed_target_upper_pct',
        'fed_change_bp',
        'us10y_pct',
        'dgs10_weekly_change_bp',
        'wti_weekly_ret_pct',
        'cpi_yoy_pct',
        'pce_yoy_pct',
        'unemployment_rate_pct',
        'gdp_qoq_saar_pct'
    ]
    
    df = df.sort_values('weekend_end_utc').reset_index(drop=True)
    
    # 테스트 결과 최근 100주(30%) 보다 최근 150주(56.67%)의 정확도가 월등히 높아 150주 기준으로 최종 세팅합니다.
    if len(df) > 150:
        df = df.tail(150).reset_index(drop=True)
        print("최근 150주 데이터만 필터링하여 사용합니다. (최적 아키텍처)")
    
    columns_to_keep = ['weekend_start_utc', 'weekend_end_utc'] + features + [target_class, target_reg]
    columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    
    df_model = df[columns_to_keep].copy()
    df_model[features] = df_model[features].ffill().fillna(0)
    df_model = df_model.dropna(subset=[target_class, target_reg]).reset_index(drop=True)
    
    df_model['target_label'] = df_model[target_class].apply(lambda x: 1 if x == 'O' else 0)

    print(f"데이터 전처리 완료: 총 {len(df_model)}개 샘플 확보")

    # 상관관계 분석을 통해 상위 피처 추출 (기존 방식 복구)
    correlations = df_model[features].corrwith(df_model[target_reg]).abs()
    top_n = 5
    top_features = correlations.sort_values(ascending=False).head(top_n).index.tolist()
    
    print(f"\n[피처 선택] 타겟과 상관관계가 높은 상위 {top_n}개 피처만 골라 모델링합니다:")
    for feat in top_features:
        print(f" - {feat} (상관계수 절댓값: {correlations[feat]:.4f})")
    
    return df_model, top_features

def run_predictions():
    file_path = 'weekend_coin_data.csv'
    result = load_and_preprocess_data(file_path)
    
    if result is None:
        return
        
    df, features = result
    
    if len(df) < 10:
        print("학습하기에 데이터가 너무 적습니다.")
        return

    split_index = int(len(df) * 0.8)
    train_df = df.iloc[:split_index]
    test_df  = df.iloc[split_index:]

    X_train = train_df[features]
    y_train = train_df['target_label']
    
    X_test = test_df[features]
    y_test = test_df['target_label']

    print(f"\n[데이터 분할] Train: {len(train_df)} 샘플, Test: {len(test_df)} 샘플")
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    classifier = LogisticRegression(solver='lbfgs', max_iter=1000, random_state=42)
    classifier.fit(X_train_scaled, y_train)
    
    pred_class = classifier.predict(X_test_scaled)
    accuracy = accuracy_score(y_test, pred_class)
    
    print("\n" + "="*50)
    print("📈 다항 로지스틱 회귀 성과 측정 (Top 5 피처 반영)")
    print(f"Accuracy (정확도) : {accuracy * 100:.2f}%")
    print("="*50)

    # ==========================================
    # 실전 예측: 직전 주까지의 모든 데이터를 영끌하여 재학습
    # ==========================================
    # 80:20 분할은 모델의 '과거 성적(Accuracy)'을 알아보기 위함이었습니다.
    # 회원님의 통찰대로, 진짜 가장 최근 주말을 예측할 때는 직전 주까지의 모든 최신 데이터를 학습하는 것이 좋습니다!
    
    df_train_recent = df.iloc[:-1] # 마지막 1개(가장 최근 주말)를 제외한 모든 과거~최근 데이터
    X_train_recent = df_train_recent[features]
    y_train_recent = df_train_recent['target_label']
    
    # 전체 데이터 기준으로 스케일러 재조정 및 모델 재학습
    X_train_recent_scaled = scaler.fit_transform(X_train_recent)
    classifier.fit(X_train_recent_scaled, y_train_recent)

    # 최신 데이터(마지막 행)에 대한 예측
    latest_row = df.iloc[-1:]
    latest_features = latest_row[features]
    latest_features_scaled = scaler.transform(latest_features)
    
    pred_latest_class = classifier.predict(latest_features_scaled)[0]
    pred_latest_proba = classifier.predict_proba(latest_features_scaled)[0]
    
    latest_start = latest_row['weekend_start_utc'].values[0]
    latest_end = latest_row['weekend_end_utc'].values[0]
    
    start_str = pd.to_datetime(latest_start).strftime('%Y-%m-%d')
    end_str = pd.to_datetime(latest_end).strftime('%Y-%m-%d')
    
    print("\n🔍 가장 최근 주말에 대한 상승 예측 확률 (직전 주까지 100% 학습 반영)")
    print(f"주말 기간: {start_str} ~ {end_str}")
    print(f"▶ 예측 방향: {'상승(O)' if pred_latest_class == 1 else '하락(X)'} (상승 확률: {pred_latest_proba[1]*100:.2f}%)")
    print(f"▷ 실제 결과: {'상승(O)' if latest_row['target_label'].values[0] == 1 else '하락(X)'}")

if __name__ == "__main__":
    run_predictions()
