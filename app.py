import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime, timezone, timedelta
import altair as alt

# ① レイアウト設定
st.set_page_config(layout="wide", page_title="SR市場動向分析")

# 日本時間(JST)の定義
JST = timezone(timedelta(hours=9))

@st.cache_data
def load_archive_data():
    url = "https://mksoul-pro.com/showroom/file/sr-event-archive.csv"
    df = pd.read_csv(url)
    df['start_dt'] = pd.to_datetime(df['started_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['duration_days'] = (df['ended_at'] - df['started_at']) / 86400
    df['day_of_week'] = df['start_dt'].dt.day_name()
    return df

# APIから参加ルーム総数を取得する関数
def fetch_total_entries(event_id):
    api_url = f"https://www.showroom-live.com/api/event/room_list?event_id={event_id}&p=1"
    try:
        res = requests.get(api_url, timeout=5).json()
        return res.get("total_entries", 0)
    except:
        return 0

df_raw = load_archive_data()

# --- サイドバー設定 ---
with st.sidebar:
    st.header("1. 取得範囲の設定")
    # カレンダーでの範囲指定
    today = datetime.now(JST).date()
    start_default = today - timedelta(days=30)
    date_range = st.date_input("分析期間を選択", [start_default, today])
    
    st.header("2. 分析フィルター")
    # 対象設定 (添付2枚目イメージ)
    target_options = ["全ライバー", "対象者限定"]
    selected_targets = st.multiselect("対象でフィルタ", target_options, default=["全ライバー"])
    
    # 期間設定 (添付1枚目イメージ)
    duration_options = ["3日以内", "1週間", "10日", "2週間", "その他"]
    selected_durations = st.multiselect("期間でフィルタ", duration_options, default=["1週間", "10日"])

    # 曜日設定
    days = ["Monday", "Thursday", "Tuesday", "Wednesday", "Friday", "Saturday", "Sunday"]
    selected_days = st.multiselect("開始曜日", days, default=["Monday", "Thursday"])

# --- フィルタリング実行 ---
# 日付範囲フィルタ
if len(date_range) == 2:
    df_filtered = df_raw[
        (df_raw['start_dt'].dt.date >= date_range[0]) & 
        (df_raw['start_dt'].dt.date <= date_range[1])
    ].copy()
else:
    df_filtered = df_raw.copy()

# 対象フィルタ
is_inner_map = {"全ライバー": False, "対象者限定": True}
target_bools = [is_inner_map[t] for t in selected_targets]
df_filtered = df_filtered[df_filtered['is_entry_scope_inner'].isin(target_bools)]

# 期間フィルタ関数
def check_duration(d):
    if "3日以内" in selected_durations and d <= 3.5: return True
    if "1週間" in selected_durations and 6.0 <= d <= 8.0: return True
    if "10日" in selected_durations and 9.0 <= d <= 11.0: return True
    if "2週間" in selected_durations and 13.0 <= d <= 15.0: return True
    if "その他" in selected_durations:
        if not (d <= 3.5 or 6.0 <= d <= 8.0 or 9.0 <= d <= 11.0 or 13.0 <= d <= 15.0): return True
    return False

df_filtered = df_filtered[df_filtered['duration_days'].apply(check_duration)]
df_filtered = df_filtered[df_filtered['day_of_week'].isin(selected_days)]

# --- メインエリア ---
st.title("📊 SHOWROOM 市場動向分析")

if not df_filtered.empty:
    st.write(f"対象イベント数: {len(df_filtered)} 件")
    
    if st.button('ルーム総数を集計してグラフを表示', type='primary'):
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, (idx, row) in enumerate(df_filtered.iterrows()):
            status_text.text(f"集計中 ({i+1}/{len(df_filtered)}): {row['event_name']}")
            count = fetch_total_entries(row['event_id'])
            
            results.append({
                "week_start": row['start_dt'].to_period('W').start_time,
                "room_count": count
            })
            progress_bar.progress((i + 1) / len(df_filtered))
            time.sleep(0.05)
            
        status_text.success("集計完了！")
        
        # 週次集計
        res_df = pd.DataFrame(results)
        weekly_summary = res_df.groupby('week_start').agg(
            total_rooms=('room_count', 'sum'),
            avg_rooms=('room_count', 'mean'),
            event_count=('room_count', 'count')
        ).reset_index()

        # 二軸グラフの作成
        base = alt.Chart(weekly_summary).encode(x=alt.X('week_start:T', title='週 (開始日)'))
        
        # 棒グラフ: イベント開催数
        bar = base.mark_bar(opacity=0.3, color='gray').encode(
            y=alt.Y('event_count:Q', title='開催イベント数')
        )
        
        # 折れ線グラフ: 参加ルーム総数
        line = base.mark_line(point=True, color='#FF4B4B').encode(
            y=alt.Y('total_rooms:Q', title='参加ルーム総数'),
            tooltip=['week_start', 'total_rooms', 'event_count']
        )
        
        st.altair_chart((bar + line).resolve_scale(y='independent'), use_container_width=True)
    
    st.divider()
    st.subheader("分析対象イベント一覧")
    st.dataframe(
        df_filtered[['event_id', 'event_name', 'start_dt', 'day_of_week', 'duration_days', 'is_entry_scope_inner']]
        .sort_values('start_dt', ascending=False),
        use_container_width=True, hide_index=True
    )
else:
    st.warning("条件に合致するイベントが見つかりません。フィルター設定を調整してください。")