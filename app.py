import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
import altair as alt

# レイアウト設定
st.set_page_config(layout="wide", page_title="SRイベント傾向分析")

# 日本時間(JST)の定義
JST = timezone(timedelta(hours=9))

@st.cache_data
def load_archive_data():
    # アーカイブCSVの読み込み
    url = "https://mksoul-pro.com/showroom/file/sr-event-archive.csv"
    df = pd.read_csv(url)
    
    # 日時変換 (JST)
    df['start_dt'] = pd.to_datetime(df['started_at'], unit='s', utc=True).dt.tz_convert(JST)
    
    # 分析用カラムの作成
    df['start_date'] = df['start_dt'].dt.date
    df['day_of_week'] = df['start_dt'].dt.day_name() # 曜日
    df['start_hour'] = df['start_dt'].dt.hour
    
    # 期間の計算（終了 - 開始）を日数で
    df['duration_days'] = (df['ended_at'] - df['started_at']) / 86400
    
    return df

st.title("📊 SHOWROOM イベント市場傾向分析")
st.caption("月曜/木曜スタートを中心とした、イベント数および参加ルーム数の推移を分析します。")

df_raw = load_archive_data()

# --- サイドバー設定（フィルタリング） ---
with st.sidebar:
    st.header("分析フィルター")
    
    # ① 対象者限定イベントの除外設定
    # is_entry_scope_inner が TRUE のものは「対象者限定」
    scope_filter = st.checkbox("「対象者限定」イベントを除外する", value=True)
    if scope_filter:
        df_active = df_raw[df_raw['is_entry_scope_inner'] == False].copy()
    else:
        df_active = df_raw.copy()

    # ② 曜日の絞り込み
    days = ["Monday", "Thursday", "Tuesday", "Wednesday", "Friday", "Saturday", "Sunday"]
    selected_days = st.multiselect("開始曜日で絞り込み", days, default=["Monday", "Thursday"])
    
    # ③ 期間（日数）の絞り込み
    # 7日間（1週間）がメインとのことなので、前後を含めた範囲を設定
    duration_range = st.slider("イベント期間(日)で絞り込み", 0.0, 20.0, (2.0, 15.0))

# フィルタリング実行
df_filtered = df_active[
    (df_active['day_of_week'].isin(selected_days)) &
    (df_active['duration_days'] >= duration_range[0]) &
    (df_active['duration_days'] <= duration_range[1])
]

# 週次での集計（開始日でリサンプリング）
df_filtered['week_start'] = pd.to_datetime(df_filtered['start_date']).dt.to_period('W').dt.start_time
weekly_summary = df_filtered.groupby('week_start').agg(
    event_count=('event_id', 'count'),
    # 本来は各イベントのroom_listを叩いて合計する必要がありますが、
    # ここではアーカイブの傾向として「開催数」をメインに可視化
).reset_index()

# --- メインエリア ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("週次のイベント開催数推移")
    line_chart = alt.Chart(weekly_summary).mark_line(point=True, color='#FF4B4B').encode(
        x=alt.X('week_start:T', title='週 (開始日)'),
        y=alt.Y('event_count:Q', title='開催イベント数'),
        tooltip=['week_start', 'event_count']
    ).properties(height=400).interactive()
    st.altair_chart(line_chart, use_container_width=True)

with c2:
    st.subheader("開始曜日の内訳")
    # 曜日ごとの比率を確認
    pie_data = df_active['day_of_week'].value_counts().reset_index()
    pie_chart = alt.Chart(pie_data).mark_arc().encode(
        theta=alt.Y('count:Q'),
        color=alt.Color('day_of_week:N', sort=days, title='曜日'),
        tooltip=['day_of_week', 'count']
    ).properties(height=400)
    st.altair_chart(pie_chart, use_container_width=True)

st.divider()

st.subheader("分析対象イベント一覧 (直近)")
st.write(f"現在のフィルター条件に合致するイベント: {len(df_filtered)} 件")
st.dataframe(
    df_filtered[['event_id', 'event_name', 'start_dt', 'day_of_week', 'duration_days', 'is_entry_scope_inner']]
    .sort_values('start_dt', ascending=False)
    .head(100),
    use_container_width=True
)