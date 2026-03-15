import streamlit as st
import pandas as pd
import requests
import io
import ftplib
import time
import pytz
from datetime import datetime, timezone, timedelta
import altair as alt

# --- 定数・タイムゾーン設定 ---
JST = pytz.timezone('Asia/Tokyo')
CSV_PATH_FTP = "/mksoul-pro.com/showroom/file/sr-event-archive.csv"
API_EVENT_ROOM_LIST_URL = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

# --- FTPヘルパー関数（既存ツール準拠） ---
def ftp_download(file_path):
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["password"]
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(ftp_user, ftp_pass)
        buffer = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {file_path}", buffer.write)
            buffer.seek(0)
            return buffer.getvalue().decode('utf-8-sig')
        except Exception:
            return None

def ftp_upload(file_path, content_bytes):
    ftp_host = st.secrets["ftp"]["host"]
    ftp_user = st.secrets["ftp"]["user"]
    ftp_pass = st.secrets["ftp"]["password"]
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login(ftp_user, ftp_pass)
        with io.BytesIO(content_bytes) as f:
            ftp.storbinary(f"STOR {file_path}", f)

# --- 更新処理ロジック ---
def run_total_entries_sync():
    st.info("📡 CSVデータをダウンロード中...")
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str:
        st.error("CSVファイルのダウンロードに失敗しました。")
        return

    df = pd.read_csv(io.StringIO(csv_str))
    
    if 'total_entries' not in df.columns:
        df['total_entries'] = 0
    
    st.write(f"🔄 全 {len(df)} 件の参加ルーム数をAPIと同期します...")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 全件ループ（変更がある場合のみ上書き）
    update_count = 0
    for i, (idx, row) in enumerate(df.iterrows()):
        eid = row['event_id']
        try:
            # APIから最新の総数を取得
            res = requests.get(f"{API_EVENT_ROOM_LIST_URL}?event_id={eid}&p=1", headers=HEADERS, timeout=5).json()
            latest = int(res.get("total_entries", 0))
            
            # 型を合わせて比較
            current = int(row['total_entries']) if pd.notna(row['total_entries']) else -1
            
            if current != latest:
                df.at[idx, 'total_entries'] = latest
                update_count += 1
            
            if i % 10 == 0 or i == len(df) - 1:
                status_text.text(f"同期中: {i+1}/{len(df)} (更新済み: {update_count}件)")
                progress_bar.progress((i + 1) / len(df))
            
            time.sleep(0.05)
        except Exception:
            continue
    
    # アップロード
    st.info("☁️ 修正したデータをFTPへアップロード中...")
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    ftp_upload(CSV_PATH_FTP, csv_bytes)
    
    st.success(f"✅ 同期完了！ {update_count} 件のルーム数を更新しました。")
    st.cache_data.clear()

# --- ページ設定 ---
st.set_page_config(layout="wide", page_title="SR市場動向分析")

# --- メインUI ---
st.title("📊 SHOWROOM 市場動向分析")

# メンテナンス機能
with st.expander("🛠️ メンテナンス: 固定CSVの参加ルーム数を最新にする"):
    st.caption("ボタンを押すと、全イベントの現在の参加ルーム数をAPIから取得し、固定CSVを上書きします。")
    if st.button("全イベントのルーム総数を同期実行"):
        run_total_entries_sync()

st.divider()

# --- データ読み込み ---
@st.cache_data(ttl=3600)
def load_data():
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(csv_str))
    # 日時・期間計算
    df['start_dt'] = pd.to_datetime(df['started_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['duration_days'] = (df['ended_at'] - df['started_at']) / 86400
    df['day_of_week'] = df['start_dt'].dt.day_name()
    df['week_start'] = df['start_dt'].dt.to_period('W').dt.start_time
    df['total_entries'] = pd.to_numeric(df['total_entries'], errors='coerce').fillna(0).astype(int)
    return df

df_raw = load_data()

if df_raw.empty:
    st.error("データの読み込みに失敗しました。CSVファイルまたはFTP設定を確認してください。")
    st.stop()

# --- サイドバー・フィルタ ---
with st.sidebar:
    st.header("分析フィルター")
    
    # 期間指定
    max_d = df_raw['start_dt'].max().date()
    date_range = st.date_input("分析期間", [max_d - timedelta(days=90), max_d])
    
    # 対象フィルタ (添付2枚目準拠)
    target_map = {"全ライバー": False, "対象者限定": True}
    sel_targets = st.multiselect("対象", list(target_map.keys()), default=["全ライバー"])
    sel_target_bools = [target_map[t] for t in sel_targets]
    
    # 期間フィルタ (添付1枚目準拠)
    dur_map = {"3日以内": (0, 3.5), "1週間": (6.0, 8.0), "10日": (9.0, 11.0), "2週間": (13.0, 15.0)}
    sel_durations = st.multiselect("イベント期間", list(dur_map.keys()) + ["その他"], default=["1週間", "10日"])
    
    # 曜日フィルタ
    days = ["Monday", "Thursday", "Tuesday", "Wednesday", "Friday", "Saturday", "Sunday"]
    sel_days = st.multiselect("開始曜日", days, default=["Monday", "Thursday"])

# --- フィルタリング ---
df_f = df_raw[
    (df_raw['start_dt'].dt.date >= date_range[0]) & 
    (df_raw['start_dt'].dt.date <= (date_range[1] if len(date_range)>1 else date_range[0])) &
    (df_raw['is_entry_scope_inner'].isin(sel_target_bools)) &
    (df_raw['day_of_week'].isin(sel_days))
].copy()

def check_dur(d):
    for s in sel_durations:
        if s in dur_map:
            l, h = dur_map[s]
            if l <= d <= h: return True
        elif s == "その他":
            if not any(l <= d <= h for l, h in dur_map.values()): return True
    return False

df_f = df_f[df_f['duration_days'].apply(check_dur)]

# --- 可視化 ---
if not df_f.empty:
    summary = df_f.groupby('week_start').agg(
        rooms=('total_entries', 'sum'),
        events=('event_id', 'count')
    ).reset_index()

    # 指標
    c1, c2, c3 = st.columns(3)
    c1.metric("対象イベント数", f"{len(df_f)}件")
    c2.metric("累計参加ルーム数", f"{df_f['total_entries'].sum():,}延べ")
    c3.metric("1イベント平均", f"{df_f['total_entries'].mean():.1f}人")

    # 二軸グラフ
    base = alt.Chart(summary).encode(x=alt.X('week_start:T', title='週 (開始日)'))
    bar = base.mark_bar(opacity=0.3, color='gray').encode(y=alt.Y('events:Q', title='イベント数'))
    line = base.mark_line(point=True, color='#FF4B4B').encode(
        y=alt.Y('rooms:Q', title='参加ルーム総数'),
        tooltip=['week_start', 'rooms', 'events']
    )
    st.altair_chart((bar + line).resolve_scale(y='independent'), use_container_width=True)

    st.dataframe(df_f[['event_name', 'start_dt', 'duration_days', 'total_entries']].sort_values('start_dt', ascending=False), use_container_width=True)
else:
    st.warning("条件に合うデータがありません。")