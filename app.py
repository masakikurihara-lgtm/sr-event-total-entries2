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
API_EVENT_SEARCH_URL = "https://www.showroom-live.com/api/event/search"
API_EVENT_ROOM_LIST_URL = "https://www.showroom-live.com/api/event/room_list"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

# --- FTPヘルパー関数 ---
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
def run_entries_sync(target_mode="recent"):
    st.info(f"📡 {target_mode} モードで同期を開始します...")
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str:
        st.error("CSVのダウンロードに失敗しました。")
        return

    df = pd.read_csv(io.StringIO(csv_str))
    
    if target_mode == "recent":
        sync_ids = []
        for s in [1, 3, 4]:
            try:
                res = requests.get(f"{API_EVENT_SEARCH_URL}?status={s}", headers=HEADERS, timeout=5).json()
                sync_ids.extend([ev.get("event_id") for ev in res.get("event_list", [])])
            except: continue
        sync_ids = list(set(sync_ids))
    else:
        sync_ids = df['event_id'].tolist()

    if not sync_ids:
        st.warning("同期対象が見つかりませんでした。")
        return

    st.write(f"🔄 {len(sync_ids)} 件の数値を最新化しています...")
    progress_bar = st.progress(0)
    
    update_count = 0
    for i, eid in enumerate(sync_ids):
        try:
            res = requests.get(f"{API_EVENT_ROOM_LIST_URL}?event_id={eid}&p=1", headers=HEADERS, timeout=5).json()
            latest = int(res.get("total_entries", 0))
            
            idx = df.index[df['event_id'] == int(eid)]
            if not idx.empty:
                current = df.at[idx[0], 'total_entries']
                if current != latest:
                    df.at[idx[0], 'total_entries'] = latest
                    update_count += 1
            
            progress_bar.progress((i + 1) / len(sync_ids))
            time.sleep(0.05)
        except: continue
    
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    ftp_upload(CSV_PATH_FTP, csv_bytes)
    st.success(f"✅ 同期完了！ {update_count} 件更新しました。")
    st.cache_data.clear()

# --- データ読み込み ---
@st.cache_data(ttl=600)
def load_data():
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str: return pd.DataFrame()
    df = pd.read_csv(io.StringIO(csv_str))
    
    df['start_dt'] = pd.to_datetime(df['started_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['end_dt'] = pd.to_datetime(df['ended_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['start_date_only'] = df['start_dt'].dt.normalize() 
    
    # 統計用の「週」列（月曜日基準）
    df['week_commencing'] = df['start_dt'].dt.to_period('W-MON').dt.start_time
    
    df['duration_days'] = (df['ended_at'] - df['started_at']) / 86400
    df['day_of_week'] = df['start_dt'].dt.day_name()
    df['total_entries'] = pd.to_numeric(df['total_entries'], errors='coerce').fillna(0).astype(int)
    df['scope_label'] = df['is_entry_scope_inner'].map({True: "対象者限定", False: "全ライバー"})
    
    return df

# --- UI設定 ---
st.set_page_config(layout="wide", page_title="SR市場動向分析")
st.title("📊 SHOWROOM 市場動向分析")

with st.expander("🛠️ 参加ルーム数の更新・同期設定"):
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("直近イベント(status 1,3,4)のみ最新化"):
            run_entries_sync(target_mode="recent")
    with col_b:
        if st.button("CSV全件を最新化"):
            run_entries_sync(target_mode="all")

st.divider()

df_raw = load_data()
if df_raw.empty:
    st.error("データの読み込みに失敗しました。")
    st.stop()

# --- フィルター ---
with st.sidebar:
    st.header("分析フィルター")
    today = datetime.now(JST).date()
    three_months_ago = today - timedelta(days=90)
    date_range = st.date_input("分析期間", [three_months_ago, today])
    sel_targets = st.multiselect("対象", ["全ライバー", "対象者限定"], default=["全ライバー"])
    dur_map = {"3日以内": (0, 3.5), "1週間": (6.0, 8.0), "10日": (9.0, 11.0), "2週間": (13.0, 15.0)}
    sel_durations = st.multiselect("イベント期間", list(dur_map.keys()) + ["その他"], default=["1週間"])
    sel_days = st.multiselect("開始曜日", ["Monday", "Thursday", "Tuesday", "Wednesday", "Friday", "Saturday", "Sunday"], default=["Monday"])

# フィルタリング
start_date_filter = date_range[0]
end_date_filter = date_range[1] if len(date_range) > 1 else start_date_filter

df_f = df_raw[
    (df_raw['start_dt'].dt.date >= start_date_filter) & 
    (df_raw['start_dt'].dt.date <= end_date_filter) &
    (df_raw['scope_label'].isin(sel_targets)) &
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

# --- 可視化と統計指標 ---
if not df_f.empty:
    # 週単位の集計データを算出
    weekly_stats = df_f.groupby('week_commencing').agg(
        event_count=('event_id', 'count'),
        total_rooms=('total_entries', 'sum')
    )
    
    avg_events_per_week = weekly_stats['event_count'].mean()
    avg_rooms_per_week = weekly_stats['total_rooms'].mean()

    c1, c2, c3 = st.columns(3)
    c1.metric("対象イベント総数", f"{len(df_f)}件")
    c2.metric("平均開催イベント数 / 週", f"{avg_events_per_week:.1f}件")
    c3.metric("平均参加ルーム数 / 週", f"{avg_rooms_per_week:.1f}ルーム")

    # グラフ用集計
    summary = df_f.groupby('start_date_only').agg(
        rooms=('total_entries', 'sum'),
        events=('event_id', 'count')
    ).reset_index()

    tooltip_content = [
        alt.Tooltip('start_date_only:T', title='開始日'),
        alt.Tooltip('rooms:Q', title='参加ルーム総数'),
        alt.Tooltip('events:Q', title='イベント数')
    ]

    base = alt.Chart(summary).encode(x=alt.X('start_date_only:T', title='イベント開始日'))
    bar = base.mark_bar(opacity=0.3, color='gray').encode(y=alt.Y('events:Q', title='イベント数'), tooltip=tooltip_content)
    line = base.mark_line(point=True, color='#FF4B4B').encode(y=alt.Y('rooms:Q', title='参加ルーム総数'), tooltip=tooltip_content)
    
    st.altair_chart((bar + line).resolve_scale(y='independent'), use_container_width=True)

    # テーブル表示
    st.subheader("分析対象イベント詳細")
    df_display = df_f.copy()
    df_display['start_fmt'] = df_display['start_dt'].dt.strftime('%Y/%m/%d %H:%M')
    df_display['end_fmt'] = df_display['end_dt'].dt.strftime('%Y/%m/%d %H:%M')
    df_display['event_link'] = "https://www.showroom-live.com/event/" + df_display['event_url_key'].astype(str)
    df_display['event_id_str'] = df_display['event_id'].astype(str)
    
    df_final = df_display[['event_name', 'event_link', 'event_id_str', 'scope_label', 'start_fmt', 'end_fmt', 'total_entries']].sort_values('start_fmt', ascending=False)
    
    st.dataframe(
        df_final,
        column_config={
            "event_name": st.column_config.TextColumn("イベント名", width="large"),
            "event_link": st.column_config.LinkColumn("リンク", display_text="開く"),
            "event_id_str": "イベントID",
            "scope_label": "対象",
            "start_fmt": "開始",
            "end_fmt": "終了",
            "total_entries": st.column_config.NumberColumn("参加ルーム数", format="%d")
        },
        column_order=("event_name", "event_link", "event_id_str", "scope_label", "start_fmt", "end_fmt", "total_entries"),
        hide_index=True,
        use_container_width=True
    )
else:
    st.warning("条件に合うデータがありません。")