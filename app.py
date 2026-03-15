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
def run_total_entries_sync():
    st.info("📡 CSVデータをダウンロード中...")
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str:
        st.error("CSVファイルのダウンロードに失敗しました。")
        return

    df = pd.read_csv(io.StringIO(csv_str))
    if 'total_entries' not in df.columns:
        df['total_entries'] = 0
    
    st.write(f"🔄 全 {len(df)} 件の同期を開始します...")
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    update_count = 0
    for i, (idx, row) in enumerate(df.iterrows()):
        eid = row['event_id']
        try:
            res = requests.get(f"{API_EVENT_ROOM_LIST_URL}?event_id={eid}&p=1", headers=HEADERS, timeout=5).json()
            latest = int(res.get("total_entries", 0))
            current = int(row['total_entries']) if pd.notna(row['total_entries']) else -1
            
            if current != latest:
                df.at[idx, 'total_entries'] = latest
                update_count += 1
            
            if i % 20 == 0 or i == len(df) - 1:
                status_text.text(f"同期中: {i+1}/{len(df)} (更新済み: {update_count}件)")
                progress_bar.progress((i + 1) / len(df))
            time.sleep(0.05)
        except:
            continue
    
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    ftp_upload(CSV_PATH_FTP, csv_bytes)
    st.success(f"✅ 同期完了！ {update_count} 件更新しました。")
    st.cache_data.clear()

# --- データ読み込み（ハイブリッド版） ---
@st.cache_data(ttl=1800) # 30分キャッシュ
def load_data_hybrid():
    # 1. 固定CSVの読み込み
    csv_str = ftp_download(CSV_PATH_FTP)
    if not csv_str: return pd.DataFrame()
    df = pd.read_csv(io.StringIO(csv_str))
    
    # 2. 直近イベント（status 1,3,4）をAPIから取得して数値を上書き
    try:
        recent_event_ids = {}
        for s in [1, 3, 4]:
            res = requests.get(f"{API_EVENT_SEARCH_URL}?status={s}", headers=HEADERS, timeout=5).json()
            for ev in res.get("event_list", []):
                # ここでは詳細なルーム数までは入っていないため、IDだけ控える
                recent_event_ids[str(ev.get("event_id"))] = True
        
        # 分析対象期間に含まれる直近イベントのみ個別にルーム数を取得（高速化のため限定的実行）
        # ※運用上、CSV側が古くても分析画面を開いた瞬間に最新が見えるようにするための処理
        # (ただし、数が多いと重くなるため、今回はCSVをベースにしつつ型変換を確実に行う)
    except:
        pass

    # 日時・期間計算
    df['start_dt'] = pd.to_datetime(df['started_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['end_dt'] = pd.to_datetime(df['ended_at'], unit='s', utc=True).dt.tz_convert(JST)
    df['duration_days'] = (df['ended_at'] - df['started_at']) / 86400
    df['day_of_week'] = df['start_dt'].dt.day_name()
    df['week_start'] = df['start_dt'].dt.to_period('W').dt.start_time
    df['total_entries'] = pd.to_numeric(df['total_entries'], errors='coerce').fillna(0).astype(int)
    
    # 対象表記の変換
    df['scope_label'] = df['is_entry_scope_inner'].map({True: "対象者限定", False: "全ライバー"})
    
    return df

# --- ページ設定 ---
st.set_page_config(layout="wide", page_title="SR市場動向分析")

# --- メインUI ---
st.title("📊 SHOWROOM 市場動向分析")

# メンテナンス機能
with st.expander("🛠️ メンテナンス: 固定CSVの全件同期"):
    if st.button("全イベントの参加ルーム数をAPIと再同期（数分かかります）"):
        run_total_entries_sync()

st.divider()

df_raw = load_data_hybrid()
if df_raw.empty:
    st.error("データの読み込みに失敗しました。")
    st.stop()

# --- サイドバー・フィルタ（デフォルト値設定） ---
with st.sidebar:
    st.header("分析フィルター")
    
    # 期間：デフォルト3ヶ月（今日をエンド）
    today = datetime.now(JST).date()
    three_months_ago = today - timedelta(days=90)
    date_range = st.date_input("分析期間", [three_months_ago, today])
    
    # 対象：デフォルト「全ライバー」
    target_options = ["全ライバー", "対象者限定"]
    sel_targets = st.multiselect("対象", target_options, default=["全ライバー"])
    
    # イベント期間：デフォルト「1週間」
    dur_map = {"3日以内": (0, 3.5), "1週間": (6.0, 8.0), "10日": (9.0, 11.0), "2週間": (13.0, 15.0)}
    sel_durations = st.multiselect("イベント期間", list(dur_map.keys()) + ["その他"], default=["1週間"])
    
    # 曜日：デフォルト「Monday」
    days_list = ["Monday", "Thursday", "Tuesday", "Wednesday", "Friday", "Saturday", "Sunday"]
    sel_days = st.multiselect("開始曜日", days_list, default=["Monday"])

# --- フィルタリング ---
start_date = date_range[0]
end_date = date_range[1] if len(date_range) > 1 else start_date

df_f = df_raw[
    (df_raw['start_dt'].dt.date >= start_date) & 
    (df_raw['start_dt'].dt.date <= end_date) &
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

# --- 可視化 ---
if not df_f.empty:
    # 指標
    c1, c2, c3 = st.columns(3)
    c1.metric("対象イベント数", f"{len(df_f)}件")
    c2.metric("累計参加ルーム数", f"{df_f['total_entries'].sum():,}延べ")
    c3.metric("1イベント平均", f"{df_f['total_entries'].mean():.1f}人")

    # 二軸グラフ
    summary = df_f.groupby('week_start').agg(
        rooms=('total_entries', 'sum'),
        events=('event_id', 'count')
    ).reset_index()

    base = alt.Chart(summary).encode(x=alt.X('week_start:T', title='週 (開始日)'))
    bar = base.mark_bar(opacity=0.3, color='gray').encode(y=alt.Y('events:Q', title='イベント数'))
    line = base.mark_line(point=True, color='#FF4B4B').encode(
        y=alt.Y('rooms:Q', title='参加ルーム総数'),
        tooltip=['week_start', 'rooms', 'events']
    )
    st.altair_chart((bar + line).resolve_scale(y='independent'), use_container_width=True)

    # --- データフレーム表示（カスタマイズ） ---
    st.subheader("分析対象イベント詳細")
    
    # 表示用に整形
    df_display = df_f.copy()
    df_display['start_fmt'] = df_display['start_dt'].dt.strftime('%Y/%m/%d %H:%M')
    df_display['end_fmt'] = df_display['end_dt'].dt.strftime('%Y/%m/%d %H:%M')
    df_display['url'] = "https://www.showroom-live.com/event/" + df_display['event_url_key']
    
    # 必要な項目のみ抽出してリネーム
    df_final = df_display[['event_name', 'event_id', 'scope_label', 'start_fmt', 'end_fmt', 'total_entries', 'url']].sort_values('start_fmt', ascending=False)
    
    st.dataframe(
        df_final,
        column_config={
            "event_name": st.column_config.TextColumn("イベント名"),
            "event_id": st.column_config.LinkColumn("イベントID", help="クリックでイベントページを開きます", display_text=r"^.*$", validate=r"^.*$"),
            "scope_label": "対象",
            "start_fmt": "開始",
            "end_fmt": "終了",
            "total_entries": st.column_config.NumberColumn("参加ルーム数", format="%d"),
            "url": st.column_config.LinkColumn("ページリンク", display_text="開く")
        },
        column_order=("event_name", "event_id", "scope_label", "start_fmt", "end_fmt", "total_entries"),
        hide_index=True,
        use_container_width=True
    )
else:
    st.warning("条件に合うデータがありません。フィルターを調整してください。")