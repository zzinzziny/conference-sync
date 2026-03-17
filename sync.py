import requests
import yaml
import datetime
import pytz
import os
from notion_client import Client

from dotenv import load_dotenv

load_dotenv()  # .env 파일 읽기

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
CONFERENCES_DB_ID = os.environ["CONFERENCES_DB_ID"]
SKIP_PAST = True

# HuggingFace ai-deadlines GitHub API — 개별 YAML 파일 목록
HF_API_URL   = "https://api.github.com/repos/huggingface/ai-deadlines/contents/src/data/conferences"
HF_RAW_BASE  = "https://raw.githubusercontent.com/huggingface/ai-deadlines/main/src/data/conferences"

notion = Client(auth=NOTION_TOKEN)

# ── YAML 파싱 ──────────────────────────────────────────
def fetch_conferences():
    # GitHub Token이 있으면 rate limit 5000회/시간으로 증가
    github_token = os.environ.get("GITHUB_TOKEN", "")
    headers = {"Authorization": f"token {github_token}"} if github_token else {}

    resp = requests.get(HF_API_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    files = [f["name"] for f in resp.json()
             if f["name"].endswith(".yml") or f["name"].endswith(".yaml")]
    print(f"   YAML 파일 {len(files)}개 발견")

    conferences = []
    for fname in files:
        url = f"{HF_RAW_BASE}/{fname}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = yaml.safe_load(r.text)
                if isinstance(data, list):
                    conferences.extend(data)
                elif isinstance(data, dict):
                    conferences.append(data)
        except Exception as e:
            print(f"  ⚠️ {fname} 파싱 실패: {e}")

    return conferences
# ── 날짜 파싱 ──────────────────────────────────────────
def parse_deadline(deadline_str, timezone_str):
    if not deadline_str or str(deadline_str).strip().upper() == "TBD":
        return None
    try:
        tz = pytz.timezone(str(timezone_str)) if timezone_str else pytz.utc
    except Exception:
        tz = pytz.utc
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]:
        try:
            dt = datetime.datetime.strptime(str(deadline_str).strip(), fmt)
            return tz.localize(dt).astimezone(pytz.utc)
        except Exception:
            continue
    return None

# ── HF 스키마에서 deadline/abstract 추출 ───────────────
def extract_deadlines(conf):
    tz = conf.get("timezone", "UTC")
    result = {
        "submission":     None,
        "abstract":       None,
        "rebuttal_start": None,
        "rebuttal_end":   None,
        "notification":   None,
    }

    deadlines_list = conf.get("deadlines")
    if isinstance(deadlines_list, list):
        for d in deadlines_list:
            dtype = str(d.get("type", "")).lower().replace(" ", "_").replace("-", "_")
            date  = d.get("date")
            if "abstract"         in dtype: result["abstract"]       = parse_deadline(date, tz)
            elif "rebuttal_start" in dtype: result["rebuttal_start"] = parse_deadline(date, tz)
            elif "rebuttal_end"   in dtype: result["rebuttal_end"]   = parse_deadline(date, tz)
            elif "rebuttal"       in dtype: result["rebuttal_start"] = parse_deadline(date, tz)
            elif "notification"   in dtype: result["notification"]   = parse_deadline(date, tz)
            elif "paper"          in dtype \
              or "submission"     in dtype: result["submission"]     = parse_deadline(date, tz)

        # submission이 없으면 abstract/notification 아닌 첫 번째 항목으로 fallback
        if result["submission"] is None:
            for d in deadlines_list:
                dtype = str(d.get("type", "")).lower()
                if not any(k in dtype for k in ["abstract", "notification", "review", "camera", "rebuttal"]):
                    result["submission"] = parse_deadline(d.get("date"), tz)
                    break
    else:
        result["submission"] = parse_deadline(conf.get("deadline"), tz)
        result["abstract"]   = parse_deadline(conf.get("abstract_deadline"), tz)

    return result

# ── Notion 기존 항목 조회 ─────────────────────────────
def get_existing_ids():
    existing = set()
    cursor = None
    while True:
        payload = {"database_id": CONFERENCES_DB_ID, "page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor

        resp = notion.databases.query(**payload)

        for page in resp["results"]:
            rt = page["properties"].get("Conference ID", {}).get("rich_text", [])
            if rt:
                existing.add(rt[0]["text"]["content"])
        if not resp.get("has_more"):
            break
        cursor = resp["next_cursor"]
    return existing

# ── Notion에 학회 추가 ────────────────────────────────
def upsert_conference(conf, existing_ids):
    conf_id    = str(conf.get("id", ""))
    year       = str(conf.get("year", ""))
    title      = f"{conf.get('title', '')} {year}".strip()
    dl = extract_deadlines(conf)
    submission     = dl["submission"]
    abstract       = dl["abstract"]
    rebuttal_start = dl["rebuttal_start"]
    rebuttal_end   = dl["rebuttal_end"]
    notification   = dl["notification"]
    now_utc    = datetime.datetime.now(pytz.utc)

    # tags → Track (리스트를 첫 번째 값으로)
    tags = conf.get("tags", [])
    if isinstance(tags, list) and tags:
        track = tags[0].upper()[:20]
    else:
        track = str(tags).upper()[:20] if tags else "MISC"

    if conf_id in existing_ids:                                       return "exists"
    if SKIP_PAST and submission and submission < now_utc:            return "skipped"
    if SKIP_PAST and not submission:                                  return "skipped"

    # 안전한 문자열 변환
    def safe_str(v):
        if v is None:            return ""
        if isinstance(v, list):  return ", ".join(str(x) for x in v)
        return str(v)

    # venue: city + country 조합
    city    = safe_str(conf.get("city"))
    country = safe_str(conf.get("country"))
    venue   = safe_str(conf.get("venue"))
    place   = venue or ", ".join(filter(None, [city, country]))

    props = {
        "Name":            {"title": [{"text": {"content": title}}]},
        "Conference ID":   {"rich_text": [{"text": {"content": conf_id}}]},
        "Full Name":       {"rich_text": [{"text": {"content": safe_str(conf.get("full_name"))}}]},
        "Track":           {"select": {"name": track or "MISC"}},
        "Place":           {"rich_text": [{"text": {"content": place}}]},
        "Conference Date": {"rich_text": [{"text": {"content": safe_str(conf.get("date"))}}]},
        "Website":         {"url": conf.get("link") or None},
        "Note":            {"rich_text": [{"text": {"content": safe_str(conf.get("note"))}}]},
    }
    if rebuttal_start:
        props["Rebuttal Start"] = {"date": {"start": rebuttal_start.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    if rebuttal_end:
        props["Rebuttal End"]   = {"date": {"start": rebuttal_end.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    if notification:
        props["Notification"]   = {"date": {"start": notification.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    if submission:
        props["Submission Deadline"] = {"date": {"start": submission.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    if abstract:
        props["Abstract Deadline"]   = {"date": {"start": abstract.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}

    notion.pages.create(
        parent={"database_id": CONFERENCES_DB_ID},
        properties=props,
    )
    return "created"

# ── 메인 ──────────────────────────────────────────────
def main():
    print("📥 HuggingFace ai-deadlines 데이터 가져오는 중...")
    conferences = fetch_conferences()
    print(f"   총 {len(conferences)}개 학회 파싱 완료\n")

    print("🔍 Notion 기존 항목 확인 중...")
    existing_ids = get_existing_ids()
    print(f"   기존 항목: {len(existing_ids)}개\n")

    stats = {"created": 0, "exists": 0, "skipped": 0, "error": 0}

    for conf in conferences:
        try:
            result = upsert_conference(conf, existing_ids)
            stats[result] += 1
            if result == "created":
                print(f"  ➕ {conf.get('title')} {conf.get('year')}")
        except Exception as e:
            stats["error"] += 1
            print(f"  ❌ 오류 ({conf.get('id', '?')}): {e}")

    print(f"""
🎉 동기화 완료!
   ➕ 추가됨 : {stats['created']}개
   ♻️  중복   : {stats['exists']}개
   ⏭️  마감됨 : {stats['skipped']}개 (스킵)
   ❌ 오류   : {stats['error']}개
    """)

if __name__ == "__main__":
    main()