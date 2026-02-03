#!/usr/bin/env python3
"""
Blip.kr K-POP Schedule Scraper v4 (RSC Payload + Unit Mapping)

blip.kr은 Next.js App Router를 사용하며, SSR HTML 테이블에는
셀당 최대 3개 이벤트만 표시. 전체 데이터는 React Server Component
payload (self.__next_f.push)에 JSON으로 포함됨.

v4 변경사항:
- 홈페이지에서 unitId → 그룹명(한글/영문) 매핑 동적 수집
- 이벤트에 unitId 포함하여 그룹별 필터링 지원
- schedule.json에 units 매핑 테이블 추가

스크래핑 범위: 전월 1일 ~ 실행일로부터 1년 후까지
"""

import json
import re
import time
import random
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# ─── 카테고리 정의 ───

CATEGORIES = {
    "축하": "#4ECDC4",
    "발매": "#FF6B6B",
    "방송": "#FFE66D",
    "구매": "#95E1D3",
    "행사": "#C7CEEA",
    "기타": "#999999",
    "비공식": "#FFB6B9",
    "SNS": "#8EC5FC",
}

# blip.kr typeId → 기본 카테고리 매핑
TYPE_ID_MAP = {
    2: "발매",    # Release, Teaser, MV, Concept Photo 등
    4: "축하",    # 생일, 기념일, 수상, 데뷔 기념 등
}

# 제목 키워드 기반 세부 카테고리 보정
CATEGORY_KEYWORDS = {
    "방송": [
        "인기가요", "Inkigayo", "음악중심", "MusicCore", "M COUNTDOWN",
        "뮤직뱅크", "Music Bank", "SHOW CHAMPION", "음악방송", "1위",
    ],
    "행사": [
        "콘서트", "Concert", "CONCERT", "팬미팅", "Fan Meeting",
        "TOUR", "Tour", "쇼케이스", "Showcase", "LIVE",
    ],
    "구매": [
        "예약", "Pre-order", "PRE-ORDER", "구매", "Purchase",
        "티켓", "Ticket", "TICKET",
    ],
    "SNS": [
        "V LIVE", "위버스", "Weverse", "인스타",
    ],
    "발매": [
        "Release", "발매", "RELEASE", "MV", "Teaser", "TEASER",
        "Concept", "CONCEPT", "Album", "ALBUM", "공개",
    ],
    "축하": [
        "HAPPY", "DAY!", "생일", "birthday", "기념일",
        "데뷔", "주년", "anniversary", "수상",
    ],
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}


# ─── RSC Payload 공통 디코딩 ───

def decode_rsc_chunk(chunk: str) -> str:
    """JavaScript 이중 이스케이프를 해제하여 파싱 가능한 문자열로 변환"""
    raw = chunk
    raw = raw.replace("\\\\", "\x00BS\x00")
    raw = raw.replace('\\"', '"')
    raw = raw.replace("\\n", "\n")
    raw = raw.replace("\x00BS\x00", "\\")
    return raw


# ─── 유닛 매핑 수집 ───

def fetch_unit_mapping() -> dict:
    """
    blip.kr 홈페이지 RSC payload에서 unitId → 그룹명 매핑 추출.

    홈페이지에는 {"unitId":N,"artistId":N,"isFilter":N,"blipName":"그룹명",...}
    형태의 아티스트 목록이 포함됨. names 배열에서 영문명도 추출.

    Returns:
        {unitId(int): {"ko": "한글명", "en": "영문명"}, ...}
    """
    print("🏠 홈페이지에서 유닛 매핑 수집 중...")

    req = Request("https://blip.kr", headers=DEFAULT_HEADERS)

    try:
        with urlopen(req, timeout=20) as response:
            html = response.read().decode("utf-8")
    except (URLError, HTTPError) as e:
        print(f"  ⚠️  홈페이지 요청 실패: {e}")
        return {}

    rsc_chunks = re.findall(
        r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL
    )

    for chunk in rsc_chunks:
        if "blipName" not in chunk:
            continue

        raw = decode_rsc_chunk(chunk)

        # unitId, blipName(한글명) 추출
        ko_matches = re.findall(
            r'\{"unitId":(\d+),"artistId":\d+,"isFilter":\d+,"blipName":"([^"]*)"',
            raw,
        )

        # 영문명 추출
        en_matches = re.findall(
            r'\{"code":"en","name":"([^"]*)","unitId":(\d+)\}',
            raw,
        )
        en_map = {}
        for en_name, uid_str in en_matches:
            en_map[int(uid_str)] = en_name

        # 매핑 구성
        unit_map = {}
        for uid_str, ko_name in ko_matches:
            uid = int(uid_str)
            unit_map[uid] = {
                "ko": ko_name,
                "en": en_map.get(uid, ko_name),
            }

        print(f"  ✅ {len(unit_map)}개 그룹 매핑 확보")
        return unit_map

    print("  ⚠️  홈페이지에서 유닛 데이터를 찾을 수 없음")
    return {}


# ─── RSC Payload 이벤트 파싱 ───

def extract_rsc_events(html: str) -> list[dict]:
    """
    Next.js RSC payload에서 스케줄 이벤트 추출.
    self.__next_f.push([1, "..."]) 내의 scheduleId 객체들을 파싱.
    """
    rsc_chunks = re.findall(
        r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL
    )

    for chunk in rsc_chunks:
        if "scheduleId" not in chunk:
            continue

        raw = decode_rsc_chunk(chunk)

        events = []
        pos = 0

        while True:
            obj_start = raw.find('{"scheduleId"', pos)
            if obj_start < 0:
                break

            # 매칭되는 중괄호 끝 찾기
            depth = 0
            obj_end = obj_start
            for j in range(obj_start, min(obj_start + 10000, len(raw))):
                if raw[j] == "{":
                    depth += 1
                elif raw[j] == "}":
                    depth -= 1
                if depth == 0:
                    obj_end = j + 1
                    break

            obj_str = raw[obj_start:obj_end]

            # message 필드 내 줄바꿈 등으로 JSON 파싱 실패 방지
            obj_str = re.sub(r'"message":"[^"]*"', '"message":""', obj_str)

            try:
                obj = json.loads(obj_str)
                events.append(obj)
            except json.JSONDecodeError:
                pass

            pos = obj_end + 1

        if events:
            return events

    return []


def classify_event(event: dict) -> str:
    """typeId + 제목 키워드로 카테고리 결정"""
    type_id = event.get("typeId")
    title = event.get("title", "")

    # 키워드 기반 세부 분류 (우선)
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in title:
                return category

    # typeId 기반 기본 분류 (fallback)
    return TYPE_ID_MAP.get(type_id, "기타")


def parse_events_to_dict(events: list[dict], year: int, month: int) -> dict:
    """RSC 이벤트 리스트 → {날짜: [이벤트]} 딕셔너리 변환"""
    result = {}
    month_prefix = f"{year}-{month:02d}-"

    for event in events:
        start_time = event.get("startTime", "")
        if not start_time:
            continue

        # ISO 시간 → KST 날짜 변환
        # startTime: "2026-01-31T15:00:00.000Z" (UTC) → KST +9h → 2026-02-01
        try:
            utc_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            kst_dt = utc_dt + timedelta(hours=9)
            date_key = kst_dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            continue

        # 해당 월만 필터
        if not date_key.startswith(month_prefix):
            continue

        title = event.get("title", "").strip()
        if not title:
            continue

        category = classify_event(event)
        unit_id = event.get("unitId")

        if date_key not in result:
            result[date_key] = []

        # 중복 제거
        existing_titles = {e["title"] for e in result[date_key]}
        if title not in existing_titles:
            entry = {
                "title": title,
                "category": category,
            }
            if unit_id is not None:
                entry["unitId"] = unit_id
            result[date_key].append(entry)

    return result


# ─── HTTP 요청 ───

def fetch_month(year: int, month: int) -> dict:
    """특정 월의 스케줄 페이지에서 RSC payload 추출"""
    url = f"https://blip.kr/schedule?year={year}&month={month}"

    req = Request(url, headers=DEFAULT_HEADERS)

    try:
        with urlopen(req, timeout=20) as response:
            html = response.read().decode("utf-8")

        events = extract_rsc_events(html)

        if not events:
            print(f"  ⚠️  {year}-{month:02d}: RSC payload에 이벤트 없음")
            return {}

        return parse_events_to_dict(events, year, month)

    except (URLError, HTTPError) as e:
        print(f"  ⚠️  {year}-{month:02d} 요청 실패: {e}")
        return {}
    except Exception as e:
        print(f"  ⚠️  {year}-{month:02d} 파싱 오류: {e}")
        return {}


# ─── 메인 스크래핑 ───

def scrape_schedule() -> dict:
    """전월 1일 ~ 실행일 기준 1년 후까지 스케줄 수집"""
    today = datetime.now()

    # 유닛 매핑 먼저 수집
    unit_map = fetch_unit_mapping()
    time.sleep(random.uniform(1.0, 2.0))

    # 시작: 전월 1일
    if today.month == 1:
        start_year, start_month = today.year - 1, 12
    else:
        start_year, start_month = today.year, today.month - 1

    # 종료: 오늘로부터 1년 후
    end_date = today + timedelta(days=365)
    end_year, end_month = end_date.year, end_date.month

    print(f"📅 스크래핑 범위: {start_year}-{start_month:02d} ~ {end_year}-{end_month:02d}")

    all_events = {}
    total_months = 0

    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        print(f"  🔄 {year}-{month:02d} 수집 중...")

        month_events = fetch_month(year, month)

        for date_key, event_list in month_events.items():
            if date_key not in all_events:
                all_events[date_key] = []
            existing_titles = {e["title"] for e in all_events[date_key]}
            for event in event_list:
                if event["title"] not in existing_titles:
                    all_events[date_key].append(event)
                    existing_titles.add(event["title"])

        total_months += 1

        # 다음 월
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

        # 요청 간 간격 (1-2초)
        time.sleep(random.uniform(1.0, 2.0))

    # 날짜 순 정렬
    sorted_events = dict(sorted(all_events.items()))

    total_events = sum(len(v) for v in sorted_events.values())
    total_days = len(sorted_events)

    # 실제 등장하는 unitId만 필터링하여 units 테이블 생성
    used_unit_ids = set()
    for date_events in sorted_events.values():
        for event in date_events:
            uid = event.get("unitId")
            if uid is not None:
                used_unit_ids.add(uid)

    # units: 이벤트에 등장하는 그룹만 포함 (JSON key는 string)
    units = {}
    unmapped = 0
    for uid in sorted(used_unit_ids):
        if uid in unit_map:
            units[str(uid)] = unit_map[uid]
        else:
            unmapped += 1
            units[str(uid)] = {"ko": "기타 그룹", "en": "Other"}

    print(f"\n✅ 스크래핑 완료!")
    print(f"   - 수집 월수: {total_months}개월")
    print(f"   - 일정 있는 날: {total_days}일")
    print(f"   - 총 이벤트: {total_events}개")
    print(f"   - 그룹 수: {len(units)}개 (매핑: {len(units)-unmapped}, 기타: {unmapped})")

    result = {
        "updated_at": today.isoformat(),
        "range": {
            "start": f"{start_year}-{start_month:02d}-01",
            "end": f"{end_year}-{end_month:02d}-{_last_day(end_year, end_month):02d}",
        },
        "categories": list(CATEGORIES.keys()),
        "category_colors": CATEGORIES,
        "units": units,
        "events": sorted_events,
        "stats": {
            "months_scraped": total_months,
            "days_with_events": total_days,
            "total_events": total_events,
            "total_units": len(units),
        },
    }

    return result


def _last_day(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (datetime(year, month + 1, 1) - timedelta(days=1)).day


# ─── 저장 ───

def save_json(data: dict, filename: str = "schedule.json"):
    # schedule.json에서 scheduleId 제외 (파일 크기 절약)
    clean_data = json.loads(json.dumps(data))
    for date_key in clean_data.get("events", {}):
        for event in clean_data["events"][date_key]:
            event.pop("scheduleId", None)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(clean_data, f, ensure_ascii=False, indent=2)
    print(f"💾 {filename} 저장 완료")


# ─── 메인 ───

def main():
    print("🎬 Blip.kr Schedule Scraper v4 (RSC + Unit Mapping) 시작\n")

    data = scrape_schedule()

    if data and data["stats"]["total_events"] > 0:
        save_json(data)
        print(f"\n📊 저장: ./schedule.json")
        print(f"📈 갱신: {data['updated_at']}")
    else:
        print("\n❌ 데이터 수집 실패 또는 이벤트 0건")
        save_json(data or {"error": "no data", "updated_at": datetime.now().isoformat()})


if __name__ == "__main__":
    main()