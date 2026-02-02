#!/usr/bin/env python3
"""
Blip.kr K-POP Schedule Scraper
매일 blip.kr의 케이팝 스케줄을 수집하여 schedule.json으로 저장
"""

import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
import asyncio

# 일정 카테고리 매핑 (색상 코드)
CATEGORY_MAPPING = {
    "축하": "#4ECDC4",      # 청록색
    "발매": "#FF6B6B",      # 빨강
    "방송": "#FFE66D",      # 노랑
    "구매": "#95E1D3",      # 연두
    "행사": "#C7CEEA",      # 보라
    "기타": "#999999",      # 회색
    "비공식": "#FFB6B9",    # 핑크
    "SNS": "#8EC5FC"        # 파랑
}

async def scrape_blip_schedule():
    """
    blip.kr/schedule에서 캘린더 데이터를 스크래핑
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("🔄 blip.kr/schedule 접속 중...")
            await page.goto('https://blip.kr/schedule', wait_until='networkidle')
            await page.wait_for_timeout(2000)  # 동적 콘텐츠 로딩 대기
            
            # 현재 표시 중인 월/년도 추출
            month_text = await page.text_content('h2')
            print(f"📅 추출 중인 월: {month_text}")
            
            # 모든 날짜 셀에서 이벤트 추출
            schedule_data = {}
            
            # 캘린더 그리드의 모든 셀 순회
            cells = await page.query_selector_all('[role="gridcell"]')
            print(f"📍 총 {len(cells)}개 날짜 셀 발견")
            
            for cell in cells:
                # 각 셀의 버튼 찾기
                button = await cell.query_selector('button')
                if button:
                    # 버튼 내 모든 리스트 아이템 추출
                    items = await button.query_selector_all('li')
                    
                    if len(items) > 0:
                        # 날짜 텍스트 추출
                        cell_text = await cell.text_content()
                        date_match = re.match(r'^(\d+)', cell_text)
                        
                        if date_match:
                            date = int(date_match.group(1))
                            events = []
                            
                            for item in items:
                                event_text = await item.text_content()
                                # 이벤트 카테고리 판단 (앞의 아이콘/텍스트는 제외)
                                event_text = event_text.strip()
                                
                                # 카테고리 파악 (비공식 아이콘이 있으면 "비공식")
                                category = "기타"
                                for cat in CATEGORY_MAPPING.keys():
                                    if cat in event_text or cat in await item.inner_html():
                                        category = cat
                                        break
                                
                                events.append({
                                    "title": event_text,
                                    "category": category
                                })
                            
                            if events:
                                schedule_data[str(date)] = events
            
            # "오늘의 스케줄"과 "다가오는 스케줄"에서 상세 정보 추출
            detailed_schedule = []
            
            # 리스트 아이템 순회 (제목, 날짜, 아티스트 정보)
            list_items = await page.query_selector_all('li[class*="schedule"]')
            
            if not list_items:
                # 대체: 모든 리스트 아이템 중에서 날짜와 시간 정보가 있는 것 찾기
                all_lists = await page.query_selector_all('section:has(h2) li')
                list_items = all_lists
            
            for idx, item in enumerate(list_items[:50]):  # 최대 50개 항목
                try:
                    # 제목, 날짜, 아티스트명 추출
                    item_text = await item.text_content()
                    generics = await item.query_selector_all('generic')
                    
                    if len(generics) >= 2:
                        title = await generics[0].text_content() if len(generics) > 0 else ""
                        date_info = await generics[1].text_content() if len(generics) > 1 else ""
                        artist = await generics[2].text_content() if len(generics) > 2 else ""
                        
                        if title and date_info:
                            detailed_schedule.append({
                                "title": title.strip(),
                                "date": date_info.strip(),
                                "artist": artist.strip()
                            })
                except Exception as e:
                    print(f"⚠️  항목 {idx} 추출 실패: {e}")
                    continue
            
            # 결과 컴파일
            result = {
                "updated_at": datetime.now().isoformat(),
                "month": month_text.strip() if month_text else "Unknown",
                "calendar": schedule_data,
                "detailed": detailed_schedule[:30],  # 상위 30개만
                "categories": list(CATEGORY_MAPPING.keys())
            }
            
            print(f"✅ 총 {len(schedule_data)}개 날짜, {len(detailed_schedule)}개 상세 일정 추출 완료")
            
            return result
            
        except Exception as e:
            print(f"❌ 스크래핑 오류: {e}")
            return None
        finally:
            await browser.close()


def save_schedule_json(data, filename='schedule.json'):
    """
    추출된 데이터를 JSON 파일로 저장
    """
    if data:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"💾 {filename}에 저장 완료")
        return True
    return False


async def main():
    """
    메인 실행 함수
    """
    print("🎬 Blip.kr Schedule Scraper 시작\n")
    
    # 1. 스크래핑 실행
    schedule_data = await scrape_blip_schedule()
    
    # 2. JSON으로 저장
    if schedule_data:
        save_schedule_json(schedule_data)
        print(f"\n📊 저장 위치: ./schedule.json")
        print(f"📈 갱신 시간: {schedule_data['updated_at']}")
    else:
        print("\n❌ 데이터 수집 실패")


if __name__ == "__main__":
    asyncio.run(main())
