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
        # 헤드리스 모드에서도 JavaScript 렌더링이 제대로 되도록 설정
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        page = await browser.new_page()
        
        try:
            print("🔄 blip.kr/schedule 접속 중...")
            
            # 페이지 로드 대기 (더 긴 타임아웃)
            await page.goto(
                'https://blip.kr/schedule',
                wait_until='domcontentloaded',
                timeout=30000
            )
            
            # JavaScript 렌더링 완료 대기
            print("⏳ 페이지 렌더링 대기 중...")
            await page.wait_for_timeout(3000)
            
            # 캘린더 데이터가 로드될 때까지 대기
            try:
                await page.wait_for_selector('[role="gridcell"]', timeout=10000)
                print("✅ 캘린더 로드 완료")
            except:
                print("⚠️  캘린더 선택자 찾기 실패, 계속 진행...")
            
            # 현재 표시 중인 월/년도 추출
            month_text = await page.text_content('h2')
            print(f"📅 추출 중인 월: {month_text}")
            
            # 모든 날짜 셀에서 이벤트 추출
            schedule_data = {}
            
            # 캘린더 그리드의 모든 셀 순회
            cells = await page.query_selector_all('[role="gridcell"]')
            print(f"📍 총 {len(cells)}개 날짜 셀 발견")
            
            cell_count = 0
            event_total = 0
            
            for idx, cell in enumerate(cells):
                try:
                    # 각 셀의 텍스트 추출
                    cell_text = await cell.text_content()
                    
                    if not cell_text or not cell_text.strip():
                        continue
                    
                    # 날짜 추출 (첫 번째 숫자)
                    date_match = re.match(r'^(\d+)', cell_text.strip())
                    
                    if date_match:
                        date = int(date_match.group(1))
                        
                        # 버튼 찾기
                        button = await cell.query_selector('button')
                        if button:
                            # 버튼 내 모든 리스트 아이템 추출
                            items = await button.query_selector_all('li')
                            
                            if len(items) > 0:
                                cell_count += 1
                                events = []
                                
                                for item in items:
                                    try:
                                        event_text = await item.text_content()
                                        event_text = event_text.strip()
                                        
                                        if not event_text:
                                            continue
                                        
                                        # 카테고리 판단
                                        category = "기타"
                                        html = await item.inner_html()
                                        
                                        # 이미지 alt나 class에서 카테고리 찾기
                                        for cat in CATEGORY_MAPPING.keys():
                                            if cat in html or cat in event_text:
                                                category = cat
                                                break
                                        
                                        events.append({
                                            "title": event_text,
                                            "category": category
                                        })
                                        event_total += 1
                                    except Exception as e:
                                        print(f"  ⚠️  이벤트 추출 실패: {e}")
                                        continue
                                
                                if events:
                                    schedule_data[str(date)] = events
                except Exception as e:
                    print(f"  ⚠️  셀 {idx} 처리 실패: {e}")
                    continue
            
            print(f"📊 캘린더 추출: {cell_count}개 날짜에서 {event_total}개 이벤트 발견")
            
            # "오늘의 스케줄"과 "다가오는 스케줄"에서 상세 정보 추출
            detailed_schedule = []
            
            # 섹션에서 리스트 아이템 찾기
            sections = await page.query_selector_all('section')
            print(f"📌 총 {len(sections)}개 섹션 발견")
            
            for section_idx, section in enumerate(sections):
                try:
                    # 섹션 제목 확인
                    heading = await section.query_selector('h2, h3')
                    if heading:
                        heading_text = await heading.text_content()
                        if "스케줄" in heading_text:
                            print(f"  📋 섹션 {section_idx}: {heading_text}")
                            
                            # 해당 섹션의 리스트 아이템 추출
                            list_items = await section.query_selector_all('li')
                            print(f"    ├─ {len(list_items)}개 항목 발견")
                            
                            for item_idx, item in enumerate(list_items[:30]):  # 최대 30개
                                try:
                                    # 제목, 날짜, 아티스트명 추출
                                    item_html = await item.inner_html()
                                    item_text = await item.text_content()
                                    
                                    # generic 태그들 찾기
                                    generics = await item.query_selector_all('generic')
                                    
                                    if len(generics) >= 2:
                                        title = await generics[0].text_content() if len(generics) > 0 else ""
                                        date_info = await generics[1].text_content() if len(generics) > 1 else ""
                                        artist = await generics[2].text_content() if len(generics) > 2 else ""
                                        
                                        title = title.strip()
                                        date_info = date_info.strip()
                                        artist = artist.strip()
                                        
                                        if title and date_info:
                                            detailed_schedule.append({
                                                "title": title,
                                                "date": date_info,
                                                "artist": artist
                                            })
                                except Exception as e:
                                    continue
                except Exception as e:
                    continue
            
            print(f"📝 상세 일정: {len(detailed_schedule)}개 추출")
            
            # 결과 컴파일
            result = {
                "updated_at": datetime.now().isoformat(),
                "month": month_text.strip() if month_text else "Unknown",
                "calendar": schedule_data,
                "detailed": detailed_schedule[:50],  # 상위 50개
                "categories": list(CATEGORY_MAPPING.keys()),
                "debug": {
                    "cells_found": len(cells),
                    "cells_with_events": cell_count,
                    "total_events": event_total,
                    "detailed_count": len(detailed_schedule)
                }
            }
            
            print(f"\n✅ 스크래핑 완료!")
            print(f"   - 캘린더: {len(schedule_data)}개 날짜")
            print(f"   - 이벤트: {event_total}개")
            print(f"   - 상세 일정: {len(detailed_schedule)}개")
            
            return result
            
        except Exception as e:
            print(f"❌ 스크래핑 오류: {e}")
            import traceback
            traceback.print_exc()
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
