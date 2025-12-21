import pandas as pd
import re
import time
from playwright.sync_api import sync_playwright

INPUT = "posts_raw.csv"
OUTPUT = "posts_filled.csv"
DELAY = 6  # 너무 줄이면 차단/오류 확률↑

def extract_numbers(text: str):
    def grab(patterns):
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                return int(m.group(1).replace(",", ""))
        return None

    likes = grab([
        r"([\d,]+)\s*likes",
        r"좋아요\s*([\d,]+)"
    ])

    comments = grab([
        r"View all\s*([\d,]+)\s*comments",
        r"([\d,]+)\s*comments",
        r"댓글\s*([\d,]+)"
    ])

    return likes, comments

def extract_date(text: str):
    m = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", text)
    return m.group(0) if m else None

def main():
    print("START fill_metrics")

    df = pd.read_csv(INPUT)
    print("Columns:", list(df.columns))
    if "post_url" not in df.columns:
        raise ValueError("posts_raw.csv에 'post_url' 컬럼이 없어. 컬럼명을 post_url로 맞춰줘…")

    urls = df["post_url"].dropna().astype(str).tolist()[:5]  # ✅ 테스트: 처음 5개만
    print(f"Will test {len(urls)} urls")

    likes_list, comments_list, dates_list = [], [], []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # 창 보여주기(안심용)
        page = browser.new_page()

        for i, url in enumerate(urls, start=1):
            print(f"[{i}/{len(urls)}] {url}")
            try:
                page.goto(url, timeout=60000)
                page.wait_for_timeout(4000)
                body = page.inner_text("body")

                likes, comments = extract_numbers(body)
                dt = extract_date(body)

                likes_list.append(likes)
                comments_list.append(comments)
                dates_list.append(dt)

                time.sleep(DELAY)
            except Exception as e:
                print("ERROR:", e)
                likes_list.append(None)
                comments_list.append(None)
                dates_list.append(None)
                time.sleep(DELAY)

        browser.close()

    out = df.copy()
    out["likes"] = pd.NA
    out["comments"] = pd.NA
    out["post_date"] = pd.NA

    # 테스트한 5개만 채우기
    for idx in range(len(urls)):
        out.loc[idx, "likes"] = likes_list[idx]
        out.loc[idx, "comments"] = comments_list[idx]
        out.loc[idx, "post_date"] = dates_list[idx]

    out.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
    print("SAVED:", OUTPUT)

if __name__ == "__main__":
    main()