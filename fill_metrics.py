import os
import re
import sys
import time
import random
import getpass
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# =========================
# Config
# =========================
INPUT = "posts_raw.csv"
OUTPUT = "posts_filled.csv"
STATE_FILE = "ig_state.json"
DEBUG_DIR = "debug_pages"

HEADLESS = True
TEST_LIMIT = None  # None = full run

# Delay / anti-bot
DELAY_MIN = 6
DELAY_MAX = 12
LONG_BREAK_PROB = 0.15
LONG_BREAK_MIN = 20
LONG_BREAK_MAX = 40

# Resume controls
START_ROW = int(os.getenv("START_ROW", "1"))  # 1-based, applied to dataframe index+1 for safety
RETRY_PER_POST = int(os.getenv("RETRY_PER_POST", "2"))

# Safety / stability
MAX_CONSECUTIVE_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "5"))
SAVE_INTERVAL = int(os.getenv("SAVE_INTERVAL", "1"))  # save after each processed post

# If test mode, shorten delays
if TEST_LIMIT is not None and TEST_LIMIT <= 20:
    DELAY_MIN = 1
    DELAY_MAX = 2
    LONG_BREAK_PROB = 0.0


# =========================
# Logging / Utilities
# =========================
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    try:
        print(line, flush=True)
    except Exception:
        pass
    try:
        with open("run.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def atomic_save(df: pd.DataFrame, path: str) -> None:
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False, encoding="utf-8-sig")
    try:
        os.replace(tmp, path)
    except Exception:
        df.to_csv(path, index=False, encoding="utf-8-sig")


def human_delay(min_s: float = None, max_s: float = None) -> None:
    mn = DELAY_MIN if min_s is None else min_s
    mx = DELAY_MAX if max_s is None else max_s
    time.sleep(random.uniform(mn, mx))


def long_break() -> None:
    if random.random() < LONG_BREAK_PROB:
        t = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
        log(f"Long break: sleeping {t:.1f}s")
        time.sleep(t)


def to_int(text: str):
    if text is None:
        return None
    s = str(text).strip().replace(",", "")
    return int(s) if re.fullmatch(r"\d+", s) else None


def dump_debug(page, row_idx: int) -> None:
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        html = page.content()
        path = os.path.join(DEBUG_DIR, f"post_{row_idx}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        log(f"Saved debug page: {path}")
    except Exception as e:
        log(f"Failed to save debug page for {row_idx}: {e}")


# =========================
# Login
# =========================
def ensure_logged_in(context, page) -> None:
    page.goto("https://www.instagram.com/", timeout=60000)
    page.wait_for_timeout(2000)

    # Logged in if not on login page
    if "accounts/login" not in page.url:
        context.storage_state(path=STATE_FILE)
        log(f"Session valid (saved): {STATE_FILE}")
        return

    username = os.getenv("IG_USERNAME") or input("Instagram username: ")
    password = os.getenv("IG_PASSWORD") or getpass.getpass("Instagram password (hidden): ")

    page.goto("https://www.instagram.com/accounts/login/", timeout=60000)
    page.wait_for_selector("input[name='username']", timeout=20000)

    try:
        page.fill("input[name='username']", username)
        page.fill("input[name='password']", password)
        page.click("button[type='submit']")
    except Exception as e:
        log(f"Login form automation failed: {e}. Please login manually in the browser window.")
        input("After manual login completes, press Enter to continue...")

    # wait redirect or challenge
    for _ in range(120):
        time.sleep(1)
        if any(x in page.url for x in ["two_factor", "challenge"]):
            log(f"Additional auth required: {page.url}")
            input("Complete the challenge/2FA in the browser, then press Enter to continue...")
        if ("accounts/login" not in page.url) and ("challenge" not in page.url) and ("two_factor" not in page.url):
            break

    if "accounts/login" in page.url:
        raise RuntimeError("Login failed: still on login page")

    context.storage_state(path=STATE_FILE)
    log(f"Session saved: {STATE_FILE}")


# =========================
# Extraction (ROOT-RESTRICTED + Anti-repeat guard support)
# =========================
def _get_post_root(page):
    """
    게시물 본문 영역(root)만 좁혀서 파싱한다.
    우선순위: main article -> article -> main -> div[role=main]
    """
    candidates = [
        "main article",
        "article",
        "main",
        "div[role='main']",
    ]
    for sel in candidates:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            pass
    return None


def _extract_like_from_root(page):
    """
    root(main/article) 안에서만 좋아요를 파싱한다.
    허용 패턴:
      - span:has-text('좋아요') 주변 숫자
      - '좋아요 123'
      - '123명 좋아요'
    """
    try:
        root = _get_post_root(page)
        if root is None:
            return None

        # 1) '좋아요' 라벨 근처 span에서 숫자 잡기
        parent = root.locator("span:has-text('좋아요') >> xpath=..").first
        if parent.count() > 0:
            spans = parent.locator("span")
            for i in range(min(spans.count(), 12)):
                v = to_int(spans.nth(i).inner_text().strip())
                if v is not None:
                    return v

        # 2) root 텍스트에서 엄격 패턴만
        txt = root.inner_text(timeout=5000)

        m = re.search(r"좋아요\s*([0-9][0-9,]*)", txt)
        if m:
            return int(m.group(1).replace(",", ""))

        m = re.search(r"([0-9][0-9,]*)\s*명\s*좋아요", txt)
        if m:
            return int(m.group(1).replace(",", ""))

        return None
    except Exception:
        return None


def _extract_comment_from_root(page):
    """
    root(main/article) 안에서만 댓글을 파싱한다.
    허용 패턴:
      - '댓글 12' (예: '댓글 12개 모두 보기')
    """
    try:
        root = _get_post_root(page)
        if root is None:
            return None

        txt = root.inner_text(timeout=5000)
        m = re.search(r"댓글\s*([0-9][0-9,]*)", txt)
        if m:
            return int(m.group(1).replace(",", ""))

        return None
    except Exception:
        return None


def validate_metrics(likes, comments):
    if likes is not None and likes < 0:
        likes = None
    if comments is not None and comments < 0:
        comments = None
    if likes is not None and comments is not None and comments > likes:
        comments = None
    return likes, comments


def extract_likes_comments(page):
    # Stabilize render
    human_delay(2, 4)
    try:
        page.mouse.wheel(0, random.randint(200, 700))
        human_delay(1, 2)
    except Exception:
        pass

    # 3회 읽고 최대값 채택
    best_likes = None
    best_comments = None

    for _ in range(3):
        likes = _extract_like_from_root(page)
        comments = _extract_comment_from_root(page)

        likes, comments = validate_metrics(likes, comments)

        if likes is not None:
            best_likes = likes if best_likes is None else max(best_likes, likes)
        if comments is not None:
            best_comments = comments if best_comments is None else max(best_comments, comments)

        time.sleep(0.6)

    return best_likes, best_comments


# =========================
# Main
# =========================
def main() -> None:
    log("[fill_metrics] Starting")

    df_raw = pd.read_csv(INPUT)
    if "post_url" not in df_raw.columns:
        raise ValueError("posts_raw.csv must have a 'post_url' column")

    # Resume: merge existing output values
    if os.path.exists(OUTPUT):
        try:
            prev = pd.read_csv(OUTPUT)
            if "post_url" in prev.columns:
                out = df_raw.copy()
                for col in ("likes", "comments", "post_date", "followers_now"):
                    if col in prev.columns:
                        prev_small = prev[["post_url", col]].drop_duplicates(subset="post_url")
                        out = out.merge(prev_small, on="post_url", how="left", suffixes=("", "_prev"))
                        if col + "_prev" in out.columns:
                            out[col] = out[col].combine_first(out.pop(col + "_prev"))
                log(f"Loaded existing {OUTPUT}; resuming.")
            else:
                out = df_raw.copy()
        except Exception as e:
            log(f"Failed to read {OUTPUT}, starting fresh: {e}")
            out = df_raw.copy()
    else:
        out = df_raw.copy()

    if "likes" not in out.columns:
        out["likes"] = pd.NA
    if "comments" not in out.columns:
        out["comments"] = pd.NA

    # ✅ 숫자형 강제 변환(빈문자/공백/문자 nan으로 인한 스킵 오류 방지)
    try:
        out["likes"] = pd.to_numeric(out["likes"], errors="coerce")
        out["comments"] = pd.to_numeric(out["comments"], errors="coerce")
    except Exception:
        pass

    consecutive_errors = 0
    processed = 0

    # ✅ 도배(반복값) 방지 가드
    last_pair = None
    same_pair_streak = 0

    with sync_playwright() as p:
        launch_kwargs = {"headless": HEADLESS}
        browser = p.chromium.launch(**launch_kwargs)

        if os.path.exists(STATE_FILE):
            context = browser.new_context(storage_state=STATE_FILE)
        else:
            context = browser.new_context()

        page = context.new_page()

        # Diagnostics (log only, do not treat as fatal)
        try:
            page.on("console", lambda msg: log(f"PW_CONSOLE [{msg.type}] {msg.text}"))
            page.on("pageerror", lambda exc: log(f"PW_PAGEERROR: {exc}"))
            page.on("requestfailed", lambda req: log(f"PW_REQUESTFAILED {req.url} {req.failure}"))
            page.on("close", lambda: log("PW_PAGE_CLOSED"))
            try:
                browser.on("disconnected", lambda: log("PW_BROWSER_DISCONNECTED"))
            except Exception:
                pass
        except Exception as e:
            log(f"Failed to attach PW handlers: {e}")

        ensure_logged_in(context, page)

        def restart_context(force_relaunch: bool = False) -> bool:
            nonlocal browser, context, page
            try:
                try:
                    context.close()
                except Exception:
                    pass
                if force_relaunch:
                    try:
                        browser.close()
                    except Exception:
                        pass
                    browser = p.chromium.launch(**launch_kwargs)
                context = browser.new_context(storage_state=STATE_FILE if os.path.exists(STATE_FILE) else None)
                page = context.new_page()
                try:
                    page.on("console", lambda msg: log(f"PW_CONSOLE [{msg.type}] {msg.text}"))
                    page.on("pageerror", lambda exc: log(f"PW_PAGEERROR: {exc}"))
                    page.on("requestfailed", lambda req: log(f"PW_REQUESTFAILED {req.url} {req.failure}"))
                    page.on("close", lambda: log("PW_PAGE_CLOSED"))
                except Exception:
                    pass
                ensure_logged_in(context, page)
                log("Restarted context")
                return True
            except Exception as e:
                log(f"restart_context failed: {e}")
                return False

        # Iterate rows preserving original index
        for row_idx, url in out["post_url"].dropna().astype(str).items():
            row_number_1based = int(row_idx) + 1
            if row_number_1based < START_ROW:
                continue

            # Resume skip: only skip if both are present
            if pd.notna(out.loc[row_idx, "likes"]) and pd.notna(out.loc[row_idx, "comments"]):
                continue

            processed += 1
            if TEST_LIMIT is not None and processed > TEST_LIMIT:
                break

            log(f"[{processed}] row={row_idx} url={url}")

            likes = None
            comments = None
            ok = False

            for attempt in range(1, RETRY_PER_POST + 1):
                try:
                    page.goto(url, timeout=60000)
                    page.wait_for_load_state("domcontentloaded", timeout=60000)

                    likes, comments = extract_likes_comments(page)

                    # ✅ 반복값(도배) 감지: 같은 값이 연속으로 나오면 NA 처리 + debug dump
                    pair = (likes, comments)
                    if pair == last_pair and (likes is not None or comments is not None):
                        same_pair_streak += 1
                    else:
                        same_pair_streak = 0
                    last_pair = pair

                    if same_pair_streak >= 5:
                        log(f"Suspicious repeated pair detected {pair} streak={same_pair_streak}. Marking as NA and dumping debug.")
                        dump_debug(page, row_idx)
                        likes, comments = None, None

                    ok = True
                    consecutive_errors = 0
                    break

                except PlaywrightTimeoutError as e:
                    log(f"TIMEOUT attempt={attempt}: {e}")
                except Exception as e:
                    log(f"ERROR attempt={attempt}: {e}")

                human_delay(2, 4)

            if not ok:
                consecutive_errors += 1
                log(f"Consecutive errors: {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}")
                dump_debug(page, row_idx)

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log("Too many consecutive errors; restarting context")
                    if not restart_context(force_relaunch=False):
                        restart_context(force_relaunch=True)
                    consecutive_errors = 0

                out.loc[row_idx, "likes"] = pd.NA
                out.loc[row_idx, "comments"] = pd.NA
            else:
                out.loc[row_idx, "likes"] = likes if likes is not None else pd.NA
                out.loc[row_idx, "comments"] = comments if comments is not None else pd.NA

                # If incomplete, dump debug for inspection
                if (likes is None) or (comments is None):
                    dump_debug(page, row_idx)
                    log(f"Extraction incomplete row={row_idx}: likes={likes} comments={comments}")

            if processed % SAVE_INTERVAL == 0:
                atomic_save(out, OUTPUT)
                log(f"AUTOSAVED: {OUTPUT}")

            human_delay()
            long_break()

        try:
            atomic_save(out, OUTPUT)
            log(f"SAVED: {OUTPUT}")
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("KeyboardInterrupt: exiting")
        sys.exit(0)