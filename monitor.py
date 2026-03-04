"""
Мониторинг цен конкурентов — без API
Вход:  товары.csv
Выход: результаты.csv + результаты.html
"""

import re, csv, json, time, logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

INPUT_FILE    = "товары.csv"
OUTPUT_CSV    = "результаты.csv"
OUTPUT_HTML   = "результаты.html"
REQUEST_DELAY = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler("monitor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-BY,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── Утилиты ───────────────────────────────────────────────────

def to_float(text: str) -> Optional[float]:
    s = re.sub(r"[^\d,.]", "", str(text)).strip()
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        v = float(s)
        return v if 0.1 < v < 9_999_999 else None
    except ValueError:
        return None


def get_domain(url: str) -> str:
    return re.sub(r"https?://(www\.)?", "", str(url)).split("/")[0]


# ── Парсеры по сайтам ─────────────────────────────────────────

def _json_ld(soup):
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
            for item in (data if isinstance(data, list) else [data]):
                p = to_float(str(((item.get("offers") or {}).get("price") or item.get("price") or "")))
                if p: return p
        except Exception:
            pass
    return None


def _by_selectors(soup, selectors):
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            p = to_float(el.get_text().split("/")[0])
            if p: return p
    return None


def _generic(soup):
    text = soup.get_text(" ")
    matches = re.findall(r"(\d[\d\s]{0,6}[,.]?\d{0,2})\s*(?:руб|р\.|р\b|BYN|byn)", text, re.I)
    prices = sorted([p for m in matches if (p := to_float(m)) and 0.5 < p < 999_999])
    return prices[len(prices) // 2] if prices else None


def parse_21vek(soup):
    return _by_selectors(soup, [
        ".price-main__value", ".product-cost__value", ".prices-item__price",
        "[class*='price_main']", "[class*='price-main']", ".price",
    ]) or _json_ld(soup) or _generic(soup)


def parse_amd(soup):
    return _by_selectors(soup, [
        ".product-buy__price", ".price-main",
        "[class*='product-price']", "[class*='buy__price']", ".price",
    ]) or _json_ld(soup) or _generic(soup)


def parse_voltra(soup):
    return _by_selectors(soup, [
        ".product-price", ".price__value",
        "[class*='product-price']", "[class*='price_value']", ".price",
    ]) or _json_ld(soup) or _generic(soup)


def parse_7745(soup):
    return _by_selectors(soup, [
        ".product-card__price-current", ".price-value",
        "[class*='price-current']", "[class*='price__value']", ".price",
    ]) or _json_ld(soup) or _generic(soup)


def parse_tpro(soup):
    return _by_selectors(soup, [
        ".product-price", ".price",
        "[class*='product-price']", "span.price", "div.price",
    ]) or _json_ld(soup) or _generic(soup)


PARSERS = {
    "21vek.by": parse_21vek,
    "amd.by":   parse_amd,
    "voltra.by": parse_voltra,
    "7745.by":  parse_7745,
    "tpro.by":  parse_tpro,
}


def fetch_price(url, session):
    if not url.startswith("http"):
        return None, "Нет ссылки"
    try:
        r = session.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        domain = get_domain(url)
        parser = next((fn for k, fn in PARSERS.items() if k in domain), _generic)
        price = parser(soup)
        return (price, "OK") if price else (None, "Цена не найдена")
    except requests.exceptions.Timeout:
        return None, "Таймаут"
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP {e.response.status_code}"
    except Exception as e:
        return None, f"Ошибка: {str(e)[:60]}"


# ── Чтение CSV ────────────────────────────────────────────────

def read_products(path):
    products = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)
        for row in reader:
            if not any(row): continue
            art   = row[0].strip() if len(row) > 0 else ""
            name  = row[1].strip() if len(row) > 1 else ""
            price = to_float(row[2]) if len(row) > 2 else None
            urls  = [row[i].strip() for i in range(3, len(row))
                     if len(row) > i and row[i].strip().startswith("http")]
            if art and price:
                products.append({"art": art, "name": name, "our_price": price, "urls": urls})
    log.info("Считано товаров: %d", len(products))
    return products


# ── Запись CSV ────────────────────────────────────────────────

def write_csv(results, path):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["Обновлено","Артикул","Наименование","Наша цена",
                    "Сайт","Цена конкурента","Разница руб","Разница %","Статус"])
        for r in results:
            our, comp = r["our_price"], r["comp_price"]
            if comp:
                dr = round(comp - our, 2)
                dp = round((comp - our) / our * 100, 1)
                st = "Мы дешевле" if dr > 0.5 else "Мы дороже" if dr < -0.5 else "Одинаково"
            else:
                dr = dp = ""
                st = r.get("error", "Ошибка")
            domain = get_domain(r["url"]) if r["url"] != "—" else "—"
            w.writerow([now, r["art"], r["name"], our, domain, comp or "", dr, dp, st])
    log.info("CSV: %s", path)


# ── Запись HTML ───────────────────────────────────────────────

def write_html(results, path):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    cheaper   = sum(1 for r in results if r["comp_price"] and r["comp_price"] > r["our_price"])
    expensive = sum(1 for r in results if r["comp_price"] and r["comp_price"] < r["our_price"])
    errors    = sum(1 for r in results if not r["comp_price"])

    rows = []
    for r in results:
        our, comp, url = r["our_price"], r["comp_price"], r["url"]
        if comp:
            dr = round(comp - our, 2)
            dp = round((comp - our) / our * 100, 1)
            badge = ('<span class="badge cheap">✅ Мы дешевле</span>' if dr > 0.5
                     else '<span class="badge exp">🔴 Мы дороже</span>' if dr < -0.5
                     else '<span class="badge eq">➖ Одинаково</span>')
            diff  = (f'<span class="pos">+{dr} р. (+{dp}%)</span>' if dr > 0.5
                     else f'<span class="neg">{dr} р. ({dp}%)</span>' if dr < -0.5
                     else "—")
            comp_td = f"{comp:.2f} р."
        else:
            badge = f'<span class="badge err">⚠️ {r.get("error","Ошибка")}</span>'
            diff = comp_td = "—"
        domain = get_domain(url) if url != "—" else "—"
        link = f'<a href="{url}" target="_blank">{domain}</a>' if url.startswith("http") else domain
        rows.append(f"<tr><td class='art'>{r['art']}</td><td>{r['name']}</td>"
                    f"<td class='p'>{our:.2f} р.</td><td>{link}</td>"
                    f"<td class='p'>{comp_td}</td><td>{diff}</td><td>{badge}</td></tr>")

    html = f"""<!DOCTYPE html><html lang="ru"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Мониторинг цен {now}</title>
<style>
body{{font-family:Arial,sans-serif;background:#f4f6f9;margin:0;padding:20px;color:#333}}
h1{{font-size:22px;margin-bottom:4px}} .meta{{color:#888;font-size:13px;margin-bottom:20px}}
.stats{{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}}
.stat{{background:#fff;border-radius:10px;padding:14px 22px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
.stat .n{{font-size:28px;font-weight:700}} .stat .l{{font-size:12px;color:#888}}
.g{{color:#22c55e}} .r{{color:#ef4444}} .gr{{color:#94a3b8}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;
       overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
th{{background:#1e293b;color:#fff;padding:12px 14px;text-align:left;
    font-size:12px;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap}}
td{{padding:11px 14px;border-bottom:1px solid #f1f5f9;font-size:13px;vertical-align:middle}}
tr:last-child td{{border-bottom:none}} tr:hover td{{background:#f8fafc}}
.art{{color:#6366f1;font-weight:600}} .p{{font-weight:600}}
.pos{{color:#22c55e}} .neg{{color:#ef4444}}
a{{color:#3b82f6;text-decoration:none}} a:hover{{text-decoration:underline}}
.badge{{padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;white-space:nowrap}}
.cheap{{background:#dcfce7;color:#16a34a}} .exp{{background:#fee2e2;color:#dc2626}}
.eq{{background:#f1f5f9;color:#64748b}} .err{{background:#fef9c3;color:#ca8a04}}
</style></head><body>
<h1>📊 Мониторинг цен конкурентов</h1>
<div class="meta">Обновлено: {now}</div>
<div class="stats">
  <div class="stat"><div class="n g">{cheaper}</div><div class="l">Мы дешевле</div></div>
  <div class="stat"><div class="n r">{expensive}</div><div class="l">Мы дороже</div></div>
  <div class="stat"><div class="n gr">{errors}</div><div class="l">Ошибок</div></div>
  <div class="stat"><div class="n gr">{len(results)}</div><div class="l">Сравнений</div></div>
</div>
<table><thead><tr><th>Артикул</th><th>Наименование</th><th>Наша цена</th>
<th>Сайт</th><th>Цена конкурента</th><th>Разница</th><th>Статус</th></tr></thead>
<tbody>{"".join(rows)}</tbody></table>
</body></html>"""

    Path(path).write_text(html, encoding="utf-8")
    log.info("HTML: %s", path)


# ── Main ──────────────────────────────────────────────────────

def main():
    log.info("=" * 55)
    log.info("Старт  %s", datetime.now().strftime("%d.%m.%Y %H:%M"))
    log.info("=" * 55)

    if not Path(INPUT_FILE).exists():
        log.error("Файл %s не найден!", INPUT_FILE)
        return

    products = read_products(INPUT_FILE)
    if not products:
        log.error("Нет товаров в %s", INPUT_FILE)
        return

    session  = requests.Session()
    results  = []
    total    = sum(len(p["urls"]) for p in products)
    done     = 0

    for prod in products:
        if not prod["urls"]:
            results.append({**prod, "url": "—", "comp_price": None, "error": "Нет ссылок"})
            continue
        for url in prod["urls"]:
            done += 1
            log.info("[%d/%d]  %s  —  %s", done, total, prod["art"], get_domain(url))
            price, status = fetch_price(url, session)
            log.info("  %s %.2f р." % ("✓", price) if price else "  ✗ %s" % status)
            results.append({"art": prod["art"], "name": prod["name"],
                             "our_price": prod["our_price"],
                             "url": url, "comp_price": price, "error": status})
            time.sleep(REQUEST_DELAY)

    write_csv(results, OUTPUT_CSV)
    write_html(results, OUTPUT_HTML)

    c = sum(1 for r in results if r["comp_price"] and r["comp_price"] > r["our_price"])
    e = sum(1 for r in results if r["comp_price"] and r["comp_price"] < r["our_price"])
    err = sum(1 for r in results if not r["comp_price"])
    log.info("✅ Дешевле: %d  🔴 Дороже: %d  ⚠️ Ошибок: %d", c, e, err)


if __name__ == "__main__":
    main()
