"""
Мониторинг цен конкурентов — без API
Вход:  товары.csv
Выход: результаты.csv + результаты.html  (широкий формат — 1 строка на товар)
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
    "Accept-Language": "ru-BY,ru;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
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
    # amd.by: JSON-LD надёжнее всего (type=Product, offers.price)
    p = _json_ld(soup)
    if p: return p
    # fallback: span#priceVal или p.new-price
    for sel in ("span#priceVal", "p.new-price"):
        el = soup.select_one(sel)
        if el:
            p = to_float(el.get_text())
            if p: return p
    return _generic(soup)


def parse_voltra(soup):
    return _by_selectors(soup, [
        ".product-price", ".price__value",
        "[class*='product-price']", "[class*='price_value']", ".price",
    ]) or _json_ld(soup) or _generic(soup)


def parse_7745(soup):
    # 7745.by: несколько вариантов в зависимости от версии HTML
    # 1. meta itemprop="price"
    el = soup.select_one("meta[itemprop='price']")
    if el:
        p = to_float(el.get("content", ""))
        if p: return p
    # 2. скрытый input с полной ценой
    el = soup.select_one("#creditPriceFull")
    if el:
        p = to_float(el.get("value", ""))
        if p: return p
    # 3. JSON внутри скрытого input bestCreditOffers — ищем priceFull
    el = soup.select_one("#bestCreditOffers")
    if el:
        try:
            import json as _json
            data = _json.loads(el.get("value", "{}"))
            # берём первый priceFull
            for months in data.values():
                for pct in months.values():
                    pf = to_float(str(pct.get("priceFull", "")))
                    if pf: return pf
        except Exception:
            pass
    # 4. обычные текстовые селекторы
    return _by_selectors(soup, [
        ".product__price-current", ".product__price",
        "[class*='price-current']",
    ]) or _json_ld(soup) or _generic(soup)


def parse_tpro(soup):
    # tpro.by (Bitrix): span.priceVal — первый активный (не закомментированный)
    el = soup.select_one("span.priceVal")
    if el:
        p = to_float(el.get_text())
        if p: return p
    return _by_selectors(soup, [
        ".priceContainer span", "[class*='priceVal']",
        "a.price span", ".price",
    ]) or _json_ld(soup) or _generic(soup)


PARSERS = {
    "21vek.by":  parse_21vek,
    "amd.by":    parse_amd,
    "voltra.by": parse_voltra,
    "7745.by":   parse_7745,
    "tpro.by":   parse_tpro,
}


def _warm_up(session, base_url):
    """Посещаем главную страницу сайта, чтобы получить куки (обход 403)."""
    try:
        session.get(base_url, timeout=15, headers=HEADERS)
        time.sleep(1.0)
    except Exception:
        pass


# Кэш: сайты, для которых уже сделан warm-up в этом запуске
_warmed: set = set()


def fetch_price(url, session):
    if not url.startswith("http"):
        return None, "Нет ссылки"
    try:
        domain = get_domain(url)

        # Для amd.by и 7745.by делаем warm-up один раз за запуск
        if domain not in _warmed and any(d in domain for d in ("amd.by", "7745.by")):
            base = re.match(r"(https?://[^/]+)", url).group(1)
            log.info("Warm-up: %s", base)
            _warm_up(session, base)
            _warmed.add(domain)

        r = session.get(url, timeout=25, headers=HEADERS)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        parser = next((fn for k, fn in PARSERS.items() if k in domain), _generic)
        price = parser(soup)
        if price:
            return price, "OK"
        # Сохраняем HTML для отладки если цена не найдена
        debug_file = f"debug_{domain.replace('.', '_')}.html"
        try:
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(r.text)
            log.warning("Цена не найдена на %s — HTML сохранён в %s", domain, debug_file)
        except Exception:
            pass
        return None, "Цена не найдена"
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


# ── Группировка: плоский список → широкий (1 строка = 1 товар) ─

def group_results(results):
    """Возвращает (список товаров, упорядоченные домены)"""
    all_domains, seen = [], set()
    for r in results:
        d = get_domain(r["url"]) if r["url"] != "—" else None
        if d and d not in seen:
            seen.add(d)
            all_domains.append(d)

    products, order = {}, []
    for r in results:
        key = r["art"]
        if key not in products:
            products[key] = {"art": r["art"], "name": r["name"],
                              "our_price": r["our_price"], "competitors": {}}
            order.append(key)
        d = get_domain(r["url"]) if r["url"] != "—" else None
        if d:
            products[key]["competitors"][d] = {
                "price": r["comp_price"],
                "url":   r["url"],
                "error": r.get("error", ""),
            }
    return [products[k] for k in order], all_domains


# ── Запись CSV (широкий формат) ───────────────────────────────

def write_csv(results, path):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    prods, domains = group_results(results)

    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        header = ["Обновлено", "Артикул", "Наименование", "Наша цена"]
        for d in domains:
            header += [d, f"{d} разн.р."]
        w.writerow(header)

        for p in prods:
            row = [now, p["art"], p["name"], p["our_price"]]
            for d in domains:
                ci = p["competitors"].get(d)
                if ci and ci["price"]:
                    dr = round(ci["price"] - p["our_price"], 2)
                    row += [ci["price"], dr]
                else:
                    row += [ci["error"] if ci else "—", ""]
            w.writerow(row)

    log.info("CSV: %s", path)


# ── Запись HTML (широкий формат) ──────────────────────────────

def write_html(results, path):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    prods, domains = group_results(results)

    cheaper = expensive = errors = 0
    for p in prods:
        for ci in p["competitors"].values():
            if ci["price"]:
                if ci["price"] > p["our_price"]: cheaper += 1
                elif ci["price"] < p["our_price"]: expensive += 1
            else:
                errors += 1

    th_comps = "".join(
        f'<th><a href="https://{d}" target="_blank">{d}</a></th>'
        for d in domains
    )

    rows = []
    for p in prods:
        our = p["our_price"]
        cells = []
        for d in domains:
            ci = p["competitors"].get(d)
            if not ci:
                cells.append("<td class='na'>—</td>")
                continue
            price = ci["price"]
            url   = ci["url"]
            if price:
                dr = round(price - our, 2)
                if dr > 0.5:
                    cls  = "cheap"
                    diff = f'<span class="dpos">▲ +{dr} р.</span>'
                elif dr < -0.5:
                    cls  = "exp"
                    diff = f'<span class="dneg">▼ {dr} р.</span>'
                else:
                    cls  = "eq"
                    diff = '<span class="deq">= одинаково</span>'
                cells.append(
                    f'<td class="{cls}">'
                    f'<a href="{url}" target="_blank">{price:.2f} р.</a>'
                    f'<br>{diff}</td>'
                )
            else:
                err = ci.get("error", "Ошибка")
                cells.append(f'<td class="na"><small>⚠️ {err}</small></td>')

        rows.append(
            "<tr>"
            f"<td class='art'>{p['art']}</td>"
            f"<td class='name'>{p['name']}</td>"
            f"<td class='ours'>{our:.2f} р.</td>"
            + "".join(cells)
            + "</tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Мониторинг цен {now}</title>
<style>
*{{box-sizing:border-box}}
body{{font-family:Arial,sans-serif;background:#f4f6f9;margin:0;padding:20px;color:#333}}
h1{{font-size:22px;margin-bottom:4px}}
.meta{{color:#888;font-size:13px;margin-bottom:20px}}
.stats{{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}}
.stat{{background:#fff;border-radius:10px;padding:14px 22px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
.stat .n{{font-size:28px;font-weight:700}} .stat .l{{font-size:12px;color:#888}}
.g{{color:#22c55e}} .r{{color:#ef4444}} .gr{{color:#94a3b8}}
.wrap{{overflow-x:auto;border-radius:10px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
table{{border-collapse:collapse;background:#fff;width:100%;white-space:nowrap}}
th{{background:#1e293b;color:#fff;padding:10px 16px;text-align:center;
    font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-right:1px solid #334}}
th:nth-child(1),th:nth-child(2),th:nth-child(3){{text-align:left}}
th a{{color:#93c5fd;text-decoration:none}} th a:hover{{text-decoration:underline}}
td{{padding:10px 16px;border-bottom:1px solid #f1f5f9;border-right:1px solid #f1f5f9;
    font-size:13px;vertical-align:middle;text-align:center}}
td:nth-child(1),td:nth-child(2),td:nth-child(3){{text-align:left}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:#f8fafc}}
.art{{color:#6366f1;font-weight:700;font-size:12px}}
.name{{max-width:200px;white-space:normal;line-height:1.4;font-size:12px}}
.ours{{font-weight:700;color:#1e293b;white-space:nowrap}}
a{{color:#3b82f6;text-decoration:none;font-weight:600}}
a:hover{{text-decoration:underline}}
.dpos{{display:block;font-size:10px;color:#16a34a;margin-top:2px}}
.dneg{{display:block;font-size:10px;color:#dc2626;margin-top:2px}}
.deq{{display:block;font-size:10px;color:#94a3b8;margin-top:2px}}
.cheap{{background:#f0fdf4}} .cheap a{{color:#16a34a}}
.exp{{background:#fff1f2}} .exp a{{color:#dc2626}}
.eq{{background:#fafafa}}
.na{{color:#cbd5e1;font-size:11px}}
</style></head><body>
<h1>📊 Мониторинг цен конкурентов</h1>
<div class="meta">Обновлено: {now}</div>
<div class="stats">
  <div class="stat"><div class="n g">{cheaper}</div><div class="l">Мы дешевле</div></div>
  <div class="stat"><div class="n r">{expensive}</div><div class="l">Мы дороже</div></div>
  <div class="stat"><div class="n gr">{errors}</div><div class="l">Ошибок</div></div>
  <div class="stat"><div class="n gr">{len(prods)}</div><div class="l">Товаров</div></div>
</div>
<div class="wrap">
<table>
  <thead><tr>
    <th>Артикул</th><th>Наименование</th><th>Наша цена</th>{th_comps}
  </tr></thead>
  <tbody>{"".join(rows)}</tbody>
</table>
</div>
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

    session = requests.Session()
    results = []
    total   = sum(len(p["urls"]) for p in products)
    done    = 0

    for prod in products:
        if not prod["urls"]:
            results.append({**prod, "url": "—", "comp_price": None, "error": "Нет ссылок"})
            continue
        for url in prod["urls"]:
            done += 1
            log.info("[%d/%d]  %s  —  %s", done, total, prod["art"], get_domain(url))
            price, status = fetch_price(url, session)
            if price:
                log.info("  ✓ %.2f р.", price)
            else:
                log.warning("  ✗ %s", status)
            results.append({"art": prod["art"], "name": prod["name"],
                             "our_price": prod["our_price"],
                             "url": url, "comp_price": price, "error": status})
            time.sleep(REQUEST_DELAY)

    write_csv(results, OUTPUT_CSV)
    write_html(results, OUTPUT_HTML)

    c   = sum(1 for r in results if r["comp_price"] and r["comp_price"] > r["our_price"])
    e   = sum(1 for r in results if r["comp_price"] and r["comp_price"] < r["our_price"])
    err = sum(1 for r in results if not r["comp_price"])
    log.info("✅ Дешевле: %d  🔴 Дороже: %d  ⚠️ Ошибок: %d", c, e, err)


if __name__ == "__main__":
    main()
