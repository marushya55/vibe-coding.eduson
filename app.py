# app.py
# ===========================================
# Streamlit-приложение:
# - Собирает отзывы App Store (Apple RSS JSON) по всем странам
# - Фильтрует по дате (последние N дней)
# - Оставляет только русскоязычные отзывы (эвристика по доле кириллицы, без langdetect)
# - Делает авто-тэгирование тем (rule-based)
# - Даёт скачать CSV из интерфейса
#
# Запуск:
#   pip install -r requirements.txt
#   streamlit run app.py
# ===========================================

import re
import time
import csv
import hashlib
import random
from datetime import datetime, timezone

import requests
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta


# -----------------------------
# Расширенный список storefront/country кодов (ISO 3166-1 alpha-2)
# -----------------------------
STORE_FRONTS = [
    "ae","ag","ai","al","am","ao","ar","at","au","az",
    "bb","be","bf","bg","bh","bj","bm","bn","bo","br","bs","bt","bw","by","bz",
    "ca","cg","ch","cl","cn","co","cr","cv","cy","cz",
    "de","dk","dm","do","dz",
    "ec","ee","eg","es",
    "fi","fj","fm","fr",
    "gb","gd","ge","gh","gm","gr","gt","gy",
    "hk","hn","hr","hu",
    "id","ie","il","in","iq","is","it",
    "jm","jo","jp",
    "ke","kg","kh","kn","kr","kw","ky","kz",
    "la","lb","lc","li","lk","lr","lt","lu","lv","ly",
    "ma","md","me","mg","mk","ml","mn","mo","mr","ms","mt","mu","mv","mw","mx","my","mz",
    "na","ne","ng","ni","nl","no","np","nz",
    "om",
    "pa","pe","pg","ph","pk","pl","pt","py",
    "qa",
    "ro","rs","ru","rw",
    "sa","sb","sc","se","sg","si","sk","sl","sn","sr","st","sv","sz",
    "tc","td","th","tj","tm","tn","tr","tt","tw","tz",
    "ua","ug","us","uy","uz",
    "vc","ve","vg","vn",
    "za",
]


# -----------------------------
# Логирование в Streamlit
# -----------------------------
def ui_log(log_box: st.delta_generator.DeltaGenerator, msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    log_box.write(f"[{ts}] {msg}")


# -----------------------------
# Надёжные HTTP-запросы: retry + экспоненциальная пауза
# -----------------------------
def request_with_retry(
    session: requests.Session,
    url: str,
    params: dict | None = None,
    timeout: int = 25,
    max_retries: int = 6,
    base_sleep: float = 0.75,
    jitter: float = 0.25,
):
    # Повторяем запросы при 429/5xx и сетевых ошибках
    for attempt in range(max_retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            status = r.status_code

            if status == 200:
                return r

            if status in (429, 500, 502, 503, 504):
                sleep_s = base_sleep * (2 ** attempt) + random.random() * jitter
                time.sleep(sleep_s)
                continue

            return None

        except requests.RequestException:
            sleep_s = base_sleep * (2 ** attempt) + random.random() * jitter
            time.sleep(sleep_s)

    return None


# -----------------------------
# Извлечение app_id и дефолтной страны из URL
# -----------------------------
def extract_app_id(app_url: str) -> str:
    m = re.search(r"/id(\d+)", app_url)
    if not m:
        raise ValueError("Не удалось извлечь app_id из URL. Ожидается /idXXXXXXXXX в ссылке.")
    return m.group(1)

def extract_default_country_from_url(app_url: str) -> str:
    m = re.search(r"apps\.apple\.com/([a-z]{2})/", app_url.lower())
    return m.group(1) if m else "us"


# -----------------------------
# iTunes Lookup: проверка доступности приложения в стране + app_name
# -----------------------------
def itunes_lookup(session: requests.Session, app_id: str, country: str) -> dict | None:
    url = "https://itunes.apple.com/lookup"
    r = request_with_retry(session, url, params={"id": app_id, "country": country})
    if not r:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if data.get("resultCount", 0) < 1:
        return None
    return data

def get_app_name(session: requests.Session, app_id: str, preferred_country: str) -> str | None:
    # Сначала пробуем страну из URL, потом US как fallback
    for c in [preferred_country, "us"]:
        data = itunes_lookup(session, app_id, c)
        if data and data.get("results"):
            return data["results"][0].get("trackName")
    return None


# -----------------------------
# Apple RSS JSON endpoint: customerreviews
# -----------------------------
def build_rss_url(country: str, app_id: str, page: int) -> str:
    return f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostrecent/json"

def parse_rss_reviews(feed_json: dict) -> list[dict]:
    # Разбираем JSON в список отзывов
    feed = (feed_json or {}).get("feed", {})
    entries = feed.get("entry", [])
    if isinstance(entries, dict):
        entries = [entries]

    parsed = []
    for e in entries:
        author = ((e.get("author") or {}).get("name") or {}).get("label")
        content = ((e.get("content") or {}).get("label"))
        title = ((e.get("title") or {}).get("label"))
        rating = ((e.get("im:rating") or {}).get("label"))
        updated = ((e.get("updated") or {}).get("label"))
        rid = ((e.get("id") or {}).get("label"))

        # Если это не отзыв (служебная запись приложения) — пропускаем
        if not (author and content and title and rating and updated and rid):
            continue

        version = ((e.get("im:version") or {}).get("label"))

        parsed.append({
            "review_id": rid,
            "author_name": author,
            "title": title,
            "review_text": content,
            "rating": int(rating) if str(rating).isdigit() else None,
            "review_date_raw": updated,
            "version": version,
        })
    return parsed

def parse_iso_date(date_str: str) -> datetime | None:
    # Приводим дату к UTC для корректного сравнения с cutoff
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# -----------------------------
# RU-фильтр без langdetect:
# считаем долю кириллицы среди букв и порогом решаем "русский / не русский"
# -----------------------------
_CYR_RE = re.compile(r"[А-Яа-яЁё]")
_LETTER_RE = re.compile(r"[A-Za-zА-Яа-яЁё]")

def ru_score(text: str) -> float:
    # Возвращает долю кириллицы среди всех букв (0..1)
    t = (text or "").strip()
    if not t:
        return 0.0
    letters = _LETTER_RE.findall(t)
    if len(letters) < 12:
        # очень короткий текст не считаем надёжным
        return 0.0
    cyr = _CYR_RE.findall(t)
    return len(cyr) / max(len(letters), 1)

def is_russian_text(title: str, body: str, threshold: float = 0.55) -> bool:
    # Склеиваем title + body и проверяем долю кириллицы
    combined = f"{title or ''} {body or ''}".strip()
    return ru_score(combined) >= threshold


# -----------------------------
# Дедуп: review_id или fallback hash
# -----------------------------
def normalized_text_for_hash(text: str) -> str:
    t = (text or "").lower().replace("ё", "е")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def make_fallback_dedupe_key(author_name: str, review_date_iso: str, text: str) -> str:
    base = f"{author_name}||{review_date_iso}||{normalized_text_for_hash(text)}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# -----------------------------
# Авто-тэгирование тем (rule-based)
# -----------------------------
TOPIC_ORDER = ["onboarding", "streak", "ads", "subscription", "bugs", "motivation"]

def _normalize_for_matching(text: str) -> str:
    # Нормализация: lower + ё->е + удаление пунктуации + схлопывание пробелов
    t = (text or "").lower().replace("ё", "е")
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _compile_keyword_patterns():
    # Словари ключевых слов/фраз по темам (EN + RU + ES + PT + FR + DE)
    kw = {
        "onboarding": [
            "onboarding","tutorial","getting started","first lesson","intro lesson","sign up","signup","log in","login","register","registration",
            "первые шаги","вводный урок","обучение","подсказки","регистрация","войти","вход",
            "tutorial","primeros pasos","registro","iniciar sesion","inicio de sesion",
            "tutorial","primeiros passos","registro","iniciar sessao","login",
            "tutoriel","premiers pas","inscription","connexion",
            "tutorial","erste schritte","registrierung","anmeldung","einloggen",
        ],
        "streak": [
            "streak","daily streak","keep streak","streak freeze",
            "серия","стрик","заморозка серии","дневная серия",
            "racha","racha diaria","congelar racha",
            "sequencia","sequencia diaria","congelar sequencia",
            "serie","serie quotidienne","geler la serie",
            "serie","tagesserie","serie einfrieren",
        ],
        "ads": [
            # ad/ads — строго по границам слов, чтобы не ловить "advice"
            "ads","ad","advertising","advertisement","commercials","too many ads","banner ad","video ad","adblock",
            "реклама","баннер","ролик","видео реклама","слишком много рекламы","адблок","adblock",
            "anuncios","publicidad","demasiados anuncios",
            "anuncios","publicidade","muitos anuncios",
            "publicite","annonces","trop de publicite",
            "werbung","anzeigen","zu viel werbung",
        ],
        "subscription": [
            # Важно: "free trial" отдельно, не просто "free"
            "subscription","subscribe","premium","plus","super","payment","price","billing","trial","free trial","refund","cancel subscription",
            "подписка","подпис","оплат","платеж","цена","стоимост","пробный период","триал","возврат","отмена подписки",
            "suscripcion","suscrib","pago","precio","facturacion","prueba gratis","reembolso","cancelar suscripcion",
            "assinatura","assinar","pagamento","preco","faturamento","teste gratis","reembolso","cancelar assinatura",
            "abonnement","s abonner","paiement","prix","facturation","essai gratuit","remboursement","annuler l abonnement",
            "abo","abonnement","abonnieren","zahlung","preis","abrechnung","kostenloser test","ruckerstattung","abo kundigen",
        ],
        "bugs": [
            "bug","glitch","crash","crashes","freezes","freeze","lag","error","not working","broken",
            "баг","глюк","вылетает","краш","зависает","лагает","ошибка","не работает",
            "error","no funciona","se cierra","bloquea","falla","bug",
            "erro","nao funciona","fecha","trava","falha","bug",
            "bug","erreur","ne marche pas","plante","bloque","ralentit",
            "fehler","funktioniert nicht","sturz","absturz","hangt","ruckelt","bug",
        ],
        "motivation": [
            "motivate","motivation","habit","progress","goals","reminders","fun","addictive","encouraging",
            "вовлекает","мотивация","привычка","прогресс","цели","напоминания","интересно","затягивает",
            "motiva","motivacion","habito","progreso","metas","recordatorios","divertido","adictivo",
            "motiva","motivacao","habito","progresso","metas","lembretes","divertido","viciante",
            "motivation","motiver","habitude","progres","objectifs","rappels","amusant","addictif",
            "motivation","motiviert","gewohnheit","fortschritt","ziele","erinnerungen","macht spass","suchtig",
        ],
    }

    patterns = {}
    for topic, words in kw.items():
        compiled = []
        for w in words:
            w_norm = _normalize_for_matching(w)
            if not w_norm:
                continue

            is_latin_single = bool(re.fullmatch(r"[a-z0-9]+", w_norm))

            if topic == "ads" and w_norm in ("ad", "ads"):
                pat = re.compile(rf"\b{re.escape(w_norm)}\b", flags=re.IGNORECASE)
            elif is_latin_single:
                pat = re.compile(rf"\b{re.escape(w_norm)}\b", flags=re.IGNORECASE)
            else:
                pat = re.compile(re.escape(w_norm), flags=re.IGNORECASE)

            compiled.append(pat)

        patterns[topic] = compiled

    return patterns

TOPIC_PATTERNS = _compile_keyword_patterns()

def tag_topics(df: pd.DataFrame) -> pd.DataFrame:
    # Добавляем topic_tags и булевые topic_*
    df = df.copy()

    def match_topics(title: str, text: str) -> dict:
        norm = _normalize_for_matching(f"{title or ''} {text or ''}")
        hits = {t: 0 for t in TOPIC_ORDER}
        for topic in TOPIC_ORDER:
            for p in TOPIC_PATTERNS[topic]:
                if p.search(norm):
                    hits[topic] = 1
                    break
        return hits

    tags_col = []
    topic_cols = {f"topic_{t}": [] for t in TOPIC_ORDER}

    for _, row in df.iterrows():
        hits = match_topics(row.get("title"), row.get("review_text"))
        for t in TOPIC_ORDER:
            topic_cols[f"topic_{t}"].append(hits[t])
        tags_col.append(",".join([t for t in TOPIC_ORDER if hits[t] == 1]))

    df["topic_tags"] = tags_col
    for t in TOPIC_ORDER:
        df[f"topic_{t}"] = topic_cols[f"topic_{t}"]

    return df


# -----------------------------
# Главная функция: сбор + RU-only + тэгирование
# -----------------------------
def scrape_appstore_reviews_all_countries(
    app_url: str,
    per_country_limit: int = 50,
    days: int = 7,
    ru_threshold: float = 0.55,
    delay_between_requests_min: float = 0.25,
    delay_between_requests_max: float = 0.55,
    log_box=None,
    progress_callback=None,
):
    app_id = extract_app_id(app_url)
    default_country = extract_default_country_from_url(app_url)

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "application/json,text/plain,*/*",
    })

    app_name = get_app_name(session, app_id, preferred_country=default_country)
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - relativedelta(days=days)

    if log_box:
        ui_log(log_box, f"app_id={app_id}, default_country={default_country}, app_name={app_name}")
        ui_log(log_box, f"Cutoff (UTC) = {cutoff.isoformat()} (последние {days} дней)")
        ui_log(log_box, f"RU-фильтр: доля кириллицы ≥ {ru_threshold:.2f}")

    all_rows = []
    seen_review_ids = set()
    seen_fallback = set()

    countries = [default_country] + [c for c in STORE_FRONTS if c != default_country]
    total_countries = len(countries)

    for idx, country in enumerate(countries):
        if progress_callback:
            progress_callback((idx + 1) / total_countries, country)

        lookup = itunes_lookup(session, app_id, country)
        if not lookup:
            if log_box:
                ui_log(log_box, f"[{country}] Приложение недоступно по lookup -> пропуск")
            continue

        pages = 0
        scanned = 0
        kept_ru = 0
        filtered_old = 0
        stop_due_to_old = False
        page = 1

        time.sleep(random.uniform(delay_between_requests_min, delay_between_requests_max))

        while scanned < per_country_limit and not stop_due_to_old:
            pages += 1
            rss_url = build_rss_url(country, app_id, page)

            r = request_with_retry(session, rss_url)
            time.sleep(random.uniform(delay_between_requests_min, delay_between_requests_max))

            if not r:
                if log_box:
                    ui_log(log_box, f"[{country}] page={page}: запрос не удался -> стоп по стране")
                break

            try:
                feed_json = r.json()
            except Exception:
                if log_box:
                    ui_log(log_box, f"[{country}] page={page}: не JSON -> стоп по стране")
                break

            reviews = parse_rss_reviews(feed_json)
            if not reviews:
                if log_box:
                    ui_log(log_box, f"[{country}] page={page}: отзывов нет -> стоп по стране")
                break

            for rv in reviews:
                if scanned >= per_country_limit:
                    break

                dt = parse_iso_date(rv.get("review_date_raw"))
                if not dt:
                    continue

                if dt < cutoff:
                    filtered_old += 1
                    stop_due_to_old = True
                    break

                review_id = rv.get("review_id") or ""
                title = rv.get("title") or ""
                text = rv.get("review_text") or ""
                author = rv.get("author_name") or ""
                rating = rv.get("rating")
                version = rv.get("version")
                review_date_iso = dt.isoformat()

                # Дедуп
                if review_id:
                    if review_id in seen_review_ids:
                        continue
                else:
                    fb = make_fallback_dedupe_key(author, review_date_iso, f"{title}\n{text}")
                    if fb in seen_fallback:
                        continue
                    seen_fallback.add(fb)

                scanned += 1
                if review_id:
                    seen_review_ids.add(review_id)

                # RU-only фильтр по доле кириллицы
                if not is_russian_text(title, text, threshold=ru_threshold):
                    continue

                kept_ru += 1
                all_rows.append({
                    "app_id": app_id,
                    "app_name": app_name,
                    "country": country,
                    "review_id": review_id if review_id else None,
                    "author_name": author,
                    "rating": rating,
                    "title": title,
                    "review_text": text,
                    "review_date": review_date_iso,
                    "version": version,
                    "language": "ru",
                    "source_url": app_url,
                })

            if log_box:
                ui_log(
                    log_box,
                    f"[{country}] pages={pages}, scanned={scanned}/{per_country_limit}, kept_ru={kept_ru}, filtered_old={filtered_old} (page={page})"
                )

            page += 1

        if log_box:
            ui_log(log_box, f"[{country}] DONE: pages={pages}, scanned={scanned}, kept_ru={kept_ru}")

    df = pd.DataFrame(all_rows)

    # Гарантируем схему
    base_cols = [
        "app_id","app_name","country","review_id","author_name","rating",
        "title","review_text","review_date","version","language","source_url"
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None

    # Тэгирование
    if len(df) > 0:
        df = tag_topics(df)
    else:
        df["topic_tags"] = ""
        for t in TOPIC_ORDER:
            df[f"topic_{t}"] = 0

    # Финальный порядок колонок
    final_cols = [
        "app_id",
        "app_name",
        "country",
        "review_id",
        "author_name",
        "rating",
        "title",
        "review_text",
        "review_date",
        "version",
        "language",
        "topic_tags",
        "topic_onboarding",
        "topic_streak",
        "topic_ads",
        "topic_subscription",
        "topic_bugs",
        "topic_motivation",
        "source_url",
    ]
    df = df[final_cols]
    return df


# ===========================================
# Streamlit UI
# ===========================================
st.set_page_config(page_title="App Store Reviews (RU) + Topic Tags", layout="wide")

st.title("App Store отзывы (все страны) → только RU → тэгирование тем")
st.caption("Источник: Apple RSS JSON (customerreviews) + iTunes Lookup. Без Selenium/Playwright. Без langdetect.")

with st.sidebar:
    st.header("Параметры")
    app_url = st.text_input(
        "App Store URL",
        value="https://apps.apple.com/us/app/duolingo-language-lessons/id570060128"
    )
    per_country_limit = st.slider("Лимит на страну (сколько просматриваем)", 5, 50, 50, 5)
    days = st.slider("Период (дней назад)", 1, 30, 7, 1)
    ru_threshold = st.slider("RU-порог (доля кириллицы)", 0.30, 0.90, 0.55, 0.05)

    st.divider()
    st.write("Скорость (чтобы меньше ловить 429):")
    delay_min = st.slider("Пауза min (сек)", 0.0, 2.0, 0.25, 0.05)
    delay_max = st.slider("Пауза max (сек)", 0.0, 3.0, 0.55, 0.05)
    if delay_max < delay_min:
        st.warning("delay_max должен быть ≥ delay_min")

run_btn = st.button("🚀 Запустить сбор")

log_box = st.empty()
progress_bar = st.progress(0, text="Ожидание запуска...")
country_badge = st.empty()

def progress_cb(progress_value: float, country: str):
    progress_bar.progress(int(progress_value * 100), text=f"Сканируем страны... ({country})")
    country_badge.info(f"Текущая страна: {country}")

if run_btn:
    try:
        df = scrape_appstore_reviews_all_countries(
            app_url=app_url,
            per_country_limit=per_country_limit,
            days=days,
            ru_threshold=ru_threshold,
            delay_between_requests_min=delay_min,
            delay_between_requests_max=delay_max,
            log_box=log_box,
            progress_callback=progress_cb,
        )
        progress_bar.progress(100, text="Готово ✅")

        st.subheader("Результат")
        st.write(f"Собрано RU-отзывов: **{len(df)}**")
        st.dataframe(df, use_container_width=True)

        # Сводка по темам
        st.subheader("Сводка по темам")
        if len(df) > 0:
            exploded = df["topic_tags"].str.split(",", expand=False).explode()
            exploded = exploded[exploded.notna() & (exploded != "")]
            if len(exploded) > 0:
                counts = exploded.value_counts().reindex(TOPIC_ORDER).fillna(0).astype(int)
                st.write(counts)
            else:
                st.write("Совпадений по темам не найдено.")

            st.subheader("Средний рейтинг по темам")
            rows = []
            for t in TOPIC_ORDER:
                sub = df[df[f"topic_{t}"] == 1]
                if len(sub) == 0:
                    continue
                avg = sub["rating"].dropna().astype(float).mean()
                rows.append({"topic": t, "avg_rating": round(float(avg), 2), "n": len(sub)})
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.write("Недостаточно данных для расчёта.")

        # Скачать CSV
        out_name = f"appstore_reviews_all_countries_{extract_app_id(app_url)}_{datetime.now().strftime('%Y%m%d')}.csv"
        csv_bytes = df.to_csv(index=False, encoding="utf-8", quoting=csv.QUOTE_ALL).encode("utf-8")

        st.download_button(
            label="⬇️ Скачать CSV",
            data=csv_bytes,
            file_name=out_name,
            mime="text/csv",
        )

    except Exception as e:
        progress_bar.progress(0, text="Ошибка ❌")
        st.error(f"Ошибка: {e}")
