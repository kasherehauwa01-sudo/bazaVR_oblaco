from __future__ import annotations

from datetime import date
from html import unescape
from html.parser import HTMLParser
from io import BytesIO
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile
import re
from urllib.parse import urljoin

import numpy as np
import pandas as pd
import streamlit as st


REQUIRED_COLUMNS = {"Телефон", "Почта", "ФИО", "ДР", "Пол", "SMS", "mail", "ШК", "ИНН"}
PREVIEW_COLUMNS = ["Телефон", "Почта", "ФИО", "ДР", "Пол", "SMS", "mail", "ШК", "Подразделение"]
INN_TO_DEPARTMENT = {
    "344211849524": "ИП Дегтярев А.И.",
    "344309962847": "ИП Куприянова О.В.",
    "590201650874": "ИП Пахалуева Л.Н.",
    "231113584561": "ИП Титаренко О.А.",
    "3445106455": "ООО Уютайм",
}

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

DB_CLIENTS_URL = "https://cosmos-api.ru/clients/"
DB_LOGIN = "manager"
DB_PASSWORD = "Deg5ho.0999Z"


class _SimpleHTMLTableParser(HTMLParser):
    """Простой парсер HTML-таблиц без внешних зависимостей."""

    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_table: list[list[str]] = []
        self._current_row: list[str] = []
        self._current_cell_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "table":
            self._in_table = True
            self._current_table = []
        elif self._in_table and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag in {"td", "th"}:
            self._in_cell = True
            self._current_cell_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._in_cell:
            cell_text = unescape("".join(self._current_cell_parts)).strip()
            self._current_row.append(cell_text)
            self._in_cell = False
            self._current_cell_parts = []
        elif tag == "tr" and self._in_row:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._current_row = []
            self._in_row = False
        elif tag == "table" and self._in_table:
            self.tables.append(self._current_table)
            self._current_table = []
            self._in_table = False


def _read_html_without_dependencies(html_text: str) -> list[pd.DataFrame]:
    """Читает таблицы из HTML через встроенный html.parser, если pandas.read_html недоступен."""
    parser = _SimpleHTMLTableParser()
    parser.feed(html_text)

    dataframes: list[pd.DataFrame] = []
    for raw_table in parser.tables:
        rows = [row for row in raw_table if any(str(cell).strip() for cell in row)]
        if len(rows) < 2:
            continue

        max_len = max(len(row) for row in rows)
        normalized_rows = [row + [""] * (max_len - len(row)) for row in rows]

        header = [str(col).strip() for col in normalized_rows[0]]
        body = normalized_rows[1:]
        if not body:
            continue

        df = pd.DataFrame(body, columns=header)
        dataframes.append(df)

    return dataframes


def _extract_attr(tag: str, attr_name: str) -> str | None:
    """Достаёт значение HTML-атрибута из тега через регулярное выражение."""
    pattern = rf'{attr_name}\s*=\s*["\']([^"\']+)["\']'
    match = re.search(pattern, tag, flags=re.IGNORECASE)
    return match.group(1) if match else None


def fetch_table_from_db(progress_bar, status_placeholder) -> pd.DataFrame:
    """Подключается к удалённой странице, логинится и возвращает таблицу клиентов."""
    try:
        import requests
    except Exception as exc:
        add_log(f"Ошибка подключения к БД: не удалось импортировать requests ({exc})")
        st.error("Не удалось подключиться к БД: отсутствует библиотека requests.")
        return pd.DataFrame()

    progress_bar.progress(5)
    status_placeholder.info("Шаг 1/5: открываем страницу авторизации...")

    try:
        session = requests.Session()
        response = session.get(DB_CLIENTS_URL, timeout=30)
        response.raise_for_status()
        login_html = response.text
    except Exception as exc:
        add_log(f"Ошибка подключения к БД: не удалось открыть страницу ({exc})")
        st.error("Не удалось открыть страницу БД.")
        return pd.DataFrame()

    progress_bar.progress(25)
    status_placeholder.info("Шаг 2/5: подготавливаем форму входа...")

    form_blocks = re.findall(r"<form[\s\S]*?</form>", login_html, flags=re.IGNORECASE)
    login_form = next((frm for frm in form_blocks if re.search(r'type\s*=\s*["\']password["\']', frm, flags=re.IGNORECASE)), None)
    if login_form is None and form_blocks:
        login_form = form_blocks[0]

    if login_form is None:
        add_log("Ошибка подключения к БД: не найдена форма авторизации")
        st.error("На странице не найдена форма авторизации.")
        return pd.DataFrame()

    form_tag_match = re.search(r"<form[^>]*>", login_form, flags=re.IGNORECASE)
    form_tag = form_tag_match.group(0) if form_tag_match else ""
    action = _extract_attr(form_tag, "action") or DB_CLIENTS_URL
    login_url = urljoin(DB_CLIENTS_URL, action)

    payload: dict[str, str] = {}
    username_field = None
    password_field = None

    input_tags = re.findall(r"<input[^>]*>", login_form, flags=re.IGNORECASE)
    for tag in input_tags:
        name = _extract_attr(tag, "name")
        if not name:
            continue
        input_type = (_extract_attr(tag, "type") or "text").lower()
        value = _extract_attr(tag, "value") or ""

        if input_type == "hidden":
            payload[name] = value
        if input_type == "password" and password_field is None:
            password_field = name
        if input_type in {"text", "email"} and username_field is None:
            username_field = name

    if username_field is None:
        for candidate in ("username", "login", "user", "email"):
            if candidate in payload or re.search(rf'name\s*=\s*["\']{candidate}["\']', login_form, flags=re.IGNORECASE):
                username_field = candidate
                break
    if password_field is None:
        for candidate in ("password", "pass", "passwd"):
            if candidate in payload or re.search(rf'name\s*=\s*["\']{candidate}["\']', login_form, flags=re.IGNORECASE):
                password_field = candidate
                break

    username_field = username_field or "username"
    password_field = password_field or "password"
    payload[username_field] = DB_LOGIN
    payload[password_field] = DB_PASSWORD

    progress_bar.progress(50)
    status_placeholder.info("Шаг 3/5: выполняем авторизацию...")

    try:
        auth_response = session.post(login_url, data=payload, timeout=30)
        auth_response.raise_for_status()
    except Exception as exc:
        add_log(f"Ошибка подключения к БД: не удалось выполнить вход ({exc})")
        st.error("Не удалось выполнить вход в БД.")
        return pd.DataFrame()

    progress_bar.progress(75)
    status_placeholder.info("Шаг 4/5: загружаем таблицу клиентов...")

    try:
        clients_response = session.get(DB_CLIENTS_URL, timeout=60)
        clients_response.raise_for_status()
        html_text = clients_response.text
    except Exception as exc:
        add_log(f"Ошибка подключения к БД: не удалось загрузить таблицу ({exc})")
        st.error("Не удалось загрузить страницу таблицы клиентов.")
        return pd.DataFrame()

    class _MemoryFile:
        def __init__(self, content: bytes) -> None:
            self._content = content

        def getvalue(self) -> bytes:
            return self._content

    df = load_table(_MemoryFile(html_text.encode("utf-8", errors="ignore")))
    if not df.empty:
        add_log(f"Подключение к БД успешно: загружено строк {len(df)}")

    progress_bar.progress(100)
    status_placeholder.success("Шаг 5/5: загрузка завершена")
    return df


def init_logs() -> None:
    """Инициализация хранилища логов в session_state."""
    if "logs" not in st.session_state:
        st.session_state.logs = []


def add_log(message: str) -> None:
    """Добавляет сообщение в лог приложения."""
    st.session_state.logs.append(message)


def load_table(file_obj) -> pd.DataFrame:
    """Читает HTML и возвращает первую таблицу с нужными колонками.

    Логика:
    - пробуем pandas.read_html доступными парсерами (lxml/html5lib);
    - если внешние парсеры недоступны, используем встроенный fallback-парсер;
    - выбираем первую таблицу, содержащую все обязательные колонки.
    """
    try:
        raw_bytes = file_obj.getvalue()
        html_text = raw_bytes.decode("utf-8", errors="ignore")
    except Exception as exc:
        add_log(f"Ошибка чтения HTML: не удалось декодировать файл ({exc})")
        st.error("Не удалось прочитать содержимое HTML-файла.")
        return pd.DataFrame()

    import importlib.util

    has_lxml = importlib.util.find_spec("lxml") is not None
    has_html5lib = importlib.util.find_spec("html5lib") is not None

    parser_attempts: list[tuple[str, dict]] = []
    if has_lxml:
        parser_attempts.append(("lxml", {"flavor": "lxml"}))
    if has_html5lib:
        parser_attempts.append(("bs4/html5lib", {"flavor": "bs4"}))

    tables: list[pd.DataFrame] = []
    last_error: Exception | None = None

    for parser_name, parser_kwargs in parser_attempts:
        try:
            tables = pd.read_html(html_text, **parser_kwargs)
            add_log(f"Чтение HTML выполнено парсером: {parser_name}")
            break
        except Exception as exc:
            last_error = exc
            add_log(f"Ошибка чтения HTML ({parser_name}): {exc}")

    if not tables:
        try:
            tables = _read_html_without_dependencies(html_text)
            if tables:
                add_log("Чтение HTML выполнено встроенным fallback-парсером (без внешних зависимостей)")
        except Exception as exc:
            add_log(f"Ошибка встроенного fallback-парсера HTML: {exc}")

    if not tables:
        st.error("Не удалось прочитать HTML-файл. Проверьте структуру таблицы.")
        if last_error is not None:
            add_log(f"Критическая ошибка чтения HTML: {last_error}")
        return pd.DataFrame()

    for idx, table in enumerate(tables, start=1):
        normalized = table.copy()
        normalized.columns = [str(col).strip() for col in normalized.columns]
        if REQUIRED_COLUMNS.issubset(set(normalized.columns)):
            add_log(f"Файл загружен: найдена подходящая таблица #{idx}, строк: {len(normalized)}")
            return normalized

    add_log("Ошибка чтения HTML: подходящая таблица с нужными колонками не найдена")
    st.error("В файле не найдена таблица с обязательными колонками.")
    return pd.DataFrame()


def sanitize_email_column(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает невалидные email в колонке Почта (заменяет на пустую строку)."""
    result = df.copy()

    def normalize_email(value: object) -> str:
        text = str(value).strip()
        if text.lower() in {"", "nan", "none"}:
            return ""
        return text if EMAIL_PATTERN.match(text) else ""

    before_non_empty = result["Почта"].astype(str).str.strip().replace({"nan": "", "None": ""}).ne("").sum()
    result["Почта"] = result["Почта"].apply(normalize_email)
    after_non_empty = result["Почта"].astype(str).str.strip().ne("").sum()
    cleared = int(before_non_empty - after_non_empty)
    if cleared > 0:
        add_log(f"Очистка Email: удалено невалидных значений {cleared}")
    return result


def build_department(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет колонку Подразделение на основе ИНН.

    В одной ячейке ИНН может быть несколько значений, разделённых пробелом.
    В этом случае возвращаем несколько подразделений в той же последовательности,
    разделяя их через " | ", чтобы можно было корректно фильтровать по отдельным подразделениям.
    """
    result = df.copy()

    def map_inn_to_department(inn_value: object) -> str:
        parts = [part.strip() for part in str(inn_value).split() if part.strip()]
        if not parts:
            return "Не определено"

        departments = [INN_TO_DEPARTMENT.get(part, "Не определено") for part in parts]
        # Убираем повторы, сохраняя исходный порядок.
        unique_departments = list(dict.fromkeys(departments))
        return " | ".join(unique_departments)

    result["Подразделение"] = result["ИНН"].apply(map_inn_to_department)
    return result


def birthday_window_mask(birthday_series: pd.Series, selected_day_month: date) -> pd.Series:
    """Возвращает маску для попадания дня рождения в окно ±7 дней по кругу года."""
    parsed = pd.to_datetime(birthday_series, dayfirst=True, errors="coerce")
    invalid_dates = parsed.isna().sum()
    if invalid_dates:
        add_log(f"Ошибки парсинга дат ДР: {invalid_dates} строк")

    # День года и выбранной даты считаем в високосном эталонном году,
    # чтобы корректно работать с 29 февраля.
    reference_year = 2000
    target_doy = date(reference_year, selected_day_month.month, selected_day_month.day).timetuple().tm_yday
    birth_doy = parsed.apply(
        lambda x: date(reference_year, x.month, x.day).timetuple().tm_yday if pd.notna(x) else np.nan
    )

    year_days = 366
    diff = np.abs(birth_doy - target_doy)
    min_diff = np.minimum(diff, year_days - diff)
    return (min_diff <= 7).fillna(False)


def apply_filters(
    df: pd.DataFrame,
    only_with_email: bool,
    use_birthday_discount: bool,
    selected_day_month: date,
    sms_consent: bool,
    email_consent: bool,
    selected_departments: list[str],
) -> pd.DataFrame:
    """Применяет фильтры к таблице и пишет шаги в лог."""
    filtered = df.copy()

    if only_with_email:
        filtered = filtered[filtered["Почта"].astype(str).str.strip() != ""]
        add_log(f"Фильтр 'только с Email': осталось {len(filtered)} строк")

    if use_birthday_discount:
        mask = birthday_window_mask(filtered["ДР"], selected_day_month)
        filtered = filtered[mask]
        add_log(f"Фильтр 'скидка в день рождения': осталось {len(filtered)} строк")

    if sms_consent:
        sms_mask = filtered["SMS"].astype(str).str.strip().str.lower() == "y"
        filtered = filtered[sms_mask]
        add_log(f"Фильтр 'согласен на СМС': осталось {len(filtered)} строк")

    if email_consent:
        mail_mask = filtered["mail"].astype(str).str.strip().str.lower() == "y"
        filtered = filtered[mail_mask]
        add_log(f"Фильтр 'согласен на Email': осталось {len(filtered)} строк")

    if selected_departments:
        selected_set = set(selected_departments)

        def has_selected_department(value: object) -> bool:
            parts = [part.strip() for part in str(value).split("|") if part.strip()]
            return bool(selected_set.intersection(parts))

        dep_mask = filtered["Подразделение"].apply(has_selected_department)
        filtered = filtered[dep_mask]
        add_log(f"Фильтр 'подразделение': осталось {len(filtered)} строк")

    return filtered


def format_birthday_for_display(series: pd.Series) -> pd.Series:
    """Форматирует ДР для отображения в виде дд.мм.гггг."""
    parsed = pd.to_datetime(series, dayfirst=True, errors="coerce")
    formatted = parsed.dt.strftime("%d.%m.%Y")
    # Если дату распарсить не удалось, сохраняем исходное значение.
    return formatted.where(parsed.notna(), series.astype(str))




def _build_xlsx_without_external_engines(df: pd.DataFrame) -> bytes:
    """Собирает минимальный XLSX (OOXML) без внешних библиотек."""

    def col_name(idx: int) -> str:
        name = ""
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            name = chr(65 + rem) + name
        return name

    rows_xml: list[str] = []

    # Заголовок
    header_cells = []
    for c_idx, col in enumerate(df.columns, start=1):
        ref = f"{col_name(c_idx)}1"
        header_cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(col))}</t></is></c>')
    rows_xml.append(f'<row r="1">{"".join(header_cells)}</row>')

    # Данные
    for r_idx, row in enumerate(df.itertuples(index=False, name=None), start=2):
        cells = []
        for c_idx, value in enumerate(row, start=1):
            ref = f"{col_name(c_idx)}{r_idx}"
            if pd.isna(value):
                text = ""
            else:
                text = str(value)
            cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{escape(text)}</t></is></c>')
        rows_xml.append(f'<row r="{r_idx}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        '</worksheet>'
    )

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Клиенты" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        '</Relationships>'
    )

    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        '</Relationships>'
    )

    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '</Types>'
    )

    buffer = BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    buffer.seek(0)
    return buffer.getvalue()


def make_xlsx_bytes(df: pd.DataFrame) -> bytes | None:
    """Готовит Excel-файл в памяти.

    Приоритет: openpyxl -> xlsxwriter -> встроенный генератор XLSX без зависимостей.
    """
    errors: list[str] = []

    for engine in ("openpyxl", "xlsxwriter"):
        output = BytesIO()
        try:
            with pd.ExcelWriter(output, engine=engine) as writer:
                df.to_excel(writer, index=False, sheet_name="Клиенты")
            output.seek(0)
            add_log(f"Экспорт XLSX выполнен через движок: {engine}")
            return output.getvalue()
        except ModuleNotFoundError as exc:
            errors.append(f"{engine}: {exc}")
        except Exception as exc:
            errors.append(f"{engine}: {exc}")

    try:
        data = _build_xlsx_without_external_engines(df)
        add_log("Экспорт XLSX выполнен встроенным генератором без внешних зависимостей")
        return data
    except Exception as exc:
        errors.append(f"builtin: {exc}")

    add_log("Ошибка экспорта XLSX: " + " | ".join(errors))
    return None


def main() -> None:
    st.set_page_config(page_title="Фильтрация клиентской базы", layout="wide")
    st.title("Фильтрация клиентской базы")
    st.caption("Загрузите HTML-файл и настройте фильтры — таблица обновляется автоматически.")

    init_logs()

    uploaded_file = st.file_uploader("Загрузка HTML файла", type=["html", "htm"])
    connect_db_clicked = st.button("Подключиться к БД")

    if connect_db_clicked:
        progress_bar = st.progress(0)
        status_placeholder = st.empty()
        df_from_db = fetch_table_from_db(progress_bar, status_placeholder)
        if not df_from_db.empty:
            st.session_state.df_from_db = df_from_db

    df = pd.DataFrame()
    if uploaded_file:
        df = load_table(uploaded_file)
    elif "df_from_db" in st.session_state:
        df = st.session_state.df_from_db.copy()

    if df.empty:
        st.info("Загрузите HTML-файл или нажмите 'Подключиться к БД'.")
        with st.expander("Логи"):
            st.text("\n".join(st.session_state.logs) if st.session_state.logs else "Логи пока пусты")
        return

    df = sanitize_email_column(df)
    df = build_department(df)

    st.subheader("Фильтры")
    col1, col2 = st.columns(2)

    with col1:
        only_with_email = st.checkbox("Показывать только клиентов с Email", value=False)

        use_birthday_discount = st.checkbox("Скидка в день рождения", value=False)

        months = {
            "01": "01 — Январь",
            "02": "02 — Февраль",
            "03": "03 — Март",
            "04": "04 — Апрель",
            "05": "05 — Май",
            "06": "06 — Июнь",
            "07": "07 — Июль",
            "08": "08 — Август",
            "09": "09 — Сентябрь",
            "10": "10 — Октябрь",
            "11": "11 — Ноябрь",
            "12": "12 — Декабрь",
        }

        default_month = f"{date.today().month:02d}"
        selected_month_code = st.selectbox(
            "Выберите месяц",
            options=list(months.keys()),
            index=list(months.keys()).index(default_month),
            format_func=lambda x: months[x],
            disabled=not use_birthday_discount,
        )

        # Используем високосный эталонный год, чтобы поддержать 29 февраля.
        max_day = pd.Timestamp(year=2000, month=int(selected_month_code), day=1).days_in_month
        default_day = min(date.today().day, max_day)
        selected_day = st.selectbox(
            "Выберите день",
            options=list(range(1, max_day + 1)),
            index=default_day - 1,
            format_func=lambda x: f"{x:02d}",
            disabled=not use_birthday_discount,
        )

        selected_day_month = date(2000, int(selected_month_code), int(selected_day))

        sms_consent = st.checkbox("Согласен на СМС", value=False)
        email_consent = st.checkbox("Согласен на Email", value=False)

    with col2:
        departments = list(INN_TO_DEPARTMENT.values())
        selected_departments = st.multiselect("Выбор подразделения", options=departments, default=departments)

    filtered_df = apply_filters(
        df=df,
        only_with_email=only_with_email,
        use_birthday_discount=use_birthday_discount,
        selected_day_month=selected_day_month,
        sms_consent=sms_consent,
        email_consent=email_consent,
        selected_departments=selected_departments,
    )

    st.markdown(f"**Общее количество строк:** {len(df)}")
    st.markdown(f"**Количество строк после фильтров:** {len(filtered_df)}")

    display_df = filtered_df.reindex(columns=PREVIEW_COLUMNS)
    if "ДР" in display_df.columns:
        display_df["ДР"] = format_birthday_for_display(display_df["ДР"])

    st.dataframe(display_df, use_container_width=True)

    xlsx_data = make_xlsx_bytes(display_df)
    if xlsx_data is None:
        st.warning("Экспорт XLSX временно недоступен из-за ошибки генерации файла.")
    else:
        st.download_button(
            label="скачать xlsx",
            data=xlsx_data,
            file_name="filtered_clients.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with st.expander("Логи"):
        st.text("\n".join(st.session_state.logs) if st.session_state.logs else "Логи пока пусты")


if __name__ == "__main__":
    main()
