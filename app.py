from __future__ import annotations

from datetime import date
from html import unescape
from html.parser import HTMLParser
from io import BytesIO

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


def build_department(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет колонку Подразделение на основе ИНН."""
    result = df.copy()
    inn_clean = result["ИНН"].astype(str).str.strip()
    result["Подразделение"] = inn_clean.map(INN_TO_DEPARTMENT).fillna("Не определено")
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
        filtered = filtered[filtered["Подразделение"].isin(selected_departments)]
        add_log(f"Фильтр 'подразделение': осталось {len(filtered)} строк")

    return filtered


def format_birthday_for_display(series: pd.Series) -> pd.Series:
    """Форматирует ДР для отображения в виде дд.мм.гггг."""
    parsed = pd.to_datetime(series, dayfirst=True, errors="coerce")
    formatted = parsed.dt.strftime("%d.%m.%Y")
    # Если дату распарсить не удалось, сохраняем исходное значение.
    return formatted.where(parsed.notna(), series.astype(str))


def make_xlsx_bytes(df: pd.DataFrame) -> bytes | None:
    """Готовит Excel-файл в памяти.

    Возвращает None, если в окружении отсутствует openpyxl или произошла ошибка экспорта.
    """
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Клиенты")
    except ModuleNotFoundError as exc:
        add_log(f"Ошибка экспорта XLSX: отсутствует зависимость ({exc})")
        return None
    except Exception as exc:
        add_log(f"Ошибка экспорта XLSX: {exc}")
        return None

    output.seek(0)
    return output.getvalue()


def main() -> None:
    st.set_page_config(page_title="Фильтрация клиентской базы", layout="wide")
    st.title("Фильтрация клиентской базы")
    st.caption("Загрузите HTML-файл и настройте фильтры — таблица обновляется автоматически.")

    init_logs()

    uploaded_file = st.file_uploader("Загрузка HTML файла", type=["html", "htm"])

    if not uploaded_file:
        st.info("Ожидается загрузка HTML-файла с клиентской таблицей.")
        with st.expander("Логи"):
            st.text("\n".join(st.session_state.logs) if st.session_state.logs else "Логи пока пусты")
        return

    df = load_table(uploaded_file)
    if df.empty:
        with st.expander("Логи"):
            st.text("\n".join(st.session_state.logs) if st.session_state.logs else "Логи пока пусты")
        return

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
        departments = sorted(df["Подразделение"].dropna().unique().tolist())
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
        st.warning("Экспорт XLSX временно недоступен: в окружении не установлен openpyxl.")
    else:
        st.download_button(
            label="Скачать xlsx",
            data=xlsx_data,
            file_name="filtered_clients.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with st.expander("Логи"):
        st.text("\n".join(st.session_state.logs) if st.session_state.logs else "Логи пока пусты")


if __name__ == "__main__":
    main()
