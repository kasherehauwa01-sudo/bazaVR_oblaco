from __future__ import annotations

from datetime import date
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


def init_logs() -> None:
    """Инициализация хранилища логов в session_state."""
    if "logs" not in st.session_state:
        st.session_state.logs = []


def add_log(message: str) -> None:
    """Добавляет сообщение в лог приложения."""
    st.session_state.logs.append(message)


def load_table(file_obj) -> pd.DataFrame:
    """Читает HTML и возвращает первую таблицу с нужными колонками.

    Поддерживает fallback, если в окружении отсутствует lxml:
    пробует сначала стандартный парсер, затем bs4/html5lib.
    """
    try:
        raw_bytes = file_obj.getvalue()
        html_text = raw_bytes.decode("utf-8", errors="ignore")
    except Exception as exc:
        add_log(f"Ошибка чтения HTML: не удалось декодировать файл ({exc})")
        st.error("Не удалось прочитать содержимое HTML-файла.")
        return pd.DataFrame()

    parser_attempts: list[tuple[str, dict]] = [
        ("lxml/auto", {}),
        ("bs4", {"flavor": "bs4"}),
        ("html5lib", {"flavor": "html5lib"}),
    ]

    tables: list[pd.DataFrame] = []
    last_error: Exception | None = None
    for parser_name, parser_kwargs in parser_attempts:
        try:
            tables = pd.read_html(html_text, **parser_kwargs)
            if parser_name != "lxml/auto":
                add_log(f"Чтение HTML выполнено через fallback-парсер: {parser_name}")
            break
        except Exception as exc:
            last_error = exc
            add_log(f"Ошибка чтения HTML ({parser_name}): {exc}")

    if not tables:
        st.error("Не удалось прочитать HTML-файл. Проверьте структуру таблицы и доступные парсеры.")
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


def make_xlsx_bytes(df: pd.DataFrame) -> bytes:
    """Готовит Excel-файл в памяти."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Клиенты")
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
        selected_day_month = st.date_input(
            "Выберите дату (дд.мм)",
            value=date.today(),
            format="DD.MM.YYYY",
            disabled=not use_birthday_discount,
        )

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
    st.dataframe(display_df, use_container_width=True)

    xlsx_data = make_xlsx_bytes(display_df)
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
