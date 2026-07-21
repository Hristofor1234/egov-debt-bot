import re
from typing import Tuple


# Allows an IIN written with spaces or hyphens, but extracts exactly 12 digits.
IIN_PATTERN = re.compile(r"(?<!\d)(?:\d[\s-]*){11}\d(?!\d)")
LABEL_PATTERN = re.compile(r"(?i)\b(?:ф\.?и\.?о\.?|iin|иин)\s*[:\-]?")


class PersonInputError(ValueError):
    pass


def parse_person_request(text: str) -> Tuple[str, str]:
    """Parse a chat message containing one FIO and one 12-digit IIN."""
    matches = list(IIN_PATTERN.finditer(text))
    if not matches:
        raise PersonInputError("Не найден ИИН из 12 цифр.")
    if len(matches) > 1:
        raise PersonInputError("Укажите только один ИИН в одном сообщении.")

    iin_match = matches[0]
    iin = re.sub(r"\D", "", iin_match.group())
    fio_source = f"{text[:iin_match.start()]} {text[iin_match.end():]}"
    fio = " ".join(LABEL_PATTERN.sub("", fio_source).split()).strip(" ,;:-")

    if len(fio) < 2:
        raise PersonInputError("Не найдено ФИО. Напишите имя рядом с ИИН.")

    return fio, iin


def format_single_result(result: dict) -> str:
    lines = [
        "Результат проверки",
        f"ФИО: {result.get('fio', '-')}",
        f"ИИН: {result.get('iin', '-')}",
        f"Статус: {result.get('check_status', '-')}",
        f"Выезд: {result.get('travel_status', '-')}",
        f"Количество задолженностей: {result.get('debts_count', 0)}",
        f"Общая сумма: {result.get('total_amount', '-')}",
    ]

    details = result.get("details", [])
    if details:
        lines.append("Задолженности:")
        for index, detail in enumerate(details, start=1):
            lines.append(
                f"{index}. {detail.get('issuer', '-')} | "
                f"{detail.get('amount', '-')} | "
                f"{detail.get('start_date', '-')}"
            )

    if result.get("error_message"):
        lines.append(f"Ошибка: {result['error_message']}")

    return "\n".join(lines)
