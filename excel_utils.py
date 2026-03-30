from pathlib import Path
from typing import List, Dict
from openpyxl import load_workbook


INPUT_SHEET = "input"
RESULT_SHEET = "result"
DETAILS_SHEET = "details"


class ExcelValidationError(Exception):
    pass


def _normalize(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_header(value) -> str:
    return _normalize(value).lower()


def read_people(file_path: Path) -> List[Dict]:
    wb = load_workbook(file_path)
    if INPUT_SHEET not in wb.sheetnames:
        raise ExcelValidationError(f"Лист '{INPUT_SHEET}' не найден")

    ws = wb[INPUT_SHEET]
    headers = [_normalize_header(cell.value) for cell in ws[1]]

    required = ["fio", "iin"]
    for col in required:
        if col not in headers:
            raise ExcelValidationError(f"Нет обязательного столбца: {col}")

    header_map = {header: idx + 1 for idx, header in enumerate(headers)}
    people = []
    seen_iins = set()

    for row_idx in range(2, ws.max_row + 1):
        fio = _normalize(ws.cell(row_idx, header_map["fio"]).value)
        iin = _normalize(ws.cell(row_idx, header_map["iin"]).value)

        if not fio and not iin:
            continue

        if not fio:
            raise ExcelValidationError(f"Пустое ФИО в строке {row_idx}")
        if not iin:
            raise ExcelValidationError(f"Пустой ИИН в строке {row_idx}")
        if not iin.isdigit() or len(iin) != 12:
            raise ExcelValidationError(f"Некорректный ИИН в строке {row_idx}: {iin}")
        if iin in seen_iins:
            raise ExcelValidationError(f"Дубликат ИИН в строке {row_idx}: {iin}")

        seen_iins.add(iin)

        people.append({
            "row_number": row_idx,
            "fio": fio,
            "iin": iin,
        })

    if not people:
        raise ExcelValidationError("Нет строк для обработки")

    return people


def write_results(source_file: Path, output_file: Path, results: List[Dict]) -> None:
    wb = load_workbook(source_file)

    if RESULT_SHEET in wb.sheetnames:
        del wb[RESULT_SHEET]
    if DETAILS_SHEET in wb.sheetnames:
        del wb[DETAILS_SHEET]

    result_ws = wb.create_sheet(RESULT_SHEET)
    details_ws = wb.create_sheet(DETAILS_SHEET)

    result_ws.append([
        "fio",
        "iin",
        "check_status",
        "travel_status",
        "total_amount",
        "debts_count",
        "error_message",
    ])

    details_ws.append([
        "fio",
        "iin",
        "issuer",
        "start_date",
        "amount",
        "executor_contact",
    ])

    for result in results:
        result_ws.append([
            result.get("fio", ""),
            result.get("iin", ""),
            result.get("check_status", ""),
            result.get("travel_status", ""),
            result.get("total_amount", ""),
            result.get("debts_count", 0),
            result.get("error_message", ""),
        ])

        for detail in result.get("details", []):
            details_ws.append([
                result.get("fio", ""),
                result.get("iin", ""),
                detail.get("issuer", ""),
                detail.get("start_date", ""),
                detail.get("amount", ""),
                detail.get("executor_contact", ""),
            ])

    wb.save(output_file)