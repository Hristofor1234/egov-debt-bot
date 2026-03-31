import asyncio
import logging
import random
import shutil
import time
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile

from config import BOT_TOKEN, INCOMING_DIR, OUTPUT_DIR
from storage import Storage
from excel_utils import read_people, write_results, ExcelValidationError
from egov_parser import EgovParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не указан в .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
storage = Storage(Path("data/bot.db"))

# ---------- Настройки бережной обработки ----------
MIN_DELAY_SECONDS = 8
MAX_DELAY_SECONDS = 14

BATCH_SIZE = 25
BATCH_PAUSE_SECONDS = 180

MAX_RETRIES = 3
RETRY_DELAYS = [30, 60]

MAX_CONSECUTIVE_ERRORS = 5
# --------------------------------------------------


def format_duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    if secs and not hours:
        parts.append(f"{secs} сек")

    return " ".join(parts) if parts else "0 сек"


def estimate_processing_time(total_rows: int, avg_check_duration: float | None) -> float:
    """
    Считаем оценку не на глаз:
    - берем среднее реальное время одной проверки из прошлых запусков
    - добавляем среднюю межстрочную паузу
    - добавляем batch pause
    """
    if total_rows <= 0:
        return 0.0

    # Если статистики еще нет — берем безопасную стартовую оценку
    if avg_check_duration is None:
        avg_check_duration = 16.0

    avg_human_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2

    processing_time = total_rows * avg_check_duration

    # Паузы между строками — кроме последней
    if total_rows > 1:
        processing_time += (total_rows - 1) * avg_human_delay

    # Паузы после блоков
    batch_pauses = (total_rows - 1) // BATCH_SIZE
    processing_time += batch_pauses * BATCH_PAUSE_SECONDS

    return processing_time


async def human_delay():
    delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    logger.info("Sleeping between checks: %s sec", delay)
    await asyncio.sleep(delay)


async def retry_check(parser: EgovParser, fio: str, iin: str) -> dict:
    last_result = None

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info("Check attempt %s/%s | fio=%s | iin=%s", attempt, MAX_RETRIES, fio, iin)

        result = await parser.check_person(fio, iin)
        last_result = result

        if result["check_status"] != "Ошибка проверки":
            return result

        if attempt < MAX_RETRIES:
            retry_delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            logger.warning(
                "Retry scheduled after error | fio=%s | iin=%s | delay=%s sec | error=%s",
                fio,
                iin,
                retry_delay,
                result.get("error_message", "")
            )
            await asyncio.sleep(retry_delay)

    return last_result


@dp.message(Command("start"))
async def start_handler(message: Message):
    logger.info("Command /start from user_id=%s", message.from_user.id)
    await message.answer(
        "Отправьте Excel-файл .xlsx\n\n"
        "Требования:\n"
        "- лист: input\n"
        "- столбцы: fio, iin\n\n"
        "Я верну готовый файл с листами input и result."
    )


@dp.message(Command("last"))
async def last_handler(message: Message):
    logger.info("Command /last from user_id=%s", message.from_user.id)
    row = storage.get_last_result_by_user(message.from_user.id)
    if not row:
        await message.answer("У вас пока нет обработанных файлов.")
        return

    _, original_name, _, output_path, _, _ = row

    if not output_path or not Path(output_path).exists():
        await message.answer("Последний результат не найден на диске.")
        return

    await message.answer_document(
        FSInputFile(output_path),
        caption=f"Ваш последний обработанный файл: {original_name}"
    )


@dp.message(F.document)
async def document_handler(message: Message):
    document = message.document

    if not document.file_name.lower().endswith(".xlsx"):
        await message.answer("Нужен именно файл формата .xlsx")
        return

    logger.info(
        "Received document | user_id=%s | file_name=%s",
        message.from_user.id,
        document.file_name
    )

    await message.answer("Файл получен. Проверяю структуру.")

    telegram_file = await bot.get_file(document.file_id)

    temp_path = INCOMING_DIR / f"temp_{document.file_name}"
    final_input_path = INCOMING_DIR / f"{message.from_user.id}_{document.file_name}"

    await bot.download_file(telegram_file.file_path, destination=temp_path)
    shutil.move(temp_path, final_input_path)

    logger.info("Saved input file to %s", final_input_path)

    file_id = storage.save_file_record(
        user_id=message.from_user.id,
        original_name=document.file_name,
        input_path=str(final_input_path),
    )

    try:
        people = read_people(final_input_path)
        logger.info("Excel validated successfully | rows=%s", len(people))
    except ExcelValidationError as e:
        logger.exception("Excel validation error")
        storage.mark_failed(file_id)
        await message.answer(f"Ошибка структуры файла:\n{e}")
        return
    except Exception as e:
        logger.exception("Unhandled file read error")
        storage.mark_failed(file_id)
        await message.answer(f"Не удалось прочитать файл:\n{e}")
        return

    results = []
    total = len(people)
    consecutive_errors = 0

    avg_check_duration = storage.get_recent_average_check_duration(limit=100)
    estimated_seconds = estimate_processing_time(total, avg_check_duration)

    if avg_check_duration is None:
        estimate_note = "Оценка стартовая, статистика еще не накоплена."
    else:
        estimate_note = f"Оценка рассчитана по среднему времени прошлых проверок: {avg_check_duration:.1f} сек на запись."

    await message.answer(
        f"Найдено строк для обработки: {total}\n"
        f"Примерное время ожидания: {format_duration(estimated_seconds)}\n"
        f"{estimate_note}\n"
        "Начинаю обработку."
    )

    async with EgovParser() as parser:
        for idx, person in enumerate(people, start=1):
            fio = person["fio"]
            iin = person["iin"]

            logger.info(
                "Processing row %s/%s | fio=%s | iin=%s",
                idx,
                total,
                fio,
                iin
            )

            started_at = time.perf_counter()
            result = await retry_check(parser, fio, iin)
            duration_seconds = time.perf_counter() - started_at

            storage.save_check_stat(
                fio=fio,
                iin=iin,
                duration_seconds=duration_seconds,
                status=result["check_status"],
            )

            results.append(result)

            logger.info(
                "Result | iin=%s | status=%s | travel=%s | debts=%s | amount=%s | error=%s | duration=%.2f sec",
                result["iin"],
                result["check_status"],
                result["travel_status"],
                result["debts_count"],
                result["total_amount"],
                result["error_message"],
                duration_seconds,
            )

            if result["check_status"] == "Ошибка проверки":
                consecutive_errors += 1
                logger.warning(
                    "Consecutive errors: %s/%s",
                    consecutive_errors,
                    MAX_CONSECUTIVE_ERRORS
                )
            else:
                consecutive_errors = 0

            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.error("Too many consecutive errors. Stopping processing.")
                await message.answer(
                    "Обработка остановлена: слишком много ошибок подряд.\n"
                    "Это похоже на нестабильную работу сайта или временное ограничение."
                )
                break

            if idx % 5 == 0 or idx == total:
                await message.answer(f"Обработано: {idx}/{total}")

            if idx < total and idx % BATCH_SIZE == 0:
                logger.info(
                    "Batch pause after %s records | sleeping %s sec",
                    idx,
                    BATCH_PAUSE_SECONDS
                )
                await message.answer(
                    f"Обработано {idx}/{total}. Делаю техническую паузу, чтобы не перегружать источник."
                )
                await asyncio.sleep(BATCH_PAUSE_SECONDS)
            elif idx < total:
                await human_delay()

    output_file = OUTPUT_DIR / f"result_{message.from_user.id}_{document.file_name}"

    try:
        logger.info("Writing output file to %s", output_file)
        write_results(final_input_path, output_file, results)
        storage.mark_processed(file_id, str(output_file))
    except Exception as e:
        logger.exception("Unhandled error in document_handler")
        storage.mark_failed(file_id)
        await message.answer(f"Ошибка при формировании итогового файла:\n{e}")
        return

    success_count = sum(1 for x in results if x["check_status"] == "Обработано")
    not_found_count = sum(1 for x in results if x["check_status"] == "Не найдено")
    error_count = sum(1 for x in results if x["check_status"] == "Ошибка проверки")

    logger.info(
        "Processing completed | total=%s | success=%s | not_found=%s | errors=%s",
        len(results),
        success_count,
        not_found_count,
        error_count
    )

    await message.answer(
        "Готово.\n"
        f"Всего строк: {len(results)}\n"
        f"Успешно: {success_count}\n"
        f"Не найдено: {not_found_count}\n"
        f"Ошибок: {error_count}"
    )

    await message.answer_document(
        FSInputFile(output_file),
        caption="Готовый файл с результатами"
    )


@dp.message(F.text)
async def text_handler(message: Message):
    text = (message.text or "").strip()

    if text.startswith("/"):
        return

    await message.answer(
        "Нужно закинуть Excel-файл .xlsx.\n"
        "Лист: input\n"
        "Столбцы: fio, iin"
    )


async def main():
    logger.info("Bot polling started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())