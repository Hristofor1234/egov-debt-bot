import asyncio
import logging
import random
import shutil
import time
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile

from config import (
    BOT_TOKEN,
    INCOMING_DIR,
    OUTPUT_DIR,
    DB_PATH,
    MIN_DELAY_SECONDS,
    MAX_DELAY_SECONDS,
    BATCH_SIZE,
    BATCH_PAUSE_SECONDS,
    MAX_RETRIES,
    MAX_CONSECUTIVE_ERRORS,
    MAX_INCOMING_FILES,
    MAX_OUTPUT_FILES,
    LOG_CHAT_ID,
    LOG_MESSAGE_THREAD_ID,
    ERROR_LOG_CHAT_ID,
    ERROR_LOG_MESSAGE_THREAD_ID,
)
from storage import Storage
from excel_utils import read_people, write_results, write_single_result, ExcelValidationError
from egov_parser import EgovParser
from person_utils import PersonInputError, format_single_result, parse_person_request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не указан в .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
storage = Storage(DB_PATH)

# A single token makes document processing strictly FIFO.  Telegram handlers can
# receive uploads concurrently, but only one Playwright session may query eGov.
processing_queue: asyncio.Queue[None] = asyncio.Queue(maxsize=1)

RETRY_DELAYS = [45, 90]


def cleanup_old_files(directory: Path, keep_last: int) -> None:
    if not directory.exists():
        return

    files = [p for p in directory.iterdir() if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    old_files = files[keep_last:]

    for file_path in old_files:
        try:
            file_path.unlink()
            logger.info("Deleted old file: %s", file_path)
        except Exception as e:
            logger.warning("Failed to delete old file %s: %s", file_path, e)


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
    if total_rows <= 0:
        return 0.0

    if avg_check_duration is None:
        avg_check_duration = 18.0

    avg_human_delay = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
    processing_time = total_rows * avg_check_duration

    if total_rows > 1:
        processing_time += (total_rows - 1) * avg_human_delay

    batch_pauses = (total_rows - 1) // BATCH_SIZE
    processing_time += batch_pauses * BATCH_PAUSE_SECONDS

    return processing_time


async def human_delay():
    delay = random.randint(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
    logger.info("Sleeping between checks: %s sec", delay)
    await asyncio.sleep(delay)


async def send_log(message_text: str) -> Message | None:
    if not LOG_CHAT_ID:
        return None

    try:
        return await bot.send_message(
            chat_id=LOG_CHAT_ID,
            message_thread_id=LOG_MESSAGE_THREAD_ID,
            text=message_text,
        )
    except Exception as e:
        logger.warning("Failed to send log message to LOG_CHAT_ID: %s", e)
        return None


async def update_task_log(log_message: Message | None, text: str) -> None:
    if log_message is None:
        return
    try:
        await log_message.edit_text(text)
    except Exception as error:
        logger.warning("Failed to update task log: %s", error)


async def send_error_log(message_text: str) -> None:
    """Send errors to a dedicated Telegram topic when it is configured."""
    if not ERROR_LOG_CHAT_ID:
        logger.error("Error topic is not configured | %s", message_text)
        return

    try:
        await bot.send_message(
            chat_id=ERROR_LOG_CHAT_ID,
            message_thread_id=ERROR_LOG_MESSAGE_THREAD_ID,
            text=message_text,
        )
    except Exception as error:
        logger.warning("Failed to send error log to Telegram topic: %s", error)


def format_user_info(message: Message) -> str:
    user = message.from_user
    if not user:
        return "Неизвестный пользователь"

    full_name = user.full_name or "Без имени"
    username = f"@{user.username}" if user.username else "без username"
    return f"{full_name} | {username} | id={user.id}"


async def retry_check(parser: EgovParser, fio: str, iin: str) -> dict:
    last_result = None

    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(
            "Check attempt %s/%s | fio=%s | iin=%s",
            attempt,
            MAX_RETRIES,
            fio,
            iin
        )

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


async def edit_progress(status_message: Message, text: str) -> None:
    try:
        await status_message.edit_text(text)
    except Exception as error:
        logger.debug("Failed to update progress message: %s", error)


async def progress_ticker(status_message: Message, stopped: asyncio.Event) -> None:
    """Show live progress while a single eGov request is running."""
    progress = 15
    while not stopped.is_set() and progress < 90:
        await asyncio.sleep(5)
        if stopped.is_set():
            return
        progress = min(progress + 10, 90)
        await edit_progress(
            status_message,
            f"Проверка eGov выполняется…\nПрогресс: {progress}%",
        )


@dp.message(Command("start"))
async def start_handler(message: Message):
    logger.info(
        "Command /start | chat_id=%s | chat_type=%s | user_id=%s",
        message.chat.id,
        message.chat.type,
        message.from_user.id if message.from_user else None,
    )
    await message.answer(
        "Отправьте Excel-файл .xlsx или одним сообщением ФИО и ИИН.\n\n"
        "Пример сообщения:\n"
        "Иванов Иван Иванович\n"
        "123456789012\n\n"
        "Требования:\n"
        "- бот читает только лист: input\n"
        "- обязательные столбцы: fio / фио и iin / иин\n\n"
        "Бот можно повторно кормить его же обработанным файлом — "
        "он заново прочитает только лист input и пересоздаст result."
    )


@dp.message(Command("chatid"))
async def chatid_handler(message: Message):
    logger.info(
        "CHATID DEBUG | chat_id=%s | chat_type=%s | user_id=%s | text=%s",
        message.chat.id,
        message.chat.type,
        message.from_user.id if message.from_user else None,
        message.text,
    )
    await message.answer(f"chat_id: {message.chat.id}")


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

    if not document.file_name or not document.file_name.lower().endswith(".xlsx"):
        await message.answer("Нужен именно Excel-файл формата .xlsx")
        return

    user_info = format_user_info(message)

    logger.info(
        "Received document | user=%s | chat_id=%s | file_name=%s",
        user_info,
        message.chat.id,
        document.file_name
    )

    task_log = await send_log(
        "Получен новый файл.\n"
        f"Пользователь: {user_info}\n"
        f"Chat ID: {message.chat.id}\n"
        f"Файл: {document.file_name}"
    )

    await message.answer("Файл получен. Проверяю структуру.")

    telegram_file = await bot.get_file(document.file_id)

    temp_path = INCOMING_DIR / f"temp_{document.file_name}"
    final_input_path = INCOMING_DIR / f"{message.from_user.id}_{document.file_name}"

    await bot.download_file(telegram_file.file_path, destination=temp_path)
    shutil.move(temp_path, final_input_path)

    logger.info("Saved input file to %s", final_input_path)

    cleanup_old_files(INCOMING_DIR, MAX_INCOMING_FILES)

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

        await send_error_log(
            "Ошибка валидации Excel.\n"
            f"Пользователь: {user_info}\n"
            f"Chat ID: {message.chat.id}\n"
            f"Файл: {document.file_name}\n"
            f"Ошибка: {e}"
        )
        await update_task_log(task_log, "Обработка файла завершилась ошибкой валидации.")

        await message.answer(f"Ошибка структуры файла:\n{e}")
        return
    except Exception as e:
        logger.exception("Unhandled file read error")
        storage.mark_failed(file_id)

        await send_error_log(
            "Ошибка чтения файла.\n"
            f"Пользователь: {user_info}\n"
            f"Chat ID: {message.chat.id}\n"
            f"Файл: {document.file_name}\n"
            f"Ошибка: {e}"
        )
        await update_task_log(task_log, "Обработка файла завершилась ошибкой чтения.")

        await message.answer(f"Не удалось прочитать файл:\n{e}")
        return

    total = len(people)
    avg_check_duration = storage.get_recent_average_check_duration(limit=100)
    estimated_seconds = estimate_processing_time(total, avg_check_duration)

    if avg_check_duration is None:
        estimate_note = "Оценка стартовая, статистика еще не накоплена."
    else:
        estimate_note = (
            f"Оценка рассчитана по среднему времени прошлых проверок: "
            f"{avg_check_duration:.1f} сек на запись."
        )

    await update_task_log(
        task_log,
        "Файл принят и добавлен в очередь.\n"
        f"Пользователь: {user_info}\n"
        f"Chat ID: {message.chat.id}\n"
        f"Файл: {document.file_name}\n"
        f"Строк к обработке: {total}\n"
        f"Оценка ожидания: {format_duration(estimated_seconds)}"
    )

    await message.answer(
        "Файл добавлен в очередь. Начну проверку, когда завершится предыдущая задача."
    )
    await processing_queue.get()
    await update_task_log(
        task_log,
        "Файл взят в обработку.\n"
        f"Файл: {document.file_name}\n"
        f"Строк к обработке: {total}"
    )

    results = []
    consecutive_errors = 0

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

                await send_error_log(
                    "Обработка остановлена из-за серии ошибок.\n"
                    f"Пользователь: {user_info}\n"
                    f"Chat ID: {message.chat.id}\n"
                    f"Файл: {document.file_name}\n"
                    f"Обработано строк: {len(results)} из {total}"
                )

                await message.answer(
                    "Обработка остановлена: слишком много ошибок подряд.\n"
                    "Похоже на нестабильную работу сайта или временное ограничение."
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
                    f"Обработано {idx}/{total}. Делаю техническую паузу, "
                    "чтобы не перегружать источник."
                )
                await asyncio.sleep(BATCH_PAUSE_SECONDS)
            elif idx < total:
                await human_delay()

    output_file = OUTPUT_DIR / f"result_{message.from_user.id}_{document.file_name}"

    try:
        logger.info("Writing output file to %s", output_file)
        write_results(final_input_path, output_file, results)
        storage.mark_processed(file_id, str(output_file))

        cleanup_old_files(OUTPUT_DIR, MAX_OUTPUT_FILES)

    except Exception as e:
        logger.exception("Unhandled error in document_handler")
        storage.mark_failed(file_id)

        await send_error_log(
            "Ошибка формирования итогового файла.\n"
            f"Пользователь: {user_info}\n"
            f"Chat ID: {message.chat.id}\n"
            f"Файл: {document.file_name}\n"
            f"Ошибка: {e}"
        )

        await message.answer(f"Ошибка при формировании итогового файла:\n{e}")
        await update_task_log(task_log, "Обработка файла завершилась ошибкой формирования результата.")
        processing_queue.task_done()
        processing_queue.put_nowait(None)
        return

    success_count = sum(1 for x in results if x["check_status"] == "Обработано")
    not_found_count = sum(1 for x in results if x["check_status"] == "Не найдено")
    error_count = sum(1 for x in results if x["check_status"] == "Ошибка проверки")

    if error_count:
        await send_error_log(
            "В обработанном Excel есть ошибки проверки.\n"
            f"Пользователь: {user_info}\n"
            f"Chat ID: {message.chat.id}\n"
            f"Файл: {document.file_name}\n"
            f"Ошибочных строк: {error_count} из {len(results)}"
        )

    logger.info(
        "Processing completed | total=%s | success=%s | not_found=%s | errors=%s",
        len(results),
        success_count,
        not_found_count,
        error_count
    )

    await update_task_log(
        task_log,
        "Обработка завершена.\n"
        f"Пользователь: {user_info}\n"
        f"Chat ID: {message.chat.id}\n"
        f"Файл: {document.file_name}\n"
        f"Всего строк: {len(results)}\n"
        f"Успешно: {success_count}\n"
        f"Не найдено: {not_found_count}\n"
        f"Ошибок: {error_count}\n"
        f"Выходной файл: {output_file.name}"
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
    processing_queue.task_done()
    processing_queue.put_nowait(None)


@dp.message(F.photo | F.video | F.audio | F.voice | F.sticker | F.animation | F.video_note)
async def unsupported_attachment_handler(message: Message):
    await message.answer(
        "Бот работает только с Excel-файлами .xlsx.\n"
        "Отправьте файл с листом 'input' и столбцами fio / фио и iin / иин."
    )


@dp.message(F.text)
async def text_handler(message: Message):
    text = (message.text or "").strip()

    if text.startswith("/"):
        return

    try:
        fio, iin = parse_person_request(text)
    except PersonInputError as error:
        await message.answer(
            f"{error}\n\n"
            "Отправьте ФИО и один ИИН из 12 цифр, например:\n"
            "Иванов Иван Иванович\n123456789012"
        )
        return

    task_log = await send_log(
        "Получен запрос на проверку из чата.\n"
        f"Пользователь: {format_user_info(message)}\n"
        f"Chat ID: {message.chat.id}"
    )
    status_message = await message.answer("Запрос принят.\nПрогресс: 0%")
    await processing_queue.get()
    await update_task_log(task_log, "Запрос взят в обработку.")
    await edit_progress(status_message, "Запрос взят в обработку.\nПрогресс: 10%")

    ticker_stopped = asyncio.Event()
    ticker_task = asyncio.create_task(progress_ticker(status_message, ticker_stopped))

    try:
        started_at = time.perf_counter()
        async with EgovParser() as parser:
            result = await retry_check(parser, fio, iin)
        duration_seconds = time.perf_counter() - started_at
        storage.save_check_stat(
            fio=fio,
            iin=iin,
            duration_seconds=duration_seconds,
            status=result["check_status"],
        )

        if result["check_status"] == "Ошибка проверки":
            await send_error_log(
                "Ошибка проверки запроса из чата.\n"
                f"Пользователь: {format_user_info(message)}\n"
                f"Chat ID: {message.chat.id}\n"
                f"Причина: {result.get('error_message', 'неизвестна')}"
            )
            await edit_progress(
                status_message,
                "Не получилось выполнить проверку.\n"
                "Прогресс: 100%\n\n"
                "eGov временно не ответил ожидаемым образом. Попробуйте позже.",
            )
            await update_task_log(task_log, "Запрос не обработан. Подробности отправлены в топик ошибок.")
            return

        output_file = OUTPUT_DIR / f"chat_result_{message.from_user.id}_{iin}.xlsx"
        write_single_result(output_file, result)
        cleanup_old_files(OUTPUT_DIR, MAX_OUTPUT_FILES)

        await edit_progress(
            status_message,
            "Проверка завершена.\n"
            "Прогресс: 100%\n\n"
            f"{format_single_result(result)}",
        )
        await message.answer_document(
            FSInputFile(output_file),
            caption="Excel-файл с результатом проверки",
        )
        await update_task_log(
            task_log,
            "Запрос обработан успешно."
        )
    except Exception as error:
        logger.exception("Chat check failed | fio=%s | iin=%s", fio, iin)
        await send_error_log(
            "Непредвиденная ошибка проверки запроса из чата.\n"
            f"Пользователь: {format_user_info(message)}\n"
            f"Chat ID: {message.chat.id}\n"
            f"Причина: {error}"
        )
        await edit_progress(
            status_message,
            f"Проверка не выполнена.\nПрогресс: 100%\n\nОшибка: {error}",
        )
        await update_task_log(task_log, "Запрос не обработан. Подробности отправлены в топик ошибок.")
    finally:
        ticker_stopped.set()
        ticker_task.cancel()
        try:
            await ticker_task
        except asyncio.CancelledError:
            pass
        processing_queue.task_done()
        processing_queue.put_nowait(None)


async def main():
    logger.info("Bot polling started")
    processing_queue.put_nowait(None)
    await send_log("Сервис eGov Debt Bot запущен и готов к обработке запросов.")
    try:
        await dp.start_polling(bot)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Bot stopped manually")


if __name__ == "__main__":
    asyncio.run(main())
