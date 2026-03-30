import asyncio
import logging
import shutil
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


@dp.message(Command("start"))
async def start_handler(message: Message):
    logger.info("Command /start from user_id=%s", message.from_user.id)
    await message.answer(
        "Отправьте Excel-файл .xlsx\n\n"
        "Требования:\n"
        "- лист: input\n"
        "- столбцы: fio, iin\n\n"
        "Я верну готовый файл с листами result и details."
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

    await message.answer(
        f"Найдено строк для обработки: {total}\n"
        "Начинаю обработку, пожалуйста подождите."
    )

    async with EgovParser() as parser:
        for idx, person in enumerate(people, start=1):
            logger.info(
                "Processing row %s/%s | fio=%s | iin=%s",
                idx,
                total,
                person["fio"],
                person["iin"]
            )

            result = await parser.check_person(person["fio"], person["iin"])
            results.append(result)

            logger.info(
                "Result | iin=%s | status=%s | travel=%s | debts=%s | amount=%s | error=%s",
                result["iin"],
                result["check_status"],
                result["travel_status"],
                result["debts_count"],
                result["total_amount"],
                result["error_message"],
            )

            await asyncio.sleep(3)

            if idx % 5 == 0 or idx == total:
                await message.answer(f"Обработано: {idx}/{total}")

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
        total,
        success_count,
        not_found_count,
        error_count
    )

    await message.answer(
        "Готово.\n"
        f"Всего строк: {total}\n"
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