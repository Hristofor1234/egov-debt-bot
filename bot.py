import asyncio
import shutil
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile

from config import BOT_TOKEN, INCOMING_DIR, OUTPUT_DIR
from storage import Storage
from excel_utils import read_people, write_results, ExcelValidationError
from egov_parser import EgovParser


if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не указан в .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
storage = Storage(Path("data/bot.db"))


@dp.message(Command("start"))
async def start_handler(message: Message):
    await message.answer(
        "Отправьте Excel-файл .xlsx\n\n"
        "Требования:\n"
        "- лист: input\n"
        "- столбцы: fio, iin\n\n"
        "Я верну готовый файл с листами result и details."
    )


@dp.message(Command("last"))
async def last_handler(message: Message):
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

    await message.answer("Файл получен. Проверяю структуру и запускаю обработку.")

    telegram_file = await bot.get_file(document.file_id)

    temp_path = INCOMING_DIR / f"temp_{document.file_name}"
    final_input_path = INCOMING_DIR / f"{message.from_user.id}_{document.file_name}"

    await bot.download_file(telegram_file.file_path, destination=temp_path)
    shutil.move(temp_path, final_input_path)

    file_id = storage.save_file_record(
        user_id=message.from_user.id,
        original_name=document.file_name,
        input_path=str(final_input_path),
    )

    try:
        people = read_people(final_input_path)
    except ExcelValidationError as e:
        storage.mark_failed(file_id)
        await message.answer(f"Ошибка структуры файла:\n{e}")
        return
    except Exception as e:
        storage.mark_failed(file_id)
        await message.answer(f"Не удалось прочитать файл:\n{e}")
        return

    results = []
    total = len(people)

    await message.answer(f"Найдено строк для обработки: {total}")

    async with EgovParser() as parser:
        for idx, person in enumerate(people, start=1):
            result = await parser.check_person(person["fio"], person["iin"])
            results.append(result)

            # Защита от слишком частых запросов
            await asyncio.sleep(5)

            if idx % 5 == 0 or idx == total:
                await message.answer(f"Обработано: {idx}/{total}")

    output_file = OUTPUT_DIR / f"result_{message.from_user.id}_{document.file_name}"

    try:
        write_results(final_input_path, output_file, results)
        storage.mark_processed(file_id, str(output_file))
    except Exception as e:
        storage.mark_failed(file_id)
        await message.answer(f"Ошибка при формировании итогового файла:\n{e}")
        return

    success_count = sum(1 for x in results if x["check_status"] == "Обработано")
    not_found_count = sum(1 for x in results if x["check_status"] == "Не найдено")
    error_count = sum(1 for x in results if x["check_status"] == "Ошибка проверки")

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


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())