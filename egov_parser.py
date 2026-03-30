import asyncio
from typing import Dict, List
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from config import HEADLESS


class EgovParser:
    """
    ВАЖНО:
    Это рабочий каркас, но селекторы надо будет подогнать под реальную страницу.
    У eGov интерфейс может меняться.
    """

    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=HEADLESS,
            slow_mo=200 if not HEADLESS else 0,
        )
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def check_person(self, fio: str, iin: str) -> Dict:
        try:
            await self.page.goto("https://egov.kz/services/SR.14/#/declaration/0//", wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)

            # -----------------------------------------------------------
            # НИЖЕ НУЖНО БУДЕТ ПОДОГНАТЬ СЕЛЕКТОРЫ ПОД РЕАЛЬНУЮ СТРАНИЦУ
            # -----------------------------------------------------------

            # Примеры возможных действий:
            # await self.page.get_by_role("button", name="Заказать услугу онлайн").click()
            # await self.page.locator("input").fill(iin)
            # await self.page.get_by_role("button", name="Далее").click()
            # await self.page.wait_for_selector("text=Результат", timeout=15000)

            # ВРЕМЕННАЯ ЗАГЛУШКА:
            await asyncio.sleep(2)

            # Тестовая логика для демонстрации потока
            if iin.endswith("0"):
                return {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Не найдено",
                    "travel_status": "",
                    "total_amount": "",
                    "debts_count": 0,
                    "error_message": "Данные не найдены",
                    "details": [],
                }

            if iin.endswith("1"):
                return {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Ошибка проверки",
                    "travel_status": "",
                    "total_amount": "",
                    "debts_count": 0,
                    "error_message": "Ошибка соединения или изменился интерфейс страницы",
                    "details": [],
                }

            return {
                "fio": fio,
                "iin": iin,
                "check_status": "Обработано",
                "travel_status": "Запрещен",
                "total_amount": "150000",
                "debts_count": 1,
                "error_message": "",
                "details": [
                    {
                        "issuer": "Пример органа",
                        "start_date": "2026-03-30",
                        "amount": "150000",
                        "executor_contact": "+7 700 000 00 00",
                    }
                ],
            }

        except PlaywrightTimeoutError:
            return {
                "fio": fio,
                "iin": iin,
                "check_status": "Ошибка проверки",
                "travel_status": "",
                "total_amount": "",
                "debts_count": 0,
                "error_message": "Таймаут ожидания страницы",
                "details": [],
            }
        except Exception as e:
            return {
                "fio": fio,
                "iin": iin,
                "check_status": "Ошибка проверки",
                "travel_status": "",
                "total_amount": "",
                "debts_count": 0,
                "error_message": str(e),
                "details": [],
            }