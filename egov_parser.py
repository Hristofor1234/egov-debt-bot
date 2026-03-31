import logging
from pathlib import Path
from typing import Dict, List
from playwright.async_api import async_playwright
from config import HEADLESS, DATA_DIR
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

DEBUG_DIR = DATA_DIR / "debug"
DEBUG_DIR.mkdir(parents=True, exist_ok=True)


class EgovParser:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None
        self.start_url = "https://egov.kz/services/SR.14/#/declaration/0//"

    async def __aenter__(self):
        logger.info("Starting Playwright browser | headless=%s", HEADLESS)
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=HEADLESS,
            slow_mo=300 if not HEADLESS else 0,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1440, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )
        self.page = await self.context.new_page()
        self.page.set_default_timeout(30000)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        logger.info("Closing Playwright browser")
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def _save_debug(self, prefix: str):
        try:
            screenshot_path = DEBUG_DIR / f"{prefix}.png"
            html_path = DEBUG_DIR / f"{prefix}.html"

            await self.page.screenshot(path=str(screenshot_path), full_page=True)
            html = await self.page.content()
            html_path.write_text(html, encoding="utf-8")

            logger.info("Saved debug screenshot: %s", screenshot_path)
            logger.info("Saved debug html: %s", html_path)
        except Exception as e:
            logger.warning("Failed to save debug artifacts: %s", e)

    async def _open_service(self):
        logger.info("Opening eGov service page")
        await self.page.goto(
            self.start_url,
            wait_until="domcontentloaded",
            timeout=60000,
        )
        await self.page.wait_for_timeout(4000)

        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            logger.info("networkidle not reached, continuing")

        possible_selectors = [
            "input[ng-model='viewModel.inputModel']",
            "input[maxlength='12']",
            "div#input input[type='text']",
            "input.input-type.monospace",
        ]

        found = False
        for selector in possible_selectors:
            count = await self.page.locator(selector).count()
            logger.info("Selector check | %s | count=%s", selector, count)
            if count > 0:
                await self.page.locator(selector).first.wait_for(state="visible", timeout=10000)
                found = True
                break

        if not found:
            await self._save_debug("open_service_failed")
            raise RuntimeError("Не найдено поле ввода ИИН на странице")

    async def _go_to_start_page(self):
        logger.info("Returning to start page")
        await self._open_service()

    async def _get_input_locator(self):
        selectors = [
            "input[ng-model='viewModel.inputModel']",
            "input[maxlength='12']",
            "div#input input[type='text']",
            "input.input-type.monospace",
        ]
        for selector in selectors:
            locator = self.page.locator(selector)
            if await locator.count() > 0:
                return locator.first
        raise RuntimeError("Поле ввода ИИН не найдено")

    async def _fill_iin_and_submit(self, iin: str):
        logger.info("Filling IIN: %s", iin)

        input_locator = await self._get_input_locator()
        await input_locator.click()
        await input_locator.fill("")
        await input_locator.type(iin, delay=80)

        entered_value = await input_locator.input_value()
        logger.info("Entered value in input: %s", entered_value)

        await self.page.wait_for_timeout(1200)

        next_button = self.page.locator("button.next-button, button:has-text('Далее')").first
        await next_button.click()

        await self.page.wait_for_timeout(5000)

        try:
            await self.page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            logger.info("networkidle after click not reached, continuing")

        await self.page.wait_for_selector(
            "div.wrapper, div.debt-item, div.pages, button.button-newreq",
            timeout=30000
        )

    async def _extract_travel_status(self) -> str:
        wrapper = self.page.locator("div.wrapper").first
        text = (await wrapper.inner_text()).lower()

        if ("выезд" in text and "запрещ" in text) or ("шығу" in text and "тыйым" in text):
            return "Запрещен"
        if ("выезд" in text and "разреш" in text) or ("шығу" in text and "рұқсат" in text):
            return "Разрешен"
        return "-"

    async def _visible_debt_items_count(self) -> int:
        return await self.page.locator("div.debt-item:visible").count()

    async def _visible_no_data_message(self) -> bool:
        candidates = self.page.locator("div.wrapper p:visible")
        count = await candidates.count()

        for i in range(count):
            text = (await candidates.nth(i).inner_text()).strip().lower()
            if (
                "сведения отсутствуют" in text
                or "не является должником" in text
                or "мәліметтер жоқ" in text
                or "борышкер болып табылмайды" in text
            ):
                return True
        return False

    async def _has_new_request_button(self) -> bool:
        return await self.page.locator("button.button-newreq, button:has-text('Новый запрос')").count() > 0

    async def _has_pagination(self) -> bool:
        return await self.page.locator("span[ng-repeat='page in pages'] a").count() > 0

    async def _extract_table_data(self, table_locator) -> Dict:
        rows = table_locator.locator("tr")
        count = await rows.count()

        item = {
            "issuer": "-",
            "start_date": "-",
            "amount": "-",
            "executor_contact": "-",
        }

        values = []

        for i in range(count):
            row = rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()
            if cell_count < 2:
                continue

            value = (await cells.nth(1).inner_text()).strip() or "-"
            values.append(value)

        # Структура таблицы:
        # 0 = орган
        # 1 = номер исполнительного производства (игнорируем)
        # 2 = дата возбуждения
        # 3 = сумма
        # 4 = категория дела (игнорируем)
        # 5 = контакт исполнителя
        # 6 = дата наложения запрета на выезд (если есть) — игнорируем

        if len(values) >= 1:
            item["issuer"] = values[0]

        if len(values) >= 3:
            item["start_date"] = values[2]

        if len(values) >= 4:
            item["amount"] = self._normalize_amount_string(values[3])

        if len(values) >= 6:
            item["executor_contact"] = values[5]

        return item

    def _normalize_amount_string(self, raw: str) -> str:
        raw = str(raw).strip()

        if not raw or raw == "-":
            return "-"

        # Убираем обычные и неразрывные пробелы, меняем запятую на точку
        normalized = raw.replace(" ", "").replace("\u00A0", "").replace(",", ".")

        cleaned_chars = []
        dot_seen = False

        for i, ch in enumerate(normalized):
            if ch.isdigit():
                cleaned_chars.append(ch)
            elif ch == "-" and i == 0:
                cleaned_chars.append(ch)
            elif ch == "." and not dot_seen:
                cleaned_chars.append(ch)
                dot_seen = True

        cleaned = "".join(cleaned_chars)

        if cleaned in ("", "-", ".", "-."):
            logger.warning("Amount is empty after cleaning: raw=%s", raw)
            return "-"

        try:
            value = Decimal(cleaned)
        except InvalidOperation:
            logger.warning("Failed to normalize amount string: raw=%s | cleaned=%s", raw, cleaned)
            return "-"

        if value == value.to_integral():
            return str(value.quantize(Decimal("1")))

        return format(value.quantize(Decimal("0.01")), "f")

    async def _get_current_page_index(self) -> int:
        page_links = self.page.locator("span[ng-repeat='page in pages'] a")
        count = await page_links.count()

        for i in range(count):
            cls = await page_links.nth(i).get_attribute("class")
            if cls and "current" in cls:
                return i
        return 0

    async def _extract_all_pages_details(self) -> List[Dict]:
        details: List[Dict] = []

        page_links = self.page.locator("span[ng-repeat='page in pages'] a")
        pages_count = await page_links.count()

        if pages_count == 0:
            logger.info("Pagination not found, parsing current page only")
            pages_count = 1

        for page_index in range(pages_count):
            if pages_count > 1:
                current_before = await self._get_current_page_index()
                logger.info(
                    "Switching page | current=%s | target=%s",
                    current_before + 1,
                    page_index + 1
                )

                if current_before != page_index:
                    page_links = self.page.locator("span[ng-repeat='page in pages'] a")
                    target_link = page_links.nth(page_index)
                    await target_link.scroll_into_view_if_needed()
                    await target_link.click()

                    await self.page.wait_for_function(
                        """
                        ([selector, index]) => {
                            const links = document.querySelectorAll(selector);
                            if (!links[index]) return false;
                            return links[index].classList.contains('current');
                        }
                        """,
                        arg=["span[ng-repeat='page in pages'] a", page_index],
                        timeout=7000
                    )

                    await self.page.wait_for_timeout(1500)

            debt_items = self.page.locator("div.debt-item:visible")
            items_count = await debt_items.count()
            logger.info("Visible debt items on page %s: %s", page_index + 1, items_count)

            for item_index in range(items_count):
                table = debt_items.nth(item_index).locator("table.decorated-table").first
                parsed = await self._extract_table_data(table)

                if parsed["issuer"] != "-" or parsed["start_date"] != "-" or parsed["amount"] != "-":
                    details.append(parsed)

        return details

    def _sum_amounts(self, details: List[Dict]) -> str:
        total = Decimal("0")

        for item in details:
            raw = str(item.get("amount", "")).strip()

            if not raw or raw == "-":
                continue

            normalized = raw.replace(" ", "").replace("\u00A0", "").replace(",", ".")

            cleaned_chars = []
            dot_seen = False

            for i, ch in enumerate(normalized):
                if ch.isdigit():
                    cleaned_chars.append(ch)
                elif ch == "-" and i == 0:
                    cleaned_chars.append(ch)
                elif ch == "." and not dot_seen:
                    cleaned_chars.append(ch)
                    dot_seen = True

            cleaned = "".join(cleaned_chars)

            if cleaned in ("", "-", ".", "-."):
                logger.warning("Amount is empty after cleaning: raw=%s", raw)
                continue

            try:
                total += Decimal(cleaned)
            except InvalidOperation:
                logger.warning("Failed to parse amount: raw=%s | cleaned=%s", raw, cleaned)
                continue

        if total == 0:
            return "-"

        if total == total.to_integral():
            return str(total.quantize(Decimal("1")))

        return format(total.quantize(Decimal("0.01")), "f")

    async def _click_new_request_if_possible(self):
        try:
            locator = self.page.locator("button.button-newreq, button:has-text('Новый запрос')")
            if await locator.count() > 0:
                logger.info("Clicking 'Новый запрос'")
                await locator.first.click()
                await self.page.wait_for_timeout(2500)
            else:
                await self._go_to_start_page()
        except Exception:
            logger.warning("Failed to return using 'Новый запрос', reopening start page")
            await self._go_to_start_page()

    async def check_person(self, fio: str, iin: str) -> Dict:
        try:
            logger.info("=== Start check_person | fio=%s | iin=%s ===", fio, iin)

            await self._open_service()
            await self._fill_iin_and_submit(iin)

            visible_items = await self._visible_debt_items_count()
            visible_no_data = await self._visible_no_data_message()
            has_new_request = await self._has_new_request_button()
            has_pagination = await self._has_pagination()

            logger.info(
                "Result markers | iin=%s | visible_debt_items=%s | visible_no_data=%s | new_request=%s | pagination=%s",
                iin,
                visible_items,
                visible_no_data,
                has_new_request,
                has_pagination
            )

            if visible_items > 0:
                travel_status = await self._extract_travel_status()
                details = await self._extract_all_pages_details()
                total_amount = self._sum_amounts(details)

                result = {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Обработано",
                    "travel_status": travel_status,
                    "total_amount": total_amount,
                    "debts_count": len(details),
                    "error_message": "",
                    "details": details,
                }

                logger.info(
                    "Parsed result | iin=%s | status=%s | travel=%s | debts=%s | total=%s",
                    iin,
                    result["check_status"],
                    result["travel_status"],
                    result["debts_count"],
                    result["total_amount"],
                )

                await self._click_new_request_if_possible()
                return result

            if visible_no_data:
                result = {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Не найдено",
                    "travel_status": "-",
                    "total_amount": "-",
                    "debts_count": 0,
                    "error_message": "",
                    "details": [],
                }
                await self._click_new_request_if_possible()
                return result

            if visible_items == 0 and has_new_request and not has_pagination:
                logger.info("Fallback no-data detected for iin=%s", iin)
                result = {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Не найдено",
                    "travel_status": "-",
                    "total_amount": "-",
                    "debts_count": 0,
                    "error_message": "",
                    "details": [],
                }
                await self._click_new_request_if_possible()
                return result

            logger.warning("Ambiguous result page for iin=%s", iin)
            await self._save_debug(f"ambiguous_result_{iin}")

            result = {
                "fio": fio,
                "iin": iin,
                "check_status": "Ошибка проверки",
                "travel_status": "-",
                "total_amount": "-",
                "debts_count": 0,
                "error_message": "Не удалось однозначно определить результат на странице",
                "details": [],
            }

            await self._click_new_request_if_possible()
            return result

        except Exception as e:
            logger.exception("Error while processing iin=%s", iin)
            await self._save_debug(f"check_person_error_{iin}")

            try:
                await self._go_to_start_page()
            except Exception:
                logger.warning("Failed to reopen start page after error")

            return {
                "fio": fio,
                "iin": iin,
                "check_status": "Ошибка проверки",
                "travel_status": "-",
                "total_amount": "-",
                "debts_count": 0,
                "error_message": str(e),
                "details": [],
            }