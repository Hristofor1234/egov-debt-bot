import logging
from pathlib import Path
from typing import Dict, List
from playwright.async_api import async_playwright
from config import HEADLESS, DATA_DIR

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

        logger.info("Page title: %s", await self.page.title())
        logger.info("Current URL: %s", self.page.url)

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

        await self.page.wait_for_timeout(1500)

        next_button = self.page.locator("button.next-button, button:has-text('Далее')").first
        if await next_button.count() == 0:
            await self._save_debug("next_button_not_found")
            raise RuntimeError("Кнопка 'Далее' не найдена")

        logger.info("Clicking 'Далее'")
        await next_button.click()

        await self.page.wait_for_timeout(5000)

        try:
            await self.page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            logger.info("networkidle after click not reached, continuing")

        logger.info("Waiting for result markers")
        await self.page.wait_for_selector(
            "div.wrapper, p[ng-show='noData'], div.debt-item, div.pages, button.button-newreq",
            timeout=30000
        )

    async def _extract_travel_status(self) -> str:
        wrapper = self.page.locator("div.wrapper").first
        text = (await wrapper.inner_text()).lower()

        if "выезд:" in text and "запрещен" in text:
            return "Запрещен"
        if "выезд:" in text and "разрешен" in text:
            return "Разрешен"
        return "-"

    async def _is_no_data(self) -> bool:
        wrapper = self.page.locator("div.wrapper").first
        text = (await wrapper.inner_text()).lower()
        return "сведения отсутствуют" in text or "не является должником" in text

    async def _extract_table_data(self, table_locator) -> Dict:
        rows = table_locator.locator("tr")
        count = await rows.count()

        item = {
            "issuer": "-",
            "enforcement_number": "-",
            "start_date": "-",
            "amount": "-",
            "executor_contact": "-",
        }

        for i in range(count):
            row = rows.nth(i)
            cells = row.locator("td")
            if await cells.count() < 2:
                continue

            key = (await cells.nth(0).inner_text()).strip().lower()
            value = (await cells.nth(1).inner_text()).strip()

            if not value:
                value = "-"

            if "орган, вынесший исполнительный документ" in key:
                item["issuer"] = value
            elif "номер исполнительного производства" in key:
                item["enforcement_number"] = value
            elif "дата возбуждения исполнительного производства" in key:
                item["start_date"] = value
            elif "сумма долга в тенге" in key:
                item["amount"] = value
            elif "информация для связи с судебным исполнителем" in key:
                item["executor_contact"] = value

        return item

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
                if any(v != "-" for v in parsed.values()):
                    details.append(parsed)

        return details

    def _sum_amounts(self, details: List[Dict]) -> str:
        total = 0
        for item in details:
            raw = item.get("amount", "")
            cleaned = "".join(ch for ch in raw if ch.isdigit())
            if cleaned:
                total += int(cleaned)
        return str(total) if total else "-"

    async def _click_new_request_if_possible(self):
        try:
            new_request_button = self.page.locator("button.button-newreq, button:has-text('Новый запрос')").first
            if await new_request_button.count() > 0:
                logger.info("Clicking 'Новый запрос'")
                await new_request_button.click()
                await self.page.wait_for_timeout(2000)

                # если после клика поле ИИН не вернулось — просто открываем стартовую ссылку
                input_candidates = [
                    "input[ng-model='viewModel.inputModel']",
                    "input[maxlength='12']",
                    "div#input input[type='text']",
                    "input.input-type.monospace",
                ]

                found = False
                for selector in input_candidates:
                    if await self.page.locator(selector).count() > 0:
                        found = True
                        break

                if not found:
                    await self._go_to_start_page()
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

            if await self._is_no_data():
                logger.info("No data found for iin=%s", iin)

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

            travel_status = await self._extract_travel_status()
            details = await self._extract_all_pages_details()
            total_amount = self._sum_amounts(details)

            if not details:
                logger.warning("Result page opened, but no debt items parsed | iin=%s", iin)
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