import logging
from typing import Dict, List
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from config import HEADLESS

logger = logging.getLogger(__name__)


class EgovParser:
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.playwright = None

    async def __aenter__(self):
        logger.info("Starting Playwright browser | headless=%s", HEADLESS)
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=HEADLESS,
            slow_mo=200 if not HEADLESS else 0,
        )
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        self.page.set_default_timeout(20000)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        logger.info("Closing Playwright browser")
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def _open_service(self):
        logger.info("Opening eGov service page")
        await self.page.goto(
            "https://egov.kz/services/SR.14/#/declaration/0//",
            wait_until="domcontentloaded"
        )
        await self.page.wait_for_timeout(3000)
        await self.page.wait_for_selector("input[ng-model='viewModel.inputModel']")

    async def _fill_iin_and_submit(self, iin: str):
        logger.info("Filling IIN: %s", iin)

        input_locator = self.page.locator("input[ng-model='viewModel.inputModel']").first
        await input_locator.click()
        await input_locator.fill("")
        await input_locator.fill(iin)

        await self.page.wait_for_timeout(1000)

        logger.info("Clicking 'Далее'")
        next_button = self.page.locator("button.next-button").first
        await next_button.click()

        logger.info("Waiting for result page")
        await self.page.wait_for_timeout(5000)
        await self.page.wait_for_selector(
            "div.wrapper, p[ng-show='noData'], .pages, .debt-item",
            timeout=20000
        )

    async def _extract_travel_status(self) -> str:
        wrapper = self.page.locator("div.wrapper").first
        text = (await wrapper.inner_text()).lower()

        if "выезд:" in text and "запрещен" in text:
            return "Запрещен"
        if "выезд:" in text and "разрешен" in text:
            return "Разрешен"
        return ""

    async def _is_no_data(self) -> bool:
        wrapper = self.page.locator("div.wrapper").first
        text = (await wrapper.inner_text()).lower()
        return (
            "сведения отсутствуют" in text
            or "не является должником" in text
        )

    async def _extract_table_data(self, table_locator) -> Dict:
        rows = table_locator.locator("tr")
        count = await rows.count()

        item = {
            "issuer": "",
            "enforcement_number": "",
            "start_date": "",
            "amount": "",
            "executor_contact": "",
        }

        for i in range(count):
            row = rows.nth(i)
            cells = row.locator("td")
            cell_count = await cells.count()

            if cell_count < 2:
                continue

            key = (await cells.nth(0).inner_text()).strip().lower()
            value = (await cells.nth(1).inner_text()).strip()

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
                        timeout=5000
                    )

                    await self.page.wait_for_timeout(1200)

                current_after = await self._get_current_page_index()
                logger.info("Current page after click: %s", current_after + 1)

            debt_items = self.page.locator("div.debt-item:visible")
            items_count = await debt_items.count()

            logger.info(
                "Visible debt items on page %s: %s",
                page_index + 1,
                items_count
            )

            for item_index in range(items_count):
                table = debt_items.nth(item_index).locator("table.decorated-table").first
                parsed = await self._extract_table_data(table)

                if any(parsed.values()):
                    details.append(parsed)

        return details

    def _sum_amounts(self, details: List[Dict]) -> str:
        total = 0
        for item in details:
            raw = item.get("amount", "")
            cleaned = "".join(ch for ch in raw if ch.isdigit())
            if cleaned:
                total += int(cleaned)
        return str(total) if total else ""

    async def check_person(self, fio: str, iin: str) -> Dict:
        try:
            logger.info("=== Start check_person | fio=%s | iin=%s ===", fio, iin)

            await self._open_service()
            await self._fill_iin_and_submit(iin)

            if await self._is_no_data():
                logger.info("No data found for iin=%s", iin)
                return {
                    "fio": fio,
                    "iin": iin,
                    "check_status": "Не найдено",
                    "travel_status": "",
                    "total_amount": "",
                    "debts_count": 0,
                    "error_message": "",
                    "details": [],
                }

            travel_status = await self._extract_travel_status()
            details = await self._extract_all_pages_details()
            total_amount = self._sum_amounts(details)

            if not details:
                logger.warning("Result page opened, but no debt items parsed | iin=%s", iin)

            result = {
                "fio": fio,
                "iin": iin,
                "check_status": "Обработано" if details or travel_status else "Не найдено",
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

            return result

        except PlaywrightTimeoutError:
            logger.exception("Timeout while processing iin=%s", iin)
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
            logger.exception("Unexpected error while processing iin=%s", iin)
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