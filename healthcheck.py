"""Read-only availability probe for the eGov entry form.

It deliberately never submits an IIN.  Docker uses it to surface changes in
the public eGov page before queued customer files start failing.
"""
import asyncio

from egov_parser import EgovParser


async def main() -> None:
    async with EgovParser() as parser:
        await parser._open_service()


if __name__ == "__main__":
    asyncio.run(main())
