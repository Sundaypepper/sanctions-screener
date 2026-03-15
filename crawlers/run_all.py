"""
Run all crawlers sequentially.
Usage: python -m crawlers.run_all
Requires env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawlers.ofac import OFACCrawler
from crawlers.un_sc import UNSCCrawler
from crawlers.eu_fsf import EUFSFCrawler
from crawlers.interpol import InterpolCrawler
from crawlers.additional import (
    UKOFSICrawler,
    WorldBankCrawler,
    CanadaCrawler,
    AustraliaCrawler,
    SECOCrawler,
    BISDeniedCrawler,
    SAMExclusionsCrawler,
)
from crawlers.extended import (
    FranceTresorCrawler,
    OFACConsolidatedCrawler,
    OHCHRSettlementCrawler,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

ALL_CRAWLERS = [
    OFACCrawler,
    UNSCCrawler,
    EUFSFCrawler,
    UKOFSICrawler,
    InterpolCrawler,
    WorldBankCrawler,
    CanadaCrawler,
    AustraliaCrawler,
    SECOCrawler,
    BISDeniedCrawler,
    SAMExclusionsCrawler,
    FranceTresorCrawler,
    OFACConsolidatedCrawler,
    OHCHRSettlementCrawler,
]


def run_all(sources: list[str] = None):
    """Run all crawlers. Optionally filter by source name."""
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_KEY"):
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables!")
        sys.exit(1)

    results = {}

    for crawler_cls in ALL_CRAWLERS:
        name = crawler_cls.SOURCE_NAME

        if sources and name not in sources:
            continue

        try:
            crawler = crawler_cls()
            count = crawler.run()
            results[name] = {'status': 'ok', 'count': count}
        except Exception as e:
            logger.error(f"[{name}] Failed: {e}")
            results[name] = {'status': 'error', 'error': str(e)}

    # Summary
    logger.info("=" * 60)
    logger.info("SYNC SUMMARY")
    logger.info("=" * 60)
    total = 0
    for name, result in results.items():
        if result['status'] == 'ok':
            logger.info(f"  ✓ {name:20s} {result['count']:>8,} entities")
            total += result['count']
        else:
            logger.info(f"  ✗ {name:20s} ERROR: {result['error'][:50]}")
    logger.info(f"  {'TOTAL':20s} {total:>8,} entities")
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    sources = sys.argv[1:] if len(sys.argv) > 1 else None
    run_all(sources)
