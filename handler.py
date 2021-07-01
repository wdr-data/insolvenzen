import json
import datetime
import os

from loguru import logger
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(os.environ["SENTRY_URI"], integrations=[AwsLambdaIntegration()])

from insolvenzen.scrapers.common import clear_caches

# Import your scraper here ⬇️
from insolvenzen.scrapers.private import write_data_private
from insolvenzen.scrapers.regular import write_data_regular
from insolvenzen.scrapers.both import write_data_both

# Add your scraper here ⬇️, without () at the end
SCRAPERS = [
    write_data_private,
    clear_caches,
    write_data_regular,
    write_data_both,
]


def scrape(event, context):
    for scraper in SCRAPERS:
        scraper_name = scraper.__name__
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("scraper", scraper_name)
            try:
                scraper()
                now = datetime.datetime.now()
                logger.info(f"Updated {scraper_name} at {now}")
            except Exception as e:
                # Catch and send error to Sentry manually so we can continue
                # running other scrapers if one fails
                logger.exception(f"Scraper {scraper_name} failed with {e}")
                sentry_sdk.capture_exception(e)

    body = {
        "message": f"Ran {len(SCRAPERS)} scrapers successfully.",
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response


if __name__ == "__main__":
    scrape("", "")
