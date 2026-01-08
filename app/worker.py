import asyncio
import logging
from app.database import get_conn
from app.scraper import scrape_url
from app.astra import save_to_primary_astra, save_to_secondary_astra
from app.dynamodb import save_to_dynamodb
from app.emailer import send_email

BATCH_SIZE = 8
IDLE_SLEEP = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

completion_notified = False


async def worker_loop():
    global completion_notified

    while True:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, url, filename
            FROM url_queue
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (BATCH_SIZE,)
        )

        rows = cur.fetchall()

        if not rows:
            cur.close()
            conn.close()

            if not completion_notified:
                send_email(
                    subject="Scraping completed",
                    body="All queued URLs have been processed."
                )
                completion_notified = True

            await asyncio.sleep(IDLE_SLEEP)
            continue

        completion_notified = False

        ids = tuple(r[0] for r in rows)
        cur.execute(
            "UPDATE url_queue SET status='IN_PROGRESS' WHERE id IN %s",
            (ids,)
        )
        conn.commit()
        cur.close()
        conn.close()

        await asyncio.gather(
            *(process_url(row_id, url, filename) for row_id, url, filename in rows)
        )


async def process_url(row_id, url, filename):
    logging.info(f"Processing URL: {url}")

    conn = get_conn()
    cur = conn.cursor()

    try:
        recipe_data = await scrape_url(url)

        # Deterministic id for idempotency
        recipe_id = f"{filename}:{url}"

        # Run blocking I/O in threads
        await asyncio.to_thread(save_to_primary_astra, recipe_id, recipe_data)
        await asyncio.to_thread(
            save_to_secondary_astra,
            recipe_id,
            recipe_data.get("title", "")
        )
        await asyncio.to_thread(save_to_dynamodb, recipe_id, recipe_data)

        cur.execute("DELETE FROM url_queue WHERE id=%s", (row_id,))
        conn.commit()

        logging.info(f"Completed URL: {url}")

    except Exception as e:
        logging.error(f"Failed URL {url}: {e}")
        cur.execute(
            "UPDATE url_queue SET status='FAILED' WHERE id=%s",
            (row_id,)
        )
        conn.commit()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(worker_loop())
