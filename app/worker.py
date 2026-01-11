import asyncio
import logging
from app.database import get_conn
from app.scraper import scrape_url
from app.astra import save_to_primary_astra, save_to_secondary_astra
from app.dynamodb import save_to_dynamodb
from app.vectorizer import build_vector_text
from app.emailer import send_email

BATCH_SIZE = 8
IDLE_SLEEP = 10

logging.basicConfig(level=logging.INFO)

email_sent = False


async def worker_loop():
    global email_sent

    while True:
        conn = get_conn()
        cur = conn.cursor()

        try:
            # BEGIN transaction implicitly
            cur.execute(
                """
                SELECT recipe_id, url
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
                conn.commit()
                cur.close()
                conn.close()

                if not email_sent:
                    send_email(
                        subject="Recipe scraping completed",
                        body="All queued recipe URLs have been processed."
                    )
                    email_sent = True

                await asyncio.sleep(IDLE_SLEEP)
                continue

            email_sent = False

            ids = tuple(r[0] for r in rows)

            # 🔑 Mark IN_PROGRESS WHILE LOCK IS HELD
            cur.execute(
                "UPDATE url_queue SET status='IN_PROGRESS' WHERE recipe_id IN %s",
                (ids,)
            )

            conn.commit()  # 🔑 release locks safely

        except Exception:
            conn.rollback()
            cur.close()
            conn.close()
            raise

        cur.close()
        conn.close()

        await asyncio.gather(
            *(process_row(recipe_id, url) for recipe_id, url in rows)
        )


async def process_row(recipe_id: int, url: str):
    logging.info(f"Processing recipe_id={recipe_id}")

    conn = get_conn()
    cur = conn.cursor()

    try:
        recipe_json = await scrape_url(url)
        vector_text = build_vector_text(recipe_json)

        # Store in all DBs
        await asyncio.to_thread(save_to_primary_astra, recipe_id, recipe_json, vector_text)
        await asyncio.to_thread(save_to_secondary_astra, recipe_id, vector_text)
        await asyncio.to_thread(save_to_dynamodb, recipe_id, recipe_json)

        # Remove from queue on success
        cur.execute("DELETE FROM url_queue WHERE recipe_id=%s", (recipe_id,))
        conn.commit()

        logging.info(f"Completed recipe_id={recipe_id}")

    except Exception as e:
        logging.error(f"Failed recipe_id={recipe_id}: {e}")
        cur.execute(
            "UPDATE url_queue SET status='FAILED' WHERE recipe_id=%s",
            (recipe_id,)
        )
        conn.commit()

    finally:
        cur.close()
        conn.close()



if __name__ == "__main__":
    asyncio.run(worker_loop())
