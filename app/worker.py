import asyncio
from app.database import get_conn
from app.scraper import scrape_url
from app.astra import save_to_astra
from app.emailer import send_email
from app.astra import save_to_primary_astra, save_to_secondary_astra
from app.dynamodb import save_to_dynamodb

BATCH_SIZE = 8

async def worker_loop():
    while True:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, url, filename
            FROM url_queue
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (BATCH_SIZE,))

        rows = cur.fetchall()

        if not rows:
            # Check for completed jobs
            cur.execute("""
                SELECT DISTINCT filename
                FROM url_queue
                WHERE status = 'PENDING'
            """)
            pending_files = cur.fetchall()

            if not pending_files:
                send_email(
                    subject="Scraping completed",
                    body="All queued CSV files have been processed."
                )

            cur.close()
            conn.close()
            await asyncio.sleep(15)
            continue

        ids = [r[0] for r in rows]
        cur.execute(
            "UPDATE url_queue SET status='IN_PROGRESS' WHERE id = ANY(%s)",
            (ids,)
        )
        conn.commit()

        await asyncio.gather(
            *[process_url(r[0], r[1]) for r in rows]
        )

        cur.close()
        conn.close()

async def process_url(row_id, url, filename):
    conn = get_conn()
    cur = conn.cursor()

    try:
        recipe_data = await scrape_url(url)

        # 1. Primary AstraDB (full JSON + vector)
        recipe_id = save_to_primary_astra(recipe_data)

        # 2. Secondary AstraDB (id + vector only)
        save_to_secondary_astra(
            recipe_id=recipe_id,
            text_for_vector=recipe_data.get("title", "")
        )

        # 3. DynamoDB (full JSON)
        save_to_dynamodb(recipe_id, recipe_data)

        # ALL succeeded → remove from queue
        cur.execute("DELETE FROM url_queue WHERE id=%s", (row_id,))
        conn.commit()

    except Exception as e:
        print(f"❌ Failed for URL {url}: {e}")
        cur.execute(
            "UPDATE url_queue SET status='FAILED' WHERE id=%s",
            (row_id,)
        )
        conn.commit()

    finally:
        cur.close()
        conn.close()
