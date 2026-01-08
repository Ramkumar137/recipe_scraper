import asyncio
from app.database import get_conn
from app.scraper import scrape_url
from app.astra import save_to_astra
from app.emailer import send_email

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
            cur.close()
            conn.close()
            await asyncio.sleep(10)
            continue

        ids = [r[0] for r in rows]
        cur.execute(
            "UPDATE url_queue SET status='IN_PROGRESS' WHERE id = ANY(%s)",
            (ids,)
        )
        conn.commit()

        tasks = []
        for _id, url, filename in rows:
            tasks.append(process_url(_id, url, filename))

        await asyncio.gather(*tasks)

        cur.close()
        conn.close()

async def process_url(row_id, url, filename):
    conn = get_conn()
    cur = conn.cursor()

    try:
        data = await scrape_url(url)
        save_to_astra(data)

        cur.execute("DELETE FROM url_queue WHERE id=%s", (row_id,))
        conn.commit()

    except Exception as e:
        cur.execute(
            "UPDATE url_queue SET status='FAILED' WHERE id=%s",
            (row_id,)
        )
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(worker_loop())
