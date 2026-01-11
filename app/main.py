from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
import pandas as pd
from app.database import get_conn
from app.emailer import send_email
from urllib.parse import urlparse

app = FastAPI()

app.mount("/ui", StaticFiles(directory="static", html=True), name="static")


def is_valid_url(url: str) -> bool:
    try:
        r = urlparse(url)
        return all([r.scheme, r.netloc])
    except:
        return False


@app.post("/upload")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    df = pd.read_csv(file.file)

    urls = [
        u.strip()
        for u in df.iloc[:, 0].dropna().unique()
        if is_valid_url(str(u))
    ]

    if not urls:
        return {"count": 0, "message": "No valid URLs found"}

    conn = get_conn()
    cur = conn.cursor()

    for url in urls:
        cur.execute(
            "INSERT INTO url_queue (filename, url) VALUES (%s, %s)",
            (file.filename, url)
        )

    conn.commit()
    cur.close()
    conn.close()

    background_tasks.add_task(
        send_email,
        "CSV queued",
        f"{file.filename} queued with {len(urls)} URLs."
    )

    return {"count": len(urls)}
