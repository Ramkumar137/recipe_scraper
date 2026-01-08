from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from fastapi.staticfiles import StaticFiles
import pandas as pd
from app.database import get_conn
from app.emailer import send_email

app = FastAPI()
app.mount("/", StaticFiles(directory="static", html=True), name="static")


@app.post("/upload")
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...)
):
    df = pd.read_csv(file.file)
    urls = df.iloc[:, 0].dropna().unique()

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

    return {"status": "queued", "count": len(urls)}
