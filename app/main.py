from fastapi import FastAPI, UploadFile, File
from fastapi.staticfiles import StaticFiles
import pandas as pd
from app.database import get_conn
from app.emailer import send_email

app = FastAPI()

# Serve static UI
app.mount("/", StaticFiles(directory="static", html=True), name="static")

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)

    conn = get_conn()
    cur = conn.cursor()

    for url in df.iloc[:, 0]:
        cur.execute(
            "INSERT INTO url_queue (filename, url) VALUES (%s, %s)",
            (file.filename, url)
        )

    conn.commit()
    cur.close()
    conn.close()

    send_email(
        subject="CSV queued",
        body=f"{file.filename} added to URL DB queue."
    )

    return {"status": "queued", "count": len(df)}
