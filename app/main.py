from fastapi import FastAPI, UploadFile, File
from fastapi.staticfiles import StaticFiles
import pandas as pd
from app.database import get_conn
from app.emailer import send_email
from urllib.parse import urlparse

app = FastAPI()

# Serve static UI
app.mount("/", StaticFiles(directory="static", html=True), name="static")

def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)

    urls = df.iloc[:, 0].dropna().unique()

    valid_urls = [u for u in urls if is_valid_url(u)]

    if not valid_urls:
        return {"error": "No valid URLs found"}

    conn = get_conn()
    cur = conn.cursor()

    for url in valid_urls:
        cur.execute(
            "INSERT INTO url_queue (filename, url) VALUES (%s, %s)",
            (file.filename, url)
        )

    conn.commit()
    cur.close()
    conn.close()

    send_email(
        subject="CSV queued",
        body=f"{file.filename} queued with {len(valid_urls)} URLs."
    )

    return {"status": "queued", "count": len(valid_urls)}
