"""
Watermark Service for "The Art of Practice" ebook.
Deployed on Render.com (free tier, no credit card).
"""

import io
import os
import uuid
import time
import tempfile
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, Response
import fitz  # PyMuPDF
import requests

app = Flask(__name__)

# ---------- CONFIG ----------
API_SECRET = os.environ.get("API_SECRET", "")
GHL_API_KEY = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID = os.environ.get("GHL_LOCATION_ID", "DbHHEJGE1RLMqU0unS6p")
GHL_EBOOK_FIELD_KEY = os.environ.get("GHL_EBOOK_FIELD_KEY", "contact.ebook_download_link")
MASTER_PDF_URL = os.environ.get("MASTER_PDF_URL", "")
BASE_URL = os.environ.get("RENDER_EXTERNAL_URL", "http://localhost:5000")
WATERMARK_FONTSIZE = float(os.environ.get("WATERMARK_FONTSIZE", "9.5"))
LINK_EXPIRY_DAYS = int(os.environ.get("LINK_EXPIRY_DAYS", "7"))
FONT_URL = "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond%5Bwght%5D.ttf"

# ---------- IN-MEMORY STORAGE ----------
pdf_store = {}
store_lock = threading.Lock()
_master_pdf_cache = None
_font_path = None


def get_font_path():
    """Download and cache EB Garamond font from Google Fonts."""
    global _font_path
    if _font_path and os.path.exists(_font_path):
        return _font_path
    # Check if bundled locally first
    local = os.path.join(os.path.dirname(__file__), "EBGaramond.ttf")
    if os.path.exists(local):
        _font_path = local
        return _font_path
    # Download from Google Fonts
    print(f"[{datetime.utcnow()}] Downloading EB Garamond font...")
    resp = requests.get(FONT_URL, timeout=30)
    resp.raise_for_status()
    tmp = os.path.join(tempfile.gettempdir(), "EBGaramond.ttf")
    with open(tmp, "wb") as f:
        f.write(resp.content)
    _font_path = tmp
    print(f"[{datetime.utcnow()}] Font cached at {tmp}")
    return _font_path


def get_master_pdf():
    """Download and cache the master PDF bytes."""
    global _master_pdf_cache
    if _master_pdf_cache:
        return io.BytesIO(_master_pdf_cache)
    if not MASTER_PDF_URL:
        raise ValueError("MASTER_PDF_URL not configured")
    print(f"[{datetime.utcnow()}] Downloading master PDF...")
    resp = requests.get(MASTER_PDF_URL, timeout=120)
    resp.raise_for_status()
    _master_pdf_cache = resp.content
    print(f"[{datetime.utcnow()}] Master PDF cached ({len(_master_pdf_cache)/1024/1024:.1f} MB)")
    return io.BytesIO(_master_pdf_cache)


def watermark_pdf(master_bytes, email):
    font_path = get_font_path()
    fontsize = WATERMARK_FONTSIZE
    doc = fitz.open(stream=master_bytes.read(), filetype="pdf")
    text = f"Licensed to: {email}"
    font = fitz.Font(fontfile=font_path)
    text_width = font.text_length(text, fontsize=fontsize)
    for page in doc:
        rect = page.rect
        w = rect.width
        h = rect.height
        x = (w - text_width) / 2
        y = h - 10
        page.insert_text(
            fitz.Point(x, y), text,
            fontfile=font_path, fontname="EBGaramond",
            fontsize=fontsize, color=(0.5, 0.5, 0.5), overlay=True,
        )
    output = io.BytesIO()
    doc.save(output, garbage=4, deflate=True)
    doc.close()
    return output.getvalue()


def update_ghl_contact(email, download_url):
    if not GHL_API_KEY or not GHL_EBOOK_FIELD_KEY:
        return {"skipped": "GHL not configured"}
    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type": "application/json",
        "Version": "2021-07-28",
    }
    lookup_url = (
        f"https://services.leadconnectorhq.com/contacts/lookup"
        f"?locationId={GHL_LOCATION_ID}&email={email}"
    )
    resp = requests.get(lookup_url, headers=headers, timeout=15)
    if resp.status_code != 200:
        return {"error": f"Lookup failed: {resp.status_code}"}
    data = resp.json()
    contacts = data.get("contacts", [])
    if not contacts:
        return {"error": "Contact not found"}
    contact_id = contacts[0]["id"]
    update_url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
    update_resp = requests.put(
        update_url, headers=headers,
        json={"customFields": [{"key": GHL_EBOOK_FIELD_KEY, "value": download_url}]},
        timeout=15,
    )
    return {"contact_id": contact_id, "updated": update_resp.status_code == 200}


def cleanup_expired():
    cutoff = datetime.utcnow() - timedelta(days=LINK_EXPIRY_DAYS)
    with store_lock:
        expired = [k for k, v in pdf_store.items() if v["created"] < cutoff]
        for k in expired:
            del pdf_store[k]
    if expired:
        print(f"[{datetime.utcnow()}] Cleaned up {len(expired)} expired PDFs")


def start_cleanup_thread():
    def loop():
        while True:
            time.sleep(3600)
            cleanup_expired()
    t = threading.Thread(target=loop, daemon=True)
    t.start()


@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "service": "Art of Practice Watermark Service",
        "status": "ok",
        "active_downloads": len(pdf_store),
    })


@app.route("/watermark", methods=["POST"])
def watermark():
    try:
        data = request.get_json(silent=True) or {}
        email = data.get("email", "").strip().lower()
        secret = data.get("secret", "")
        if API_SECRET and secret != API_SECRET:
            return jsonify({"error": "Unauthorized"}), 403
        if not email or "@" not in email:
            return jsonify({"error": "Valid email required"}), 400
        master = get_master_pdf()
        pdf_bytes = watermark_pdf(master, email)
        download_id = str(uuid.uuid4())
        with store_lock:
            pdf_store[download_id] = {
                "pdf_bytes": pdf_bytes,
                "email": email,
                "created": datetime.utcnow(),
            }
        download_url = f"{BASE_URL}/download/{download_id}"
        ghl_result = update_ghl_contact(email, download_url)
        print(f"[{datetime.utcnow()}] Watermarked PDF for {email}")
        return jsonify({
            "download_url": download_url,
            "email": email,
            "download_id": download_id,
            "ghl_update": ghl_result,
        })
    except Exception as e:
        print(f"[{datetime.utcnow()}] ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/download/<download_id>", methods=["GET"])
def download(download_id):
    with store_lock:
        entry = pdf_store.get(download_id)
    if not entry:
        return Response(
            "<html><body><h2>Download link expired</h2>"
            "<p>Please contact laido@theartofpractice.com for a new link.</p>"
            "</body></html>", status=404, content_type="text/html",
        )
    if datetime.utcnow() - entry["created"] > timedelta(days=LINK_EXPIRY_DAYS):
        with store_lock:
            pdf_store.pop(download_id, None)
        return Response(
            "<html><body><h2>Download link expired</h2>"
            "<p>Contact laido@theartofpractice.com for help.</p>"
            "</body></html>", status=410, content_type="text/html",
        )
    return send_file(
        io.BytesIO(entry["pdf_bytes"]),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="The_Art_of_Practice.pdf",
    )


start_cleanup_thread()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

