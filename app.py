"""
Watermark Service for "The Art of Practice" ebook.
Deployed on Render.com (free tier, no credit card).

v2: Signed URL downloads — generates PDF on-the-fly, no storage needed.
    Links survive service restarts and never expire.

    Cache-bust redeploy 2026-05-11T06:26:06.338554Z — refresh master PDF from Drive.
"""

import io
import os
import hmac
import hashlib
import base64
import tempfile
from datetime import datetime

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
FONT_URL = "https://github.com/google/fonts/raw/main/ofl/ebgaramond/EBGaramond%5Bwght%5D.ttf"

# ---------- CACHES ----------
_master_pdf_cache = None
_font_path = None


# ---------- SIGNED URL HELPERS ----------
def generate_signed_token(email):
    """Generate a signed token encoding the buyer's email.
    Format: base64url(email).hmac_hex_signature
    """
    email_b64 = base64.urlsafe_b64encode(email.encode()).decode().rstrip("=")
    signature = hmac.new(
        API_SECRET.encode(), email.encode(), hashlib.sha256
    ).hexdigest()[:32]
    return f"{email_b64}.{signature}"


def verify_signed_token(token):
    """Verify token and return the email, or None if invalid."""
    parts = token.split(".")
    if len(parts) != 2:
        return None
    email_b64, signature = parts
    # Restore base64 padding
    padding = 4 - len(email_b64) % 4
    if padding != 4:
        email_b64 += "=" * padding
    try:
        email = base64.urlsafe_b64decode(email_b64).decode()
    except Exception:
        return None
    expected = hmac.new(
        API_SECRET.encode(), email.encode(), hashlib.sha256
    ).hexdigest()[:32]
    if not hmac.compare_digest(signature, expected):
        return None
    return email


# ---------- PDF HELPERS ----------
def get_font_path():
    """Download and cache EB Garamond font from Google Fonts."""
    global _font_path
    if _font_path and os.path.exists(_font_path):
        return _font_path
    local = os.path.join(os.path.dirname(__file__), "EBGaramond.ttf")
    if os.path.exists(local):
        _font_path = local
        return _font_path
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


def watermark_pdf(master_bytes, email, display_name=None):
    font_path = get_font_path()
    fontsize = WATERMARK_FONTSIZE
    doc = fitz.open(stream=master_bytes.read(), filetype="pdf")
    text = f"Licensed to: {display_name} — {email}" if display_name else f"Licensed to: {email}"
    font = fitz.Font(fontfile=font_path)
    text_width = font.text_length(text, fontsize=fontsize)

    for page in doc:
        rect = page.rect
        w = rect.width
        h = rect.height
        x = (w - text_width) / 2
        y = h - 10
        page.insert_text(
            fitz.Point(x, y),
            text,
            fontfile=font_path,
            fontname="EBGaramond",
            fontsize=fontsize,
            color=(0.5, 0.5, 0.5),
            overlay=True,
        )

    output = io.BytesIO()
    doc.save(output)  # garbage=4, deflate=True removed for ~3-5s speedup per download
    doc.close()
    return output.getvalue()


def lookup_ghl_name(email):
    """Look up firstName/lastName for email via GHL v2 contacts API."""
    if not GHL_API_KEY:
        return None
    try:
        headers = {
            "Authorization": f"Bearer {GHL_API_KEY}",
            "Version": "2021-07-28",
            "Accept": "application/json",
        }
        url = (
            f"https://services.leadconnectorhq.com/contacts/"
            f"?locationId={GHL_LOCATION_ID}&query={email}"
        )
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"[{datetime.utcnow()}] GHL lookup failed: {resp.status_code} {resp.text[:200]}")
            return None
        data = resp.json() or {}
        contacts = data.get("contacts") or []
        if not contacts:
            return None
        c = contacts[0]
        first = (c.get("firstName") or "").strip()
        last = (c.get("lastName") or "").strip()
        full = (first + " " + last).strip()
        return full or None
    except Exception as e:
        print(f"[{datetime.utcnow()}] GHL lookup error: {e}")
        return None


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
        update_url,
        headers=headers,
        json={"customFields": [{"key": GHL_EBOOK_FIELD_KEY, "value": download_url}]},
        timeout=15,
    )
    return {"contact_id": contact_id, "updated": update_resp.status_code == 200}


# ---------- ROUTES ----------
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "service": "Art of Practice Watermark Service",
        "status": "ok",
        "version": "2.0-signed-urls",
    })


@app.route("/watermark", methods=["POST"])
def watermark():
    """Generate a signed download URL for the buyer.
    No PDF is stored - it gets generated on-the-fly when the link is clicked.
    """
    try:
        data = request.get_json(silent=True) or {}
        email = data.get("email", "").strip().lower()
        secret = data.get("secret", "")

        if API_SECRET and secret != API_SECRET:
            return jsonify({"error": "Unauthorized"}), 403
        if not email or "@" not in email:
            return jsonify({"error": "Valid email required"}), 400

        # Generate signed download token
        token = generate_signed_token(email)
        download_url = f"{BASE_URL}/dl/{token}"

        # Update GHL contact with the permanent download link
        ghl_result = update_ghl_contact(email, download_url)

        print(f"[{datetime.utcnow()}] Signed download URL generated for {email}")

        return jsonify({
            "download_url": download_url,
            "email": email,
            "ghl_update": ghl_result,
        })

    except Exception as e:
        print(f"[{datetime.utcnow()}] ERROR: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/dl/<token>", methods=["GET"])
def download_signed(token):
    """Verify the signed token, generate the watermarked PDF on-the-fly, and serve it.
    No storage needed - the PDF is freshly generated each time.
    Links never expire and survive service restarts.
    """
    email = verify_signed_token(token)
    if not email:
        return Response(
            "<html><body><h2>Invalid download link</h2>"
            "<p>Please contact laido@theartofpractice.com for help.</p>"
            "</body></html>",
            status=403,
            content_type="text/html",
        )

    try:
        display_name = lookup_ghl_name(email)
        master = get_master_pdf()
        pdf_bytes = watermark_pdf(master, email, display_name)
        print(f"[{datetime.utcnow()}] On-demand PDF generated for {email}")

        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name="The_Art_of_Practice.pdf",
        )
    except Exception as e:
        print(f"[{datetime.utcnow()}] Download error for {email}: {e}")
        return Response(
            "<html><body><h2>Temporary error</h2>"
            "<p>Please try again in a moment. If the issue persists, "
            "contact laido@theartofpractice.com.</p>"
            "</body></html>",
            status=500,
            content_type="text/html",
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
