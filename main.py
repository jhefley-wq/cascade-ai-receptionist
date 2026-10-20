"""
Cascade RV Solar Solutions — AI Receptionist recovery application.

Audio pipeline (unchanged):
  Twilio → G.711 µ-law 8kHz → PCM 16-bit 24kHz → OpenAI Realtime API
  OpenAI → PCM 16-bit 24kHz → G.711 µ-law 8kHz → Twilio
"""
from __future__ import annotations

import asyncio
import audioop
import base64
import html
import json
import logging
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode

import requests
import websockets
from dotenv import load_dotenv

# Load local development configuration before storage reads DATABASE_URL. Railway
# supplies environment variables directly, so this is a no-op there when absent.
load_dotenv()

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, status
from fastapi.responses import HTMLResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from twilio.request_validator import RequestValidator
from twilio.twiml.voice_response import Connect, VoiceResponse

from storage import (
    append_transcript_line,
    attach_stream,
    call_count,
    caller_for_call,
    get_call_view,
    get_receptionist_state,
    init_db,
    list_call_views,
    set_receptionist_state,
    update_extracted_fields,
    upsert_call,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("cascade-receptionist")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
OWNER_EMAIL = os.getenv("OWNER_EMAIL", "")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
PORT = int(os.getenv("PORT", "8000"))
OPENAI_REALTIME_MODEL = os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime-2.1")
OPENAI_REALTIME_URL = "wss://api.openai.com/v1/realtime?" + urlencode({"model": OPENAI_REALTIME_MODEL})
VOICE = "echo"

# Set this true in a real/staging public environment. It remains false by default
# solely so a local server can be exercised without a Twilio callback signature.
TWILIO_VALIDATE_SIGNATURES = os.getenv("TWILIO_VALIDATE_SIGNATURES", "false").strip().lower() in {"1", "true", "yes", "on"}
# These should be exact public origins/URLs when a reverse proxy rewrites scheme/host.
TWILIO_WEBHOOK_BASE_URL = os.getenv("TWILIO_WEBHOOK_BASE_URL", "").strip().rstrip("/")
TWILIO_MEDIA_STREAM_URL = os.getenv("TWILIO_MEDIA_STREAM_URL", "").strip()
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

basic_auth = HTTPBasic(auto_error=False)
app = FastAPI()
# A transient optimization only. Call records remain in the database even if this
# cache is emptied or the process restarts.
pending_calls: dict[str, str] = {}

SYSTEM_MESSAGE = """You are Alex, the Technical Advisor for Cascade RV Solar Solutions, a mobile RV solar
installation company based in Prineville, Oregon, owned by Jason Hefley.

YOUR ROLE:
You are an experienced RV solar consultant with years of field experience. Your primary objective
is to educate first and sell second. Speak naturally and conversationally — never like a salesperson.
Explain technical concepts in clear, easy-to-understand language without talking down to the customer.
Always remain calm, patient, and professional. Your goal is to become the most trusted technical
resource a customer has ever spoken with.

PERSONALITY:
Warm, knowledgeable, and direct. You speak like a real person. Keep responses SHORT — 1-3 sentences
unless the caller asks for detailed information. Never ramble. Never pressure a customer.
If you do not know an answer with confidence, say so honestly rather than guessing.

COMPANY INFORMATION:
- Business: Cascade RV Solar Solutions
- Owner: Jason Hefley
- Phone: (503) 919-0521
- Location: Prineville, Oregon
- Service Area: All of Oregon (mobile service — we come to you)
- Website: cascadesolarrvsolutions.com
- Experience: 30+ years in solar and electrical systems

SERVICES:
- Custom RV solar system design and installation
- Solar consultation and system sizing
- Troubleshooting and repair of existing solar systems
- DIY guidance and training
- Financing available through Enhancify
- Preferred brands: Victron Energy, Renogy, EPOCH batteries, Rich Solar

YOUR RESPONSIBILITIES:
- Answer technical questions about RV solar and electrical systems
- Help customers understand the advantages and disadvantages of different system designs
- Assist customers in determining their power needs based on how they use their RV
- Explain Victron products, lithium batteries, MPPT charge controllers, inverter chargers,
  battery monitors, DC-DC chargers, solar panels, wiring, fusing, and commissioning
- Help troubleshoot common RV electrical issues
- Recommend scheduling an appointment when a project requires detailed design, installation,
  or advanced troubleshooting
- Never recommend replacing equipment until the problem has been properly diagnosed
- When discussing safety, clearly explain risks without being alarmist
- For high-current, battery, or AC wiring work, recommend qualified personnel
- If the caller seems to be in an emergency (e.g., electrical issue, fire risk), advise them
  to call 911 or a licensed electrician immediately

PRODUCT KNOWLEDGE (VICTRON EXPERT):
You are the system expert on Victron Energy components (except batteries):
- Inverter/Chargers: Victron MultiPlus-II is our go-to recommendation. Recommend 24V systems
  when the inverter exceeds 3000W or battery-to-inverter wiring would exceed 4/0 gauge.
- Solar Charge Controllers: Victron SmartSolar MPPT (100/30 through 250/100). Sized using
  daily energy consumption (Wh), Peak Sun Hours (PSH), and 70-80% system efficiency factor.
  Include a 20-30% oversize factor for flat mounting and seasonal variation.
- DC-DC Chargers: Victron Orion-Tr Smart Isolated DC-DC charger for alternator charging.
- Distribution: Victron Lynx Distributor (1000A busbar) and Lynx Shunt VE.Can.
  Wire sizes for Lynx-to-inverter connections are calculated at max power.
- Monitoring: Always bundle Cerbo GX with the Victron SmartShunt and GX Touch 70 display.
- Batteries: Never recommend Victron batteries. Premium systems use EPOCH LiFePO4.
  Budget systems use Renogy batteries.
- Solar Panels: Renogy ShadowFlux (N-Type, 10x bypass diodes, excellent in partial shade,
  25% efficiency, IP67+) is our preferred panel. Rich Solar MEGA 200W or 400W is the
  alternative. Design arrays in 200W increments: 200W, 800W, 1200W, 1600W.

PRICING & TIMELINE:
- Free consultations available
- Typical installation: 4-5 days of work
- Booking lead time: approximately 4-6 weeks out
- 1-year labor warranty on all installations
- Financing available through Enhancify

FREQUENTLY ASKED QUESTIONS:
Q: What areas do you serve?
A: All of Oregon. We're mobile — we come to your location.

Q: How much does a solar installation cost?
A: It depends on system size and complexity. Jason offers free consultations for accurate quotes.

Q: How long does installation take?
A: Most installations take 4-5 days. We're typically booked about 4-6 weeks out.

Q: What brands do you use?
A: Victron Energy for components, EPOCH LiFePO4 for premium batteries, Renogy for budget
   systems and solar panels, and Rich Solar MEGA panels as an alternative.

Q: Do you offer financing?
A: Yes, through Enhancify. Jason can walk through the options during a consultation.

Q: Do you work on all types of RVs?
A: Yes — motorhomes, fifth wheels, travel trailers, toy haulers, and more.

CLARIFYING QUESTIONS (ask before making recommendations):
Always ask clarifying questions naturally, one at a time, before giving advice:
- What year, make, and model is your RV?
- How do you typically camp? (boondocking, campgrounds, full-time, etc.)
- What appliances do you want to run?
- What batteries do you currently have?
- Do you already have solar?
- What inverter or charger is currently installed?

LEAD CAPTURE & QUALIFICATION:
When a caller wants a callback, consultation, or to leave a message, ask qualifying questions
naturally first (RV type, year/make/model, goals, current setup, location in Oregon), then
collect their contact info:
1. Full name
2. Best phone number
3. Email address (optional but helpful)

Confirm their info back to them and let them know Jason will be in touch soon.
At the end of every appropriate conversation, ask whether they would like to schedule
a consultation or installation with Cascade RV Solar Solutions.

IMPORTANT RULES:
- NEVER QUOTE A PRICE OR A PRICE RANGE. If asked about cost, explain that pricing depends entirely on the specific RV and requirements, and that Jason will discuss pricing during his callback.
- Never make up technical specs you are unsure of
- If asked something you don't know, say Jason will be happy to discuss it in a consultation
- Never pressure a customer into purchasing anything
- Focus on helping them make informed decisions
- The customer should leave every conversation feeling informed, respected, and confident,
  whether or not they purchase anything"""

# ── Audio conversion helpers ──────────────────────────────────────────────────
def ulaw8k_to_pcm24k(ulaw_b64: str) -> str:
    try:
        ulaw_bytes = base64.b64decode(ulaw_b64)
        pcm_8k = audioop.ulaw2lin(ulaw_bytes, 2)
        pcm_24k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 24000, None)
        return base64.b64encode(pcm_24k).decode("utf-8")
    except Exception:
        logger.debug("Unable to convert incoming audio frame")
        return ""


def pcm24k_to_ulaw8k_bytes(pcm_bytes: bytes) -> bytes:
    try:
        pcm_8k, _ = audioop.ratecv(pcm_bytes, 2, 1, 24000, 8000, None)
        return audioop.lin2ulaw(pcm_8k, 2)
    except Exception:
        logger.debug("Unable to convert outgoing audio frame")
        return b""


# ── Authentication and Twilio request validation ───────────────────────────────
def require_admin(credentials: HTTPBasicCredentials | None = Depends(basic_auth)) -> None:
    """Protect operator routes and fail closed when credentials are not configured."""
    import secrets

    if not ADMIN_USERNAME or not ADMIN_PASSWORD:
        # A dashboard without configured credentials must never expose lead data.
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Dashboard authentication is not configured")
    if credentials is None or not (
        secrets.compare_digest(credentials.username, ADMIN_USERNAME)
        and secrets.compare_digest(credentials.password, ADMIN_PASSWORD)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )


def _raw_query(scope: dict) -> str:
    return scope.get("query_string", b"").decode("latin-1")


def _with_query(url: str, scope: dict) -> str:
    query = _raw_query(scope)
    return f"{url}?{query}" if query else url


def _http_callback_url(request: Request) -> str:
    """Build the external callback URL used by Twilio when signing the request."""
    if TWILIO_WEBHOOK_BASE_URL:
        return _with_query(f"{TWILIO_WEBHOOK_BASE_URL}{request.url.path}", request.scope)
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip()
    forwarded_host = request.headers.get("x-forwarded-host", "").split(",")[0].strip()
    scheme = forwarded_proto or request.url.scheme
    host = forwarded_host or request.headers.get("host", request.url.netloc)
    return _with_query(f"{scheme}://{host}{request.url.path}", request.scope)


def _websocket_callback_url(websocket: WebSocket) -> str:
    """Build the exact public WSS URL that Twilio signs during its stream handshake."""
    if TWILIO_MEDIA_STREAM_URL:
        return _with_query(TWILIO_MEDIA_STREAM_URL, websocket.scope)
    forwarded_host = websocket.headers.get("x-forwarded-host", "").split(",")[0].strip()
    host = forwarded_host or websocket.headers.get("host", websocket.url.netloc)
    return _with_query(f"wss://{host}{websocket.url.path}", websocket.scope)


async def _validated_twilio_form(request: Request) -> tuple[bool, dict[str, str]]:
    """Return an already-read form and whether a required Twilio signature was valid."""
    content_type = request.headers.get("content-type", "").lower()
    if content_type.startswith("application/json"):
        raw_body = await request.body()
        try:
            payload = json.loads(raw_body)
            form = {str(key): str(value) for key, value in payload.items()} if isinstance(payload, dict) else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            form = {}
        signature_params: object = raw_body.decode("utf-8", errors="replace")
    else:
        raw_form = await request.form()
        form = {key: str(value) for key, value in raw_form.items()}
        # Keep the MultiDict for the SDK: it can represent repeated parameter
        # values that a regular dict would silently collapse.
        signature_params = raw_form
    if not TWILIO_VALIDATE_SIGNATURES:
        return True, form
    signature = request.headers.get("x-twilio-signature", "")
    if not TWILIO_AUTH_TOKEN or not signature:
        logger.warning("Rejected Twilio webhook because signature validation prerequisites were missing")
        return False, form
    try:
        valid = RequestValidator(TWILIO_AUTH_TOKEN).validate(_http_callback_url(request), signature_params, signature)
    except Exception:
        logger.warning("Rejected Twilio webhook because signature validation could not complete")
        return False, form
    if not valid:
        logger.warning("Rejected Twilio webhook with an invalid signature")
    return valid, form


def _valid_twilio_websocket(websocket: WebSocket) -> bool:
    """Validate the Twilio Media Streams handshake when signature enforcement is enabled."""
    if not TWILIO_VALIDATE_SIGNATURES:
        return True
    signature = websocket.headers.get("x-twilio-signature", "")
    if not TWILIO_AUTH_TOKEN or not signature:
        logger.warning("Rejected Media Stream because signature validation prerequisites were missing")
        return False
    try:
        valid = RequestValidator(TWILIO_AUTH_TOKEN).validate(_websocket_callback_url(websocket), {}, signature)
    except Exception:
        logger.warning("Rejected Media Stream because signature validation could not complete")
        return False
    if not valid:
        logger.warning("Rejected Media Stream with an invalid signature")
    return valid


def _parse_twilio_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _media_stream_url(request: Request) -> str:
    """Return the public stream URL without changing existing default TwiML behavior."""
    if TWILIO_MEDIA_STREAM_URL:
        return TWILIO_MEDIA_STREAM_URL
    return f"wss://{request.headers.get('host', '')}/media-stream"


# ── GPT-4o lead extraction (sync, runs in a worker thread) ────────────────────
def _gpt4o_extract(transcript_lines: list[str], caller_number: str) -> dict:
    """Call GPT-4o to extract durable lead fields after a stream concludes."""
    if not transcript_lines:
        return {}
    transcript_text = "\n".join(transcript_lines)
    prompt = f"""Analyze this phone call transcript between an AI receptionist (Alex) and a caller.
Extract the following fields. Return ONLY a JSON object with these exact keys (use null if not mentioned):
- name: Caller's first and last name
- phone: Phone number the caller explicitly stated (not the caller ID)
- email: Email address
- rv_details: RV type, year, make, and model
- goals: What they want to achieve
- current_setup: Their current electrical/solar setup
- location: Their location in Oregon
- summary: A 2-3 sentence summary of the caller's needs and call outcome.

Transcript:
{transcript_text}"""
    if not OPENAI_API_KEY:
        logger.warning("Lead extraction skipped because OPENAI_API_KEY is not configured")
        return {"summary": "Transcript captured; automated lead extraction was unavailable."}
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "max_tokens": 600,
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        extracted = json.loads(payload["choices"][0]["message"]["content"])
        return extracted if isinstance(extracted, dict) else {}
    except Exception:
        logger.error("Automated lead extraction failed")
        return {"summary": "Transcript captured; automated lead extraction failed."}


# ── Email notification ────────────────────────────────────────────────────────
def send_lead_email(lead: dict) -> None:
    """Send an operator notification without writing customer content to application logs."""
    if not SENDGRID_API_KEY or not OWNER_EMAIL:
        logger.warning("Lead email skipped because SendGrid or owner email is not configured")
        return

    def safe(value: object) -> str:
        return html.escape(str(value or "—"), quote=True)

    def row(label: str, key: str, last: bool = False) -> str:
        border = "" if last else "border-bottom:1px solid #eee;"
        return (
            f'<tr><td style="padding:10px;{border}color:#666;width:140px;"><strong>{label}</strong></td>'
            f'<td style="padding:10px;{border}">{safe(lead.get(key))}</td></tr>'
        )

    transcript = "\n".join(safe(line) for line in lead.get("transcript", []))
    html_content = f"""
    <div style="font-family:Arial,sans-serif;max-width:620px;margin:0 auto;background:#f8f9fa;padding:20px;border-radius:8px;">
      <div style="background:#1a3a5c;color:white;padding:20px;border-radius:8px 8px 0 0;">
        <h2 style="margin:0;">New Lead — Cascade RV Solar Solutions</h2>
        <p style="margin:4px 0 0;opacity:0.8;font-size:14px;">Captured by your AI Receptionist · {safe(lead.get('timestamp', ''))}</p>
      </div>
      <div style="background:white;padding:24px;border-radius:0 0 8px 8px;"><table style="width:100%;border-collapse:collapse;">
        {row('Caller ID', 'caller_number')}{row('Name', 'name')}{row('Phone', 'phone')}{row('Email', 'email')}
        {row('RV Details', 'rv_details')}{row('Goals', 'goals')}{row('Current Setup', 'current_setup')}
        {row('Location', 'location')}{row('Summary', 'summary', last=True)}
      </table>{"<hr style='margin:20px 0;border:none;border-top:1px solid #eee;'><p style='font-size:13px;color:#444;white-space:pre-wrap;'><strong>Full Transcript:</strong><br>" + transcript + "</p>" if transcript else ""}</div>
    </div>"""
    try:
        subject = "New Lead — Cascade RV Solar Solutions" if lead.get("name") else "Missed/Partial Call — Cascade RV Solar Solutions"
        message = Mail(from_email=OWNER_EMAIL, to_emails=OWNER_EMAIL, subject=subject, html_content=html_content)
        SendGridAPIClient(SENDGRID_API_KEY).send(message)
        logger.info("Lead notification email sent")
    except Exception:
        logger.error("Failed to send lead notification email")


@app.on_event("startup")
def initialize_database() -> None:
    init_db()
    logger.info("Durable call storage initialized")


# ── Operator routes (HTTP Basic authentication required) ──────────────────────
@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def dashboard() -> str:
    is_active, toggled_at = await asyncio.to_thread(get_receptionist_state)
    lead_count = await asyncio.to_thread(call_count)
    sc = "#22c55e" if is_active else "#ef4444"
    status_text = "ACTIVE — Answering Calls" if is_active else "INACTIVE — Calls go to voicemail"
    btn_label = "Turn OFF Receptionist" if is_active else "Turn ON Receptionist"
    btn_color = "#ef4444" if is_active else "#22c55e"
    btn_action = "off" if is_active else "on"
    return f"""<!DOCTYPE html><html><head><title>Cascade RV Solar — AI Receptionist</title>
    <meta name="viewport" content="width=device-width,initial-scale=1"><style>
    *{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}.card{{background:#1e293b;border-radius:16px;padding:40px;max-width:560px;width:100%;box-shadow:0 25px 50px rgba(0,0,0,.4)}}.logo{{font-size:13px;color:#64748b;text-transform:uppercase;letter-spacing:2px;margin-bottom:8px}}h1{{font-size:24px;margin-bottom:32px}}.badge{{display:flex;align-items:center;gap:10px;background:#0f172a;border-radius:12px;padding:16px 24px;margin-bottom:32px}}.dot{{width:14px;height:14px;border-radius:50%;background:{sc};box-shadow:0 0 10px {sc}}}.st{{font-size:15px;font-weight:600;color:{sc}}}.stats{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:32px}}.stat{{background:#0f172a;border-radius:10px;padding:16px;text-align:center}}.sn{{font-size:28px;font-weight:700;color:#38bdf8}}.sl{{font-size:12px;color:#64748b;margin-top:4px}}.btn,.lbtn{{display:block;width:100%;padding:16px;color:white;border-radius:12px;font-size:16px;font-weight:700;text-decoration:none;text-align:center;margin-bottom:16px}}.btn{{background:{btn_color}}}.lbtn{{background:transparent;color:#38bdf8;border:2px solid #38bdf8}}.ta{{font-size:12px;color:#64748b;margin-top:12px;text-align:center}}</style></head><body><div class="card">
    <div class="logo">Cascade RV Solar Solutions</div><h1>AI Receptionist Dashboard</h1><div class="badge"><div class="dot"></div><div class="st">{status_text}</div></div><div class="stats"><div class="stat"><div class="sn">{lead_count}</div><div class="sl">Calls Persisted</div></div><div class="stat"><div class="sn">Realtime</div><div class="sl">Voice Engine</div></div></div><a href="/toggle?action={btn_action}" class="btn">{btn_label}</a><a href="/leads" class="lbtn">View Captured Leads</a><div class="ta">Last toggled: {html.escape(toggled_at)}</div></div></body></html>"""


@app.get("/toggle", dependencies=[Depends(require_admin)])
async def toggle_receptionist(action: str = "on") -> Response:
    await asyncio.to_thread(set_receptionist_state, action.lower() == "on")
    return Response(content='<html><head><meta http-equiv="refresh" content="0;url=/"></head></html>', media_type="text/html")


# ── Twilio webhooks (not Basic-auth protected; protected by optional signature) ─
@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call(request: Request) -> Response:
    valid, form = await _validated_twilio_form(request)
    if not valid:
        return Response(status_code=status.HTTP_403_FORBIDDEN)
    caller_from = form.get("From") or request.query_params.get("From", "")
    call_sid = form.get("CallSid") or request.query_params.get("CallSid", "")
    call_status = form.get("CallStatus") or request.query_params.get("CallStatus") or "initiated"
    if call_sid:
        try:
            await asyncio.to_thread(upsert_call, call_sid, caller_number=caller_from or None, status=call_status)
            if caller_from:
                pending_calls[call_sid] = caller_from
        except Exception:
            logger.error("Unable to persist incoming call webhook")
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    logger.info("Incoming call webhook accepted")

    is_active, _ = await asyncio.to_thread(get_receptionist_state)
    response = VoiceResponse()
    if not is_active:
        response.say("Thank you for calling Cascade RV Solar Solutions. We are currently unavailable. Please leave a message after the tone and we will return your call shortly.", voice="Polly.Matthew-Neural")
        response.record(max_length=120, play_beep=True)
        response.hangup()
        return Response(content=str(response), media_type="application/xml")
    connect = Connect()
    connect.stream(url=_media_stream_url(request))
    response.append(connect)
    return Response(content=str(response), media_type="application/xml")


@app.post("/call-status")
async def call_status(request: Request) -> Response:
    valid, form = await _validated_twilio_form(request)
    if not valid:
        return Response(status_code=status.HTTP_403_FORBIDDEN)
    call_sid = form.get("CallSid", "")
    call_status_value = form.get("CallStatus", "")
    if not call_sid or not call_status_value:
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    try:
        await asyncio.to_thread(
            upsert_call,
            call_sid,
            caller_number=form.get("From") or None,
            status=call_status_value,
            occurred_at=_parse_twilio_timestamp(form.get("Timestamp")),
        )
    except Exception:
        logger.error("Unable to persist Twilio call status webhook")
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    logger.info("Twilio call status persisted")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ── Twilio Media Streams ↔ OpenAI Realtime bridge ──────────────────────────────
@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket) -> None:
    if not _valid_twilio_websocket(websocket):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    logger.info("Twilio Media Stream connected")

    lead: dict = {"call_sid": None, "caller_number": None, "transcript": []}
    stream_sid: str | None = None
    stop_event = asyncio.Event()

    async def capture_transcript(speaker: str, transcript: str) -> None:
        text = transcript.strip()
        if not text:
            return
        lead["transcript"].append(f"{speaker}: {text}")
        if lead.get("call_sid"):
            try:
                await asyncio.to_thread(append_transcript_line, lead["call_sid"], speaker, text)
            except Exception:
                logger.error("Unable to persist transcript line")

    async def finalize() -> None:
        call_sid = lead.get("call_sid")
        if not call_sid:
            return
        try:
            extracted = await asyncio.to_thread(_gpt4o_extract, lead["transcript"], lead.get("caller_number", ""))
            await asyncio.to_thread(update_extracted_fields, call_sid, extracted, "complete")
            saved_lead = await asyncio.to_thread(get_call_view, call_sid)
            if saved_lead:
                await asyncio.to_thread(send_lead_email, saved_lead)
            logger.info("Call finalization persisted")
        except Exception:
            logger.error("Call finalization failed")

    try:
        async with websockets.connect(
            OPENAI_REALTIME_URL,
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            open_timeout=15,
        ) as openai_ws:
            await _send_session_update(openai_ws)
            await _send_initial_greeting(openai_ws)

            async def receive_from_twilio() -> None:
                nonlocal stream_sid
                try:
                    async for raw in websocket.iter_text():
                        data = json.loads(raw)
                        event = data.get("event")
                        if event == "start":
                            start_data = data.get("start", {})
                            stream_sid = start_data.get("streamSid")
                            call_sid = start_data.get("callSid", "")
                            lead["call_sid"] = call_sid
                            caller = pending_calls.pop(call_sid, None)
                            if not caller:
                                caller = await asyncio.to_thread(caller_for_call, call_sid)
                            if not caller:
                                caller = (start_data.get("customParameters", {}).get("caller") or start_data.get("customParameters", {}).get("From") or start_data.get("from") or start_data.get("From") or start_data.get("caller"))
                            if caller:
                                lead["caller_number"] = caller
                            if call_sid:
                                await asyncio.to_thread(attach_stream, call_sid, stream_sid, caller)
                            logger.info("Twilio Media Stream started")
                        elif event == "media":
                            pcm_b64 = ulaw8k_to_pcm24k(data.get("media", {}).get("payload", ""))
                            if pcm_b64:
                                try:
                                    await openai_ws.send(json.dumps({"type": "input_audio_buffer.append", "audio": pcm_b64}))
                                except Exception:
                                    pass
                        elif event == "stop":
                            logger.info("Twilio Media Stream stop received")
                            break
                except Exception:
                    logger.error("Error receiving Twilio Media Stream frames")
                finally:
                    stop_event.set()
                    try:
                        await openai_ws.close()
                    except Exception:
                        pass

            async def send_to_twilio() -> None:
                nonlocal stream_sid
                pcm_buffer = b""
                chunk_size = 960  # ~20 ms at 24 kHz, 16-bit mono
                try:
                    async for raw in openai_ws:
                        if stop_event.is_set():
                            break
                        message = json.loads(raw)
                        event_type = message.get("type")
                        if event_type == "response.output_audio.delta" and message.get("delta"):
                            try:
                                pcm_buffer += base64.b64decode(message["delta"])
                            except Exception:
                                pass
                            while len(pcm_buffer) >= chunk_size and stream_sid:
                                chunk, pcm_buffer = pcm_buffer[:chunk_size], pcm_buffer[chunk_size:]
                                ulaw = pcm24k_to_ulaw8k_bytes(chunk)
                                if ulaw:
                                    try:
                                        await websocket.send_text(json.dumps({"event": "media", "streamSid": stream_sid, "media": {"payload": base64.b64encode(ulaw).decode()}}))
                                    except Exception:
                                        pass
                        elif event_type == "response.output_audio.done":
                            if pcm_buffer and stream_sid:
                                ulaw = pcm24k_to_ulaw8k_bytes(pcm_buffer)
                                if ulaw:
                                    try:
                                        await websocket.send_text(json.dumps({"event": "media", "streamSid": stream_sid, "media": {"payload": base64.b64encode(ulaw).decode()}}))
                                    except Exception:
                                        pass
                            pcm_buffer = b""
                        elif event_type == "input_audio_buffer.speech_started" and stream_sid:
                            try:
                                await websocket.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))
                            except Exception:
                                pass
                        elif event_type == "response.output_audio_transcript.done":
                            await capture_transcript("Alex", message.get("transcript", ""))
                        elif event_type == "response.done":
                            for item in message.get("response", {}).get("output", []):
                                if item.get("type") == "message" and item.get("role") == "assistant":
                                    for content in item.get("content", []):
                                        transcript = content.get("transcript", "") if content.get("type") == "audio" else ""
                                        entry = f"Alex: {transcript.strip()}" if transcript else ""
                                        if entry and entry not in lead["transcript"]:
                                            await capture_transcript("Alex", transcript)
                        elif event_type == "conversation.item.input_audio_transcription.completed":
                            await capture_transcript("Caller", message.get("transcript", ""))
                        elif event_type == "conversation.item.added":
                            item = message.get("item", {})
                            if item.get("role") == "user":
                                for content in item.get("content", []):
                                    text = content.get("text", "") if content.get("type") == "input_text" else ""
                                    if text and not text.startswith("Greet the caller warmly"):
                                        await capture_transcript("Caller", text)
                        elif event_type == "error":
                            logger.error("OpenAI Realtime returned an error event")
                except Exception:
                    logger.error("Error forwarding OpenAI Realtime frames")

            try:
                await asyncio.gather(receive_from_twilio(), send_to_twilio())
            finally:
                await finalize()
    except Exception:
        logger.error("WebSocket session error")
    finally:
        logger.info("Twilio Media Stream closed")


async def _send_session_update(openai_ws) -> None:
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": SYSTEM_MESSAGE,
            "tools": [],
            "tool_choice": "none",
            "audio": {
                "input": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "transcription": {"model": "whisper-1", "language": "en"},
                    "turn_detection": {"type": "server_vad", "threshold": 0.8, "prefix_padding_ms": 300, "silence_duration_ms": 1200, "create_response": True, "interrupt_response": True},
                },
                "output": {"format": {"type": "audio/pcm", "rate": 24000}, "voice": VOICE, "speed": 1.0},
            },
        },
    }
    await openai_ws.send(json.dumps(session_update))
    logger.info("OpenAI Realtime session configured")


async def _send_initial_greeting(openai_ws) -> None:
    await openai_ws.send(json.dumps({
        "type": "conversation.item.create",
        "item": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Greet the caller warmly. Say: 'Hi, thanks for calling Cascade RV Solar Solutions. Jason is currently working on an RV or with another customer right now, so you've reached Alex, his AI assistant. Are you looking for information on a new system, or do you need some technical advice?'"}]},
    }))
    await openai_ws.send(json.dumps({"type": "response.create"}))
    logger.info("Initial greeting triggered")


# ── Authenticated leads dashboard ─────────────────────────────────────────────
@app.get("/leads", response_class=HTMLResponse, dependencies=[Depends(require_admin)])
async def get_leads() -> str:
    leads = await asyncio.to_thread(list_call_views)
    if not leads:
        rows_html = '<tr><td colspan="8" style="text-align:center;color:#64748b;padding:40px;">No calls captured yet.</td></tr>'
    else:
        rows = []
        for index, lead in enumerate(leads):
            bg = "#1e293b" if index % 2 == 0 else "#162032"
            def cell(key: str, fallback: str = "—") -> str:
                return html.escape(str(lead.get(key) or fallback), quote=True)
            timestamp = cell("timestamp", "")[:19].replace("T", " ")
            rows.append(f'<tr style="background:{bg};"><td>{timestamp}</td><td>{cell("status")}</td><td><strong>{cell("name", lead.get("caller_number") or "Unknown")}</strong></td><td>{cell("phone", lead.get("caller_number") or "—")}</td><td>{cell("email")}</td><td>{cell("rv_details")}</td><td>{cell("goals")}</td><td style="max-width:220px;word-wrap:break-word;">{cell("summary")}</td></tr>')
        rows_html = "".join(rows)
    return f'''<!DOCTYPE html><html><head><title>Leads — Cascade RV Solar Solutions</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>*{{box-sizing:border-box;margin:0;padding:0}}body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:30px 20px}}.hdr{{max-width:1300px;margin:0 auto 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}.hdr h1{{font-size:22px}}.sub{{font-size:13px;color:#64748b;margin-top:4px}}.back{{padding:10px 20px;background:#1e293b;color:#38bdf8;border:2px solid #38bdf8;border-radius:10px;text-decoration:none}}.badge{{display:inline-block;background:#38bdf820;color:#38bdf8;border-radius:20px;padding:4px 14px;font-size:13px;margin-left:12px}}.wrap{{max-width:1300px;margin:0 auto;overflow-x:auto;border-radius:14px}}table{{width:100%;border-collapse:collapse;font-size:14px}}thead tr{{background:#0f172a}}th{{padding:14px 16px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase;white-space:nowrap}}td{{padding:14px 16px;color:#cbd5e1;vertical-align:top;border-top:1px solid #1e293b}}</style></head><body><div class="hdr"><div><h1>Captured Leads <span class="badge">{len(leads)} total</span></h1><div class="sub">Cascade RV Solar Solutions — AI Receptionist</div></div><a href="/" class="back">&larr; Dashboard</a></div><div class="wrap"><table><thead><tr><th>Date &amp; Time</th><th>Status</th><th>Name</th><th>Phone</th><th>Email</th><th>RV Details</th><th>Goals</th><th>Summary</th></tr></thead><tbody>{rows_html}</tbody></table></div></body></html>'''


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
