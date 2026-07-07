"""
Cascade RV Solar Solutions — AI Receptionist
Uses OpenAI Realtime API (GA) + Twilio Media Streams for near-zero latency voice.

Audio pipeline:
  Twilio → G.711 µ-law 8kHz → upsample to PCM 16-bit 24kHz → OpenAI Realtime API
  OpenAI  → PCM 16-bit 24kHz → downsample to G.711 µ-law 8kHz → Twilio
"""

import os
import json
import base64
import asyncio
import audioop
import logging
import requests
from datetime import datetime

import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, Response
from twilio.twiml.voice_response import VoiceResponse, Connect
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cascade-receptionist")

# ── Environment variables ─────────────────────────────────────────────────────
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY", "")
TWILIO_ACCOUNT_SID  = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "")
OWNER_PHONE         = os.environ.get("OWNER_PHONE", "+15039190521")
OWNER_EMAIL         = os.environ.get("OWNER_EMAIL", "jhefley@cascadesolarrvsolutions.com")
SENDGRID_API_KEY    = os.environ.get("SENDGRID_API_KEY", "")
PORT                = int(os.environ.get("PORT", 8000))

# ── OpenAI Realtime GA config ─────────────────────────────────────────────────
OPENAI_REALTIME_URL = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2025-08-28"
VOICE = "echo"

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
    except Exception as e:
        logger.debug(f"ulaw→pcm error: {e}")
        return ""


def pcm24k_to_ulaw8k_bytes(pcm_bytes: bytes) -> bytes:
    try:
        pcm_8k, _ = audioop.ratecv(pcm_bytes, 2, 1, 24000, 8000, None)
        return audioop.lin2ulaw(pcm_8k, 2)
    except Exception as e:
        logger.debug(f"pcm→ulaw error: {e}")
        return b""


# ── App state ─────────────────────────────────────────────────────────────────
app = FastAPI()
receptionist_state = {"active": True, "toggled_at": "Never"}
lead_data_store = []
pending_calls: dict = {}  # call_sid -> caller phone number, set by /incoming-call


# ── GPT-4o lead extraction (sync, runs in thread pool) ───────────────────────
def _gpt4o_extract(transcript_lines: list, caller_number: str) -> dict:
    """Call GPT-4o via REST to extract lead fields and generate a call summary."""
    if not transcript_lines:
        return {}
    transcript_text = "\n".join(transcript_lines)
    prompt = f"""Analyze this phone call transcript between an AI receptionist (Alex) and a caller.
Extract the following fields. Return ONLY a JSON object with these exact keys (use null if not mentioned):
- name: Caller's first and last name (look closely at the greeting or when they are asked for contact info)
- phone: Phone number the caller explicitly stated (not the caller ID)
- email: Email address
- rv_details: RV type, year, make, and model
- goals: What they want to achieve (boondocking, off-grid AC, full-time living, etc.)
- current_setup: Their current electrical/solar setup
- location: Their location in Oregon
- summary: A 2-3 sentence summary of the caller's needs and the outcome of the call.

Transcript:
{transcript_text}"""

    try:
        resp = requests.post(
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
        data = resp.json()
        return json.loads(data["choices"][0]["message"]["content"])
    except Exception as e:
        logger.error(f"GPT-4o extraction error: {e}")
        return {"summary": f"Transcript captured but GPT-4o extraction failed: {e}"}


# ── Email notification ────────────────────────────────────────────────────────
def send_lead_email(lead: dict):
    if not SENDGRID_API_KEY:
        logger.warning("SendGrid API key not set — skipping email")
        return
    try:
        def row(label, key, last=False):
            border = "" if last else "border-bottom:1px solid #eee;"
            return (f'<tr><td style="padding:10px;{border}color:#666;width:140px;"><strong>{label}</strong></td>'
                    f'<td style="padding:10px;{border}">{lead.get(key) or "—"}</td></tr>')

        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:620px;margin:0 auto;background:#f8f9fa;padding:20px;border-radius:8px;">
          <div style="background:#1a3a5c;color:white;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;">&#128222; {'New Lead' if lead.get('name') else 'Missed/Partial Call'} — Cascade RV Solar Solutions</h2>
            <p style="margin:4px 0 0;opacity:0.8;font-size:14px;">Captured by your AI Receptionist &nbsp;·&nbsp; {lead.get('timestamp','')[:19].replace('T',' ')} UTC</p>
          </div>
          <div style="background:white;padding:24px;border-radius:0 0 8px 8px;">
            <table style="width:100%;border-collapse:collapse;">
              {row('Caller ID', 'caller_number')}
              {row('Name', 'name')}
              {row('Phone', 'phone')}
              {row('Email', 'email')}
              {row('RV Details', 'rv_details')}
              {row('Goals', 'goals')}
              {row('Current Setup', 'current_setup')}
              {row('Location', 'location')}
              {row('Summary', 'summary', last=True)}
            </table>
            {"<hr style='margin:20px 0;border:none;border-top:1px solid #eee;'><p style='font-size:13px;color:#444;white-space:pre-wrap;'><strong>Full Transcript:</strong><br>" + chr(10).join(lead.get('transcript', [])) + "</p>" if lead.get('transcript') else ""}
          </div>
          <p style="text-align:center;margin-top:16px;font-size:12px;color:#aaa;">— Cascade RV Solar Solutions AI Receptionist</p>
        </div>"""

        subject = (f"New Lead: {lead['name']} — Cascade RV Solar Solutions"
                   if lead.get("name")
                   else f"Missed/Partial Call: {lead.get('caller_number', 'Unknown')} — Cascade RV Solar Solutions")

        msg = Mail(from_email=OWNER_EMAIL, to_emails=OWNER_EMAIL,
                   subject=subject, html_content=html)
        SendGridAPIClient(SENDGRID_API_KEY).send(msg)
        logger.info(f"Lead email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send lead email: {e}")


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    is_active   = receptionist_state["active"]
    sc          = "#22c55e" if is_active else "#ef4444"
    status_text = "ACTIVE — Answering Calls" if is_active else "INACTIVE — Calls go to voicemail"
    btn_label   = "Turn OFF Receptionist" if is_active else "Turn ON Receptionist"
    btn_color   = "#ef4444" if is_active else "#22c55e"
    btn_action  = "off" if is_active else "on"
    lead_count  = len(lead_data_store)
    toggled_at  = receptionist_state["toggled_at"]
    return f"""<!DOCTYPE html><html><head>
    <title>Cascade RV Solar — AI Receptionist</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
      *{{box-sizing:border-box;margin:0;padding:0}}
      body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}}
      .card{{background:#1e293b;border-radius:16px;padding:40px;max-width:560px;width:100%;box-shadow:0 25px 50px rgba(0,0,0,.4)}}
      .logo{{font-size:13px;color:#64748b;text-transform:uppercase;letter-spacing:2px;margin-bottom:8px}}
      h1{{font-size:24px;font-weight:700;color:#f1f5f9;margin-bottom:32px}}
      .badge{{display:inline-flex;align-items:center;gap:10px;background:#0f172a;border-radius:12px;padding:16px 24px;margin-bottom:32px;width:100%}}
      .dot{{width:14px;height:14px;border-radius:50%;background:{sc};box-shadow:0 0 10px {sc};flex-shrink:0}}
      .st{{font-size:15px;font-weight:600;color:{sc}}}
      .stats{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:32px}}
      .stat{{background:#0f172a;border-radius:10px;padding:16px;text-align:center}}
      .sn{{font-size:28px;font-weight:700;color:#38bdf8}}
      .sl{{font-size:12px;color:#64748b;margin-top:4px}}
      .btn{{display:block;width:100%;padding:16px;background:{btn_color};color:white;border:none;border-radius:12px;font-size:16px;font-weight:700;cursor:pointer;text-decoration:none;text-align:center;margin-bottom:16px}}
      .btn:hover{{opacity:.85}}
      .lbtn{{display:block;width:100%;padding:14px;background:transparent;color:#38bdf8;border:2px solid #38bdf8;border-radius:12px;font-size:15px;font-weight:600;text-decoration:none;text-align:center}}
      .lbtn:hover{{background:#38bdf820}}
      .footer{{margin-top:24px;font-size:12px;color:#475569;text-align:center}}
      .ta{{font-size:12px;color:#475569;margin-top:12px;text-align:center}}
    </style></head><body>
    <div class="card">
      <div class="logo">Cascade RV Solar Solutions</div>
      <h1>AI Receptionist Dashboard</h1>
      <div class="badge"><div class="dot"></div><div class="st">{status_text}</div></div>
      <div class="stats">
        <div class="stat"><div class="sn">{lead_count}</div><div class="sl">Leads Captured</div></div>
        <div class="stat"><div class="sn">Realtime</div><div class="sl">Voice Engine</div></div>
      </div>
      <a href="/toggle?action={btn_action}" class="btn">{btn_label}</a>
      <a href="/leads" class="lbtn">View Captured Leads</a>
      <div class="ta">Last toggled: {toggled_at}</div>
      <div class="footer">(503) 919-0521 &nbsp;·&nbsp; Prineville, OR &nbsp;·&nbsp; cascadesolarrvsolutions.com</div>
    </div></body></html>"""


@app.get("/toggle")
async def toggle_receptionist(action: str = "on"):
    receptionist_state["active"] = (action.lower() == "on")
    receptionist_state["toggled_at"] = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    return Response(content='<html><head><meta http-equiv="refresh" content="0;url=/"></head></html>',
                    media_type="text/html")


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call(request: Request):
    host = request.headers.get("host", "")
    # Capture caller number from Twilio's POST body before the WebSocket even opens
    form = await request.form() if request.method == "POST" else {}
    caller_from = form.get("From") or request.query_params.get("From", "")
    call_sid    = form.get("CallSid") or request.query_params.get("CallSid", "")
    if caller_from and call_sid:
        pending_calls[call_sid] = caller_from
        logger.info(f"Incoming call from {caller_from} (SID: {call_sid})")

    response = VoiceResponse()
    if not receptionist_state["active"]:
        response.say("Thank you for calling Cascade RV Solar Solutions. We are currently unavailable. "
                     "Please leave a message after the tone and we will return your call shortly.",
                     voice="Polly.Matthew-Neural")
        response.record(max_length=120, play_beep=True)
        response.hangup()
        return Response(content=str(response), media_type="application/xml")
    connect = Connect()
    connect.stream(url=f"wss://{host}/media-stream")
    response.append(connect)
    return Response(content=str(response), media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    """Bridge Twilio Media Streams ↔ OpenAI Realtime API (GA)."""
    await websocket.accept()
    logger.info("Twilio Media Stream connected")

    # Lead record — transcript is a plain list of strings
    lead = {
        "timestamp":     datetime.utcnow().isoformat(),
        "call_sid":      None,
        "caller_number": None,
        "phone":         None,
        "name":          None,
        "email":         None,
        "rv_details":    None,
        "goals":         None,
        "current_setup": None,
        "location":      None,
        "summary":       None,
        "transcript":    [],   # ← always a list, never None
    }
    stream_sid = None
    stop_event = asyncio.Event()  # signals send_to_twilio to exit when Twilio disconnects

    try:
        async with websockets.connect(
            OPENAI_REALTIME_URL,
            additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            open_timeout=15,
        ) as openai_ws:

            await _send_session_update(openai_ws)
            await _send_initial_greeting(openai_ws)

            # ── Twilio → OpenAI ────────────────────────────────────
            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for raw in websocket.iter_text():
                        data = json.loads(raw)
                        event = data.get("event")

                        if event == "start":
                            start_data = data["start"]
                            stream_sid = start_data["streamSid"]
                            call_sid_val = start_data.get("callSid", "")
                            lead["call_sid"] = call_sid_val
                            # Log full payload to diagnose caller number location
                            logger.info(f"START keys: {list(start_data.keys())}")
                            logger.info(f"START customParameters: {start_data.get('customParameters')}")
                            logger.info(f"START callSid: {call_sid_val}")
                            # 1. Best source: captured from the /incoming-call POST body
                            caller = pending_calls.pop(call_sid_val, None)
                            # 2. Fallback: check every known field in the start payload
                            if not caller:
                                caller = (
                                    start_data.get("customParameters", {}).get("caller")
                                    or start_data.get("customParameters", {}).get("From")
                                    or start_data.get("from")
                                    or start_data.get("From")
                                    or start_data.get("caller")
                                )
                            if caller:
                                lead["caller_number"] = caller
                                lead["phone"] = caller
                            logger.info(f"Stream started: {stream_sid} caller={caller}")

                        elif event == "media":
                            pcm_b64 = ulaw8k_to_pcm24k(data["media"]["payload"])
                            if pcm_b64:
                                try:
                                    await openai_ws.send(json.dumps({
                                        "type": "input_audio_buffer.append",
                                        "audio": pcm_b64,
                                    }))
                                except Exception:
                                    pass

                        elif event == "stop":
                            logger.info("Twilio stream stop received")
                            break
                except Exception as e:
                    logger.error(f"receive_from_twilio error: {e}")
                finally:
                    # Close the OpenAI WebSocket so send_to_twilio's async-for loop exits immediately
                    stop_event.set()
                    logger.info("stop_event set — closing OpenAI WebSocket")
                    try:
                        await openai_ws.close()
                    except Exception:
                        pass

            # ── OpenAI → Twilio ──────────────────────────────────────────────
            async def send_to_twilio():
                nonlocal stream_sid
                pcm_buffer = b""
                CHUNK = 960  # ~20 ms at 24 kHz 16-bit mono
                try:
                    async for raw in openai_ws:
                        if stop_event.is_set():
                            logger.info("stop_event detected — exiting send_to_twilio")
                            break
                        msg = json.loads(raw)
                        etype = msg.get("type")

                        if etype not in ("response.output_audio.delta",):
                            logger.info(f"OpenAI event: {etype}")

                        # ── Audio streaming ──────────────────────────────────
                        if etype == "response.output_audio.delta" and msg.get("delta"):
                            try:
                                pcm_buffer += base64.b64decode(msg["delta"])
                            except Exception:
                                pass
                            while len(pcm_buffer) >= CHUNK and stream_sid:
                                chunk, pcm_buffer = pcm_buffer[:CHUNK], pcm_buffer[CHUNK:]
                                ulaw = pcm24k_to_ulaw8k_bytes(chunk)
                                if ulaw:
                                    try:
                                        await websocket.send_text(json.dumps({
                                            "event": "media",
                                            "streamSid": stream_sid,
                                            "media": {"payload": base64.b64encode(ulaw).decode()},
                                        }))
                                    except Exception:
                                        pass

                        elif etype == "response.output_audio.done":
                            if pcm_buffer and stream_sid:
                                ulaw = pcm24k_to_ulaw8k_bytes(pcm_buffer)
                                if ulaw:
                                    try:
                                        await websocket.send_text(json.dumps({
                                            "event": "media",
                                            "streamSid": stream_sid,
                                            "media": {"payload": base64.b64encode(ulaw).decode()},
                                        }))
                                    except Exception:
                                        pass
                            pcm_buffer = b""

                        # ── Barge-in ─────────────────────────────────────────
                        elif etype == "input_audio_buffer.speech_started" and stream_sid:
                            try:
                                await websocket.send_text(json.dumps({"event": "clear", "streamSid": stream_sid}))
                            except Exception:
                                pass

                        # ── Transcript: Alex's words (streaming delta events) ──
                        elif etype == "response.output_audio_transcript.done":
                            t = msg.get("transcript", "")
                            if t:
                                lead["transcript"].append(f"Alex: {t}")
                                logger.info(f"Alex transcript captured ({len(t)} chars): {t[:60]}")

                        # ── Also capture from response.done as fallback ───────
                        elif etype == "response.done":
                            for item in msg.get("response", {}).get("output", []):
                                if item.get("type") == "message" and item.get("role") == "assistant":
                                    for c in item.get("content", []):
                                        if c.get("type") == "audio" and c.get("transcript"):
                                            # Only add if not already captured via transcript.done
                                            entry = f"Alex: {c['transcript']}"
                                            if entry not in lead["transcript"]:
                                                lead["transcript"].append(entry)
                                                logger.info(f"Alex transcript (response.done fallback): {c['transcript'][:60]}")

                        # ── Transcript: Caller's words via Whisper transcription ──────
                        # This event fires after Whisper completes transcription of caller audio
                        elif etype == "conversation.item.input_audio_transcription.completed":
                            t = (msg.get("transcript") or "").strip()
                            logger.info(f"Whisper transcription.completed full msg: {str(msg)[:400]}")
                            if t:
                                lead["transcript"].append(f"Caller: {t}")
                                logger.info(f"Caller transcript captured (Whisper): {t[:80]}")

                        # conversation.item.added fires immediately when item is created;
                        # transcript is always None here for audio items, so we only use
                        # it to capture injected text items (but filter out our own greeting prompt)
                        elif etype == "conversation.item.added":
                            item = msg.get("item", {})
                            if item.get("role") == "user":
                                content = item.get("content", [])
                                for c in content:
                                    if c.get("type") == "input_text" and c.get("text"):
                                        text = c["text"].strip()
                                        # Filter out the injected greeting instruction
                                        if not text.startswith("Greet the caller warmly"):
                                            lead["transcript"].append(f"Caller: {text}")
                                            logger.info(f"Caller transcript captured (input_text): {text[:80]}")

                        elif etype == "error":
                            logger.error(f"OpenAI error event: {msg.get('error')}")

                except Exception as e:
                    logger.error(f"send_to_twilio error: {e}")

            # ── Run both tasks, then ALWAYS finalize ─────────────────────────
            finalized = False

            async def finalize():
                nonlocal finalized
                if finalized:
                    return
                finalized = True
                logger.info(f"Finalizing lead. Transcript lines: {len(lead['transcript'])}")
                loop = asyncio.get_event_loop()
                extracted = await loop.run_in_executor(
                    None, _gpt4o_extract, lead["transcript"], lead.get("caller_number", "")
                )
                for key in ("name", "email", "rv_details", "goals", "current_setup", "location", "summary"):
                    if extracted.get(key):
                        lead[key] = extracted[key]
                if extracted.get("phone"):
                    lead["phone"] = extracted["phone"]
                lead_data_store.append(lead)
                logger.info(f"Lead saved: {lead.get('name') or lead.get('caller_number')}")
                send_lead_email(lead)

            try:
                await asyncio.gather(receive_from_twilio(), send_to_twilio())
            except Exception as e:
                logger.error(f"gather error: {e}")

            # Finalize runs whether gather completed normally, raised, or was cancelled
            try:
                await finalize()
            except Exception as fe:
                logger.error(f"finalize error: {fe}")

    except Exception as e:
        logger.error(f"WebSocket session error: {e}")
    finally:
        logger.info("WebSocket session closed")


async def _send_session_update(openai_ws):
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
                    "transcription": {
                        "model": "whisper-1",
                        "language": "en",
                    },
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.8,
                        "prefix_padding_ms": 300,
                        "silence_duration_ms": 1200,
                        "create_response": True,
                        "interrupt_response": True,
                    },
                },
                "output": {
                    "format": {"type": "audio/pcm", "rate": 24000},
                    "voice": VOICE,
                    "speed": 1.0,
                },
            },
            # input_audio_transcription is now correctly nested at audio.input.transcription (GA format)
        },
    }
    await openai_ws.send(json.dumps(session_update))
    logger.info("Session update sent")


async def _send_initial_greeting(openai_ws):
    await openai_ws.send(json.dumps({
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "Greet the caller warmly. Say: 'Hi, thanks for calling Cascade RV Solar Solutions. Jason is currently working on an RV or with another customer right now, so you've reached Alex, his AI assistant. Are you looking for information on a new system, or do you need some technical advice?'",
            }],
        },
    }))
    await openai_ws.send(json.dumps({"type": "response.create"}))
    logger.info("Initial greeting triggered")


# ── Leads dashboard ───────────────────────────────────────────────────────────
@app.get("/leads", response_class=HTMLResponse)
async def get_leads():
    leads = lead_data_store
    total = len(leads)
    if total == 0:
        rows_html = '<tr><td colspan="7" style="text-align:center;color:#64748b;padding:40px;">No leads captured yet.</td></tr>'
    else:
        rows_html = ""
        for i, ld in enumerate(reversed(leads)):
            ts      = ld.get("timestamp", "")[:19].replace("T", " ")
            name    = ld.get("name") or ld.get("caller_number", "Unknown")
            phone   = ld.get("phone") or ld.get("caller_number", "—")
            email   = ld.get("email") or "—"
            rv      = ld.get("rv_details") or "—"
            goals   = ld.get("goals") or "—"
            summary = ld.get("summary") or "—"
            bg      = "#1e293b" if i % 2 == 0 else "#162032"
            rows_html += f"""<tr style="background:{bg};">
                <td>{ts}</td><td><strong>{name}</strong></td><td>{phone}</td>
                <td>{email}</td><td>{rv}</td><td>{goals}</td>
                <td style="max-width:220px;word-wrap:break-word;">{summary}</td>
            </tr>"""

    return f"""<!DOCTYPE html><html><head>
    <title>Leads — Cascade RV Solar Solutions</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
      *{{box-sizing:border-box;margin:0;padding:0}}
      body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:30px 20px}}
      .hdr{{max-width:1200px;margin:0 auto 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}
      .hdr h1{{font-size:22px;font-weight:700;color:#f1f5f9}}
      .sub{{font-size:13px;color:#64748b;margin-top:4px}}
      .back{{padding:10px 20px;background:#1e293b;color:#38bdf8;border:2px solid #38bdf8;border-radius:10px;text-decoration:none;font-size:14px;font-weight:600}}
      .badge{{display:inline-block;background:#38bdf820;color:#38bdf8;border-radius:20px;padding:4px 14px;font-size:13px;font-weight:700;margin-left:12px}}
      .wrap{{max-width:1200px;margin:0 auto;overflow-x:auto;border-radius:14px;box-shadow:0 10px 40px rgba(0,0,0,.4)}}
      table{{width:100%;border-collapse:collapse;font-size:14px}}
      thead tr{{background:#0f172a}}
      thead th{{padding:14px 16px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:1px;font-weight:600;white-space:nowrap}}
      tbody td{{padding:14px 16px;color:#cbd5e1;vertical-align:top;border-top:1px solid #1e293b}}
      tbody tr:hover td{{background:#1e3a5f!important}}
    </style></head><body>
    <div class="hdr">
      <div><h1>Captured Leads <span class="badge">{total} total</span></h1>
      <div class="sub">Cascade RV Solar Solutions — AI Receptionist</div></div>
      <a href="/" class="back">&larr; Dashboard</a>
    </div>
    <div class="wrap"><table>
      <thead><tr>
        <th>Date &amp; Time</th><th>Name</th><th>Phone</th>
        <th>Email</th><th>RV Details</th><th>Goals</th><th>Summary</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table></div></body></html>"""


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
