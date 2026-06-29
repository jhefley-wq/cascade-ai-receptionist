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
from datetime import datetime

import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, Response
from twilio.twiml.voice_response import VoiceResponse, Connect, Stream
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# ── Logging ──────────────────────────────────────────────────────────────────
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
VOICE = "echo"   # Deep, calm, professional male voice (GA API supported voices)

SYSTEM_MESSAGE = """You are Alex, the professional AI receptionist for Cascade RV Solar Solutions, 
a mobile RV solar installation company based in Prineville, Oregon, owned by Jason Hefley.

PERSONALITY: Professional, warm, knowledgeable, and concise. You speak like a real person — 
natural, friendly, and helpful. Keep responses SHORT — 1-3 sentences maximum unless the caller 
asks for detailed information. Do not ramble.

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
- Preferred brands: Victron, Renogy, EPOCH batteries

PRICING & TIMELINE:
- Free consultations available
- Typical installation: 4-5 days of work
- Booking lead time: approximately 4-6 weeks out
- 1-year labor warranty on all installations

FREQUENTLY ASKED QUESTIONS:
Q: What areas do you serve?
A: We serve all of Oregon. We're mobile, so we come to your location.

Q: How much does a solar installation cost?
A: It varies based on system size and complexity. Jason offers free consultations to provide accurate quotes. Would you like to schedule one?

Q: How long does installation take?
A: Most installations take 4-5 days. We're typically booked about 4-6 weeks out.

Q: What brands do you use?
A: We work with premium brands including Victron, Renogy, and EPOCH batteries.

Q: Do you offer financing?
A: Yes, we offer financing through Enhancify. Jason can walk you through the options during a consultation.

Q: Do you work on all types of RVs?
A: Yes — motorhomes, fifth wheels, travel trailers, toy haulers, and more.

LEAD CAPTURE & QUALIFICATION:
When a caller wants a callback, consultation, or to leave a message, you MUST FIRST ask qualifying questions before asking for their contact info.
Ask these naturally, one at a time:
1. What type of RV do they have? (Motorhome, fifth wheel, travel trailer, etc.)
2. What is the year, make, and model?
3. What are they trying to achieve? (Boondocking, full-time living, running AC off-grid, etc.)
4. What is their current electrical setup?
5. Where are they located in Oregon?

Only AFTER gathering this context, transition naturally to collecting their contact info:
1. Their full name
2. Best phone number to reach them
3. Email address (optional but helpful)

After collecting their info, confirm it back to them and let them know Jason will be in touch soon.

IMPORTANT RULES:
- Never make up prices or specific technical specs you are not sure about
- If asked something you don't know, say Jason will be happy to discuss it during a consultation
- Always be warm and professional
- Keep answers brief and conversational
- If the caller seems to be in an emergency (e.g., electrical issue, fire risk), advise them to call 911 or a licensed electrician immediately"""

# ── Audio conversion helpers ──────────────────────────────────────────────────
# Twilio sends/receives G.711 µ-law at 8kHz.
# OpenAI GA Realtime API uses PCM 16-bit signed little-endian at 24kHz.

def ulaw8k_to_pcm24k(ulaw_b64: str) -> str:
    """Convert base64 G.711 µ-law 8kHz → base64 PCM 16-bit 24kHz."""
    try:
        ulaw_bytes = base64.b64decode(ulaw_b64)
        # µ-law → linear PCM 16-bit
        pcm_8k = audioop.ulaw2lin(ulaw_bytes, 2)
        # 8kHz → 24kHz (3x upsample)
        pcm_24k, _ = audioop.ratecv(pcm_8k, 2, 1, 8000, 24000, None)
        return base64.b64encode(pcm_24k).decode("utf-8")
    except Exception as e:
        logger.debug(f"ulaw→pcm error: {e}")
        return ""


def pcm24k_to_ulaw8k(pcm_b64: str) -> str:
    """Convert base64 PCM 16-bit 24kHz → base64 G.711 µ-law 8kHz."""
    try:
        pcm_bytes = base64.b64decode(pcm_b64)
        # 24kHz → 8kHz (3x downsample)
        pcm_8k, _ = audioop.ratecv(pcm_bytes, 2, 1, 24000, 8000, None)
        # linear PCM 16-bit → µ-law
        ulaw_8k = audioop.lin2ulaw(pcm_8k, 2)
        return base64.b64encode(ulaw_8k).decode("utf-8")
    except Exception as e:
        logger.debug(f"pcm→ulaw error: {e}")
        return ""


# ── App state ─────────────────────────────────────────────────────────────────
app = FastAPI()
receptionist_state = {"active": True, "toggled_at": "Never"}
lead_data_store = []


# ── Email notification ────────────────────────────────────────────────────────
def send_lead_email(lead: dict):
    """Send lead notification email via SendGrid."""
    if not SENDGRID_API_KEY:
        logger.warning("SendGrid API key not set — skipping email")
        return
    try:
        html = f"""
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;background:#f8f9fa;padding:20px;border-radius:8px;">
          <div style="background:#1a3a5c;color:white;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;">&#128222; New Lead — Cascade RV Solar Solutions</h2>
            <p style="margin:4px 0 0;opacity:0.8;font-size:14px;">Captured by your AI Receptionist</p>
          </div>
          <div style="background:white;padding:24px;border-radius:0 0 8px 8px;">
            <table style="width:100%;border-collapse:collapse;">
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;width:140px;"><strong>Caller ID</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('caller_number','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Name</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('name','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Phone</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('phone','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Email</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('email','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>RV Details</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('rv_details','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Goals</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('goals','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Current Setup</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('current_setup','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Budget</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('budget','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Location</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('location','—')}</td></tr>
              <tr><td style="padding:10px;border-bottom:1px solid #eee;color:#666;"><strong>Inquiry</strong></td><td style="padding:10px;border-bottom:1px solid #eee;">{lead.get('inquiry','—')}</td></tr>
              <tr><td style="padding:10px;color:#666;"><strong>Message</strong></td><td style="padding:10px;">{lead.get('message','—')}</td></tr>
            </table>
            <p style="margin-top:20px;font-size:12px;color:#999;">Received: {lead.get('timestamp','')[:19].replace('T',' ')} UTC</p>
            <p style="margin-top:4px;font-size:12px;color:#999;">Log in to your dashboard to view all leads.</p>
          </div>
          <p style="text-align:center;margin-top:16px;font-size:12px;color:#aaa;">— Cascade RV Solar Solutions AI Receptionist</p>
        </div>
        """
        # Determine subject based on how much info was captured
        if lead.get("name"):
            subject = f"New Lead: {lead['name']} — Cascade RV Solar Solutions"
        else:
            subject = f"Missed/Partial Call: {lead.get('caller_number', 'Unknown')} — Cascade RV Solar Solutions"
        message = Mail(
            from_email=OWNER_EMAIL,
            to_emails=OWNER_EMAIL,
            subject=subject,
            html_content=html,
        )
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        logger.info(f"Lead email sent for {lead.get('name')}")
    except Exception as e:
        logger.error(f"Failed to send lead email: {e}")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Status dashboard with ON/OFF toggle."""
    is_active = receptionist_state["active"]
    status_color = "#22c55e" if is_active else "#ef4444"
    status_text  = "ACTIVE — Answering Calls" if is_active else "INACTIVE — Calls go to voicemail"
    btn_label    = "Turn OFF Receptionist" if is_active else "Turn ON Receptionist"
    btn_color    = "#ef4444" if is_active else "#22c55e"
    btn_action   = "off" if is_active else "on"
    lead_count   = len(lead_data_store)
    toggled_at   = receptionist_state["toggled_at"]

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Cascade RV Solar Solutions — AI Receptionist</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: #0f172a;
                color: #e2e8f0;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }}
            .card {{
                background: #1e293b;
                border-radius: 16px;
                padding: 40px;
                max-width: 560px;
                width: 100%;
                box-shadow: 0 25px 50px rgba(0,0,0,0.4);
            }}
            .logo {{ font-size: 13px; color: #64748b; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 8px; }}
            h1 {{ font-size: 24px; font-weight: 700; color: #f1f5f9; margin-bottom: 32px; }}
            .status-badge {{
                display: inline-flex; align-items: center; gap: 10px;
                background: #0f172a; border-radius: 12px; padding: 16px 24px;
                margin-bottom: 32px; width: 100%;
            }}
            .dot {{ width: 14px; height: 14px; border-radius: 50%; background: {status_color}; box-shadow: 0 0 10px {status_color}; flex-shrink: 0; }}
            .status-text {{ font-size: 15px; font-weight: 600; color: {status_color}; }}
            .stats {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 32px; }}
            .stat {{ background: #0f172a; border-radius: 10px; padding: 16px; text-align: center; }}
            .stat-number {{ font-size: 28px; font-weight: 700; color: #38bdf8; }}
            .stat-label {{ font-size: 12px; color: #64748b; margin-top: 4px; }}
            .toggle-btn {{
                display: block; width: 100%; padding: 16px; background: {btn_color};
                color: white; border: none; border-radius: 12px; font-size: 16px;
                font-weight: 700; cursor: pointer; text-decoration: none; text-align: center;
                transition: opacity 0.2s; margin-bottom: 16px;
            }}
            .toggle-btn:hover {{ opacity: 0.85; }}
            .leads-btn {{
                display: block; width: 100%; padding: 14px; background: transparent;
                color: #38bdf8; border: 2px solid #38bdf8; border-radius: 12px;
                font-size: 15px; font-weight: 600; cursor: pointer; text-decoration: none;
                text-align: center; transition: all 0.2s;
            }}
            .leads-btn:hover {{ background: #38bdf820; }}
            .footer {{ margin-top: 24px; font-size: 12px; color: #475569; text-align: center; }}
            .toggled-at {{ font-size: 12px; color: #475569; margin-top: 12px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="card">
            <div class="logo">Cascade RV Solar Solutions</div>
            <h1>AI Receptionist Dashboard</h1>
            <div class="status-badge">
                <div class="dot"></div>
                <div class="status-text">{status_text}</div>
            </div>
            <div class="stats">
                <div class="stat">
                    <div class="stat-number">{lead_count}</div>
                    <div class="stat-label">Leads Captured</div>
                </div>
                <div class="stat">
                    <div class="stat-number">Realtime</div>
                    <div class="stat-label">Voice Engine</div>
                </div>
            </div>
            <a href="/toggle?action={btn_action}" class="toggle-btn">{btn_label}</a>
            <a href="/leads" class="leads-btn">View Captured Leads</a>
            <div class="toggled-at">Last toggled: {toggled_at}</div>
            <div class="footer">(503) 919-0521 &nbsp;·&nbsp; Prineville, OR &nbsp;·&nbsp; cascadesolarrvsolutions.com</div>
        </div>
    </body>
    </html>
    """


@app.get("/toggle")
async def toggle_receptionist(action: str = "on"):
    """Toggle the receptionist ON or OFF."""
    receptionist_state["active"] = (action.lower() == "on")
    receptionist_state["toggled_at"] = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    logger.info(f"Receptionist toggled: {'ACTIVE' if receptionist_state['active'] else 'INACTIVE'}")
    return Response(
        content='<html><head><meta http-equiv="refresh" content="0;url=/"></head></html>',
        media_type="text/html",
    )


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def incoming_call(request: Request):
    """Handle incoming Twilio calls."""
    host = request.headers.get("host", "")
    response = VoiceResponse()

    if not receptionist_state["active"]:
        response.say(
            "Thank you for calling Cascade RV Solar Solutions. We are currently unavailable. "
            "Please leave a message after the tone and we will return your call shortly.",
            voice="Polly.Matthew-Neural",
        )
        response.record(max_length=120, play_beep=True)
        response.hangup()
        return Response(content=str(response), media_type="application/xml")

    connect = Connect()
    connect.stream(url=f"wss://{host}/media-stream")
    response.append(connect)

    return Response(content=str(response), media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    """WebSocket endpoint bridging Twilio Media Streams ↔ OpenAI Realtime API (GA)."""
    await websocket.accept()
    logger.info("Twilio Media Stream connected")

    call_lead = {
        "timestamp": datetime.utcnow().isoformat(),
        "call_sid": None,
        "caller_number": None,
        "name": None,
        "phone": None,
        "email": None,
        "rv_details": None,
        "goals": None,
        "current_setup": None,
        "budget": None,
        "location": None,
        "inquiry": None,
        "message": None,
    }
    stream_sid = None

    try:
        async with websockets.connect(
            OPENAI_REALTIME_URL,
            additional_headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
            },
            open_timeout=15,
        ) as openai_ws:

            await _send_session_update(openai_ws)
            await _send_initial_greeting(openai_ws)

            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in websocket.iter_text():
                        data = json.loads(message)
                        event = data.get("event")

                        if event == "start":
                            stream_sid = data["start"]["streamSid"]
                            call_lead["call_sid"] = data["start"].get("callSid")
                            # Capture caller ID immediately on connect
                            caller = data["start"].get("customParameters", {}).get("caller") or data["start"].get("from")
                            if caller:
                                call_lead["caller_number"] = caller
                                call_lead["phone"] = caller
                            logger.info(f"Stream started: {stream_sid} from {caller}")

                        elif event == "media":
                            # Convert G.711 µ-law 8kHz → PCM 16-bit 24kHz
                            ulaw_b64 = data["media"]["payload"]
                            pcm_b64 = ulaw8k_to_pcm24k(ulaw_b64)
                            if pcm_b64:
                                try:
                                    await openai_ws.send(json.dumps({
                                        "type": "input_audio_buffer.append",
                                        "audio": pcm_b64,
                                    }))
                                except Exception:
                                    pass

                        elif event == "stop":
                            logger.info("Stream stopped")
                            break
                except Exception as e:
                    logger.error(f"Error receiving from Twilio: {e}")

            async def send_to_twilio():
                nonlocal stream_sid
                # Buffer to accumulate small PCM chunks before converting and sending
                pcm_buffer = b""
                # Send ~20ms of audio at a time: 24000 Hz * 2 bytes * 0.02s = 960 bytes
                CHUNK_SIZE = 960
                try:
                    async for openai_message in openai_ws:
                        response = json.loads(openai_message)
                        event_type = response.get("type")

                        # Log all event types to diagnose audio flow
                        if event_type not in ("response.output_audio.delta", "input_audio_buffer.append"):
                            logger.info(f"OpenAI event: {event_type}")

                        # Stream audio back to Twilio (convert PCM 24kHz → G.711 µ-law 8kHz)
                        # GA API uses response.output_audio.delta (not response.audio.delta)
                        if event_type == "response.output_audio.delta" and response.get("delta"):
                            logger.info(f"Audio delta received: {len(response['delta'])} chars")
                            try:
                                pcm_buffer += base64.b64decode(response["delta"])
                            except Exception:
                                pass
                            # Send in chunks of CHUNK_SIZE bytes
                            while len(pcm_buffer) >= CHUNK_SIZE and stream_sid:
                                chunk = pcm_buffer[:CHUNK_SIZE]
                                pcm_buffer = pcm_buffer[CHUNK_SIZE:]
                                try:
                                    # Downsample 24kHz PCM → 8kHz µ-law
                                    pcm_8k, _ = audioop.ratecv(chunk, 2, 1, 24000, 8000, None)
                                    ulaw_8k = audioop.lin2ulaw(pcm_8k, 2)
                                    ulaw_b64 = base64.b64encode(ulaw_8k).decode("utf-8")
                                    await websocket.send_text(json.dumps({
                                        "event": "media",
                                        "streamSid": stream_sid,
                                        "media": {"payload": ulaw_b64},
                                    }))
                                except Exception as e:
                                    logger.debug(f"Audio send error: {e}")

                        # Flush remaining buffer at end of response
                        elif event_type == "response.output_audio.done":
                            if pcm_buffer and stream_sid:
                                try:
                                    pcm_8k, _ = audioop.ratecv(pcm_buffer, 2, 1, 24000, 8000, None)
                                    ulaw_8k = audioop.lin2ulaw(pcm_8k, 2)
                                    ulaw_b64 = base64.b64encode(ulaw_8k).decode("utf-8")
                                    await websocket.send_text(json.dumps({
                                        "event": "media",
                                        "streamSid": stream_sid,
                                        "media": {"payload": ulaw_b64},
                                    }))
                                except Exception as e:
                                    logger.debug(f"Audio flush error: {e}")
                                pcm_buffer = b""

                        # Barge-in: clear Twilio buffer when user starts speaking
                        elif event_type == "input_audio_buffer.speech_started":
                            logger.info("Barge-in detected — clearing audio buffer")
                            if stream_sid:
                                try:
                                    await websocket.send_text(json.dumps({
                                        "event": "clear",
                                        "streamSid": stream_sid,
                                    }))
                                except Exception:
                                    pass

                        # Capture AI's transcript responses
                        elif event_type == "response.done":
                            output = response.get("response", {}).get("output", [])
                            for item in output:
                                if item.get("type") == "message" and item.get("role") == "assistant":
                                    for content in item.get("content", []):
                                        if content.get("type") == "audio" and content.get("transcript"):
                                            call_lead["transcript"].append(f"Alex: {content['transcript']}")

                        # Capture Caller's transcript
                        elif event_type == "conversation.item.input_audio_transcription.completed":
                            transcript = response.get("transcript", "")
                            if transcript:
                                call_lead["transcript"].append(f"Caller: {transcript}")

                        elif event_type == "error":
                            logger.error(f"OpenAI error: {response.get('error')}")

                except Exception as e:
                    logger.error(f"Error sending to Twilio: {e}")

            try:
                await asyncio.gather(receive_from_twilio(), send_to_twilio())
            except Exception as e:
                logger.error(f"gather error: {e}")
            finally:
                # Always fires — even on abrupt hang-up or task cancellation
                logger.info("Call ended — finalizing lead via GPT-4o")
                # Run the GPT-4o summary extraction in the background so it doesn't block the WebSocket close
                asyncio.create_task(_extract_and_finalize_lead(call_lead))

    except Exception as e:
        logger.error(f"WebSocket session error: {e}")
    finally:
        logger.info("WebSocket session closed")


async def _extract_and_finalize_lead(lead: dict):
    """Use GPT-4o to analyze the full transcript, extract fields, and send email."""
    if lead.get("_email_sent"):
        return
    lead["_email_sent"] = True

    transcript_text = "\n".join(lead.get("transcript", []))
    if transcript_text.strip():
        try:
            client = AsyncOpenAI(api_key=OPENAI_API_KEY)
            prompt = f"""
            Analyze the following phone conversation transcript between an AI receptionist (Alex) and a caller.
            Extract the following information if provided by the caller. If not provided, leave as null.
            Return a JSON object with these exact keys:
            - name: Caller's name
            - phone: Caller's phone number (if they explicitly said it, otherwise null)
            - email: Caller's email address
            - rv_details: Type, year, make, model of their RV
            - goals: What they want to achieve (e.g., boondocking, off-grid AC)
            - current_setup: Their current electrical setup
            - location: Their location in Oregon
            - summary: A concise 2-3 sentence summary of the caller's needs and the outcome of the call.

            Transcript:
            {transcript_text}
            """
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            data = json.loads(response.choices[0].message.content)
            
            # Merge extracted data into the lead dictionary
            for key in ["name", "email", "rv_details", "goals", "current_setup", "location", "summary"]:
                if data.get(key):
                    lead[key] = data[key]
            
            # Only override phone if they explicitly provided a different one than caller ID
            if data.get("phone"):
                lead["phone"] = data["phone"]

            logger.info(f"GPT-4o extraction successful for {lead.get('caller_number')}")
        except Exception as e:
            logger.error(f"GPT-4o extraction error: {e}")
            lead["summary"] = "Error extracting summary from transcript. See logs."

    # Always save and email, even if transcript was empty (we still have caller ID)
    lead_data_store.append(lead)
    logger.info(f"Lead saved: {lead.get('name') or lead.get('caller_number')}")
    try:
        send_lead_email(lead)
    except Exception as e:
        logger.error(f"Email error: {e}")


async def _send_session_update(openai_ws):
    """Send session configuration to OpenAI Realtime API (GA schema)."""
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
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.5,
                        "prefix_padding_ms": 300,
                        "silence_duration_ms": 600,
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
            "input_audio_transcription": {
                "model": "whisper-1"
            }
        },
    }
    await openai_ws.send(json.dumps(session_update))
    logger.info("Session update sent to OpenAI Realtime API (GA)")



async def _send_initial_greeting(openai_ws):
    """Trigger Alex to speak first when the call connects."""
    initial_item = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "Greet the caller warmly and professionally. Introduce yourself as Alex from Cascade RV Solar Solutions and ask how you can help them today. Keep it to one or two sentences.",
                }
            ],
        },
    }
    await openai_ws.send(json.dumps(initial_item))
    await openai_ws.send(json.dumps({"type": "response.create"}))
    logger.info("Initial greeting triggered")


# ── Leads dashboard ───────────────────────────────────────────────────────────

@app.get("/leads", response_class=HTMLResponse)
async def get_leads():
    """Return all captured leads as a formatted HTML table."""
    leads = lead_data_store
    total = len(leads)

    if total == 0:
        rows_html = '<tr><td colspan="6" style="text-align:center;color:#64748b;padding:40px;">No leads captured yet. Calls will appear here automatically.</td></tr>'
    else:
        rows_html = ""
        for i, lead in enumerate(reversed(leads)):
            ts = lead.get("timestamp", "")[:19].replace("T", " ")
            name = lead.get("name") or lead.get("caller_number", "Unknown")
            phone = lead.get("phone") or lead.get("caller_number", "—")
            email = lead.get("email") or "—"
            rv_details = lead.get("rv_details") or "—"
            goals = lead.get("goals") or "—"
            budget = lead.get("budget") or "—"
            row_bg = "#1e293b" if i % 2 == 0 else "#162032"
            rows_html += f"""
            <tr style="background:{row_bg};">
                <td>{ts}</td>
                <td><strong>{name}</strong></td>
                <td>{phone}</td>
                <td>{email}</td>
                <td>{rv_details}</td>
                <td>{goals}</td>
                <td>{budget}</td>
            </tr>"""

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Leads — Cascade RV Solar Solutions</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * {{ box-sizing: border-box; margin: 0; padding: 0; }}
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; padding: 30px 20px; }}
            .header {{ max-width: 1100px; margin: 0 auto 24px; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }}
            .header h1 {{ font-size: 22px; font-weight: 700; color: #f1f5f9; }}
            .header .sub {{ font-size: 13px; color: #64748b; margin-top: 4px; }}
            .back-btn {{ padding: 10px 20px; background: #1e293b; color: #38bdf8; border: 2px solid #38bdf8; border-radius: 10px; text-decoration: none; font-size: 14px; font-weight: 600; }}
            .back-btn:hover {{ background: #38bdf820; }}
            .badge {{ display: inline-block; background: #38bdf820; color: #38bdf8; border-radius: 20px; padding: 4px 14px; font-size: 13px; font-weight: 700; margin-left: 12px; }}
            .table-wrap {{ max-width: 1100px; margin: 0 auto; overflow-x: auto; border-radius: 14px; box-shadow: 0 10px 40px rgba(0,0,0,0.4); }}
            table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
            thead tr {{ background: #0f172a; }}
            thead th {{ padding: 14px 16px; text-align: left; color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; white-space: nowrap; }}
            tbody td {{ padding: 14px 16px; color: #cbd5e1; vertical-align: top; border-top: 1px solid #1e293b; }}
            tbody tr:hover td {{ background: #1e3a5f !important; }}
        </style>
    </head>
    <body>
        <div class="header">
            <div>
                <h1>Captured Leads <span class="badge">{total} total</span></h1>
                <div class="sub">Cascade RV Solar Solutions &mdash; AI Receptionist</div>
            </div>
            <a href="/" class="back-btn">&larr; Back to Dashboard</a>
        </div>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Date &amp; Time</th>
                        <th>Name</th>
                        <th>Phone</th>
                        <th>Email</th>
                        <th>RV Details</th>
                        <th>Goals</th>
                        <th>Budget</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
