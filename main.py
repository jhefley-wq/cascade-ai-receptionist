"""
Cascade RV Solar Solutions — AI Phone Receptionist
===================================================
Platform: Twilio (phone) + OpenAI GPT-4o (AI brain) + Twilio TTS (voice)
Author: Built for Jason Hefley / Cascade RV Solar Solutions

Call Flow:
  - Jason sets his phone to Do Not Disturb
  - Carrier forwards unanswered calls to the Twilio number
  - This server answers, runs the AI receptionist, captures leads
  - Jason can toggle the receptionist ON/OFF via the web dashboard
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import HTMLResponse
import openai
from twilio.twiml.voice_response import VoiceResponse, Gather
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Cascade RV Solar Solutions — AI Receptionist")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OWNER_PHONE    = os.getenv("OWNER_PHONE", "+15039190521")
OWNER_EMAIL    = os.getenv("OWNER_EMAIL", "jhefley@cascadesolarrvsolutions.com")
BUSINESS_NAME  = "Cascade RV Solar Solutions"

openai_client = openai.OpenAI(api_key=OPENAI_API_KEY)

# ─────────────────────────────────────────────
# Receptionist ON/OFF toggle state
# ─────────────────────────────────────────────
receptionist_state = {
    "active": True,
    "toggled_at": datetime.now().isoformat(),
}

# ─────────────────────────────────────────────
# System Prompt — AI Receptionist Persona
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """
You are Alex, the professional AI phone receptionist for Cascade RV Solar Solutions.
You speak in a calm, confident, professional male voice with a neutral American accent.
You are knowledgeable, helpful, and represent the brand with warmth and expertise.

BUSINESS INFORMATION:
- Business: Cascade RV Solar Solutions
- Owner: Jason Hefley
- Location: Prineville, Oregon (Central Oregon)
- Phone: (503) 919-0521
- Email: jhefley@cascadesolarrvsolutions.com
- Website: cascadesolarrvsolutions.com

SERVICES:
1. Free Consultation — Discuss RV setup, boondocking goals, power needs, and budget
2. Custom Installation — Professional off-grid solar system installation (~5 days on-site)
3. Troubleshooting & Repair — Electrical diagnosis and repair of existing RV electrical systems
4. DIY Tools — Wire size calculator, system design tool, system analyzer
5. Training Resources — Educational content for DIY solar independence
6. Shop — Solar components and equipment
7. Financing — Available through Enhancify (soft credit pull, no impact on credit score)

SERVICE AREA:
- All of Oregon (statewide mobile installation)
- Including Portland, Bend, Eugene, Medford, and everywhere in between
- Mileage rates apply but are kept minimal

PRICING:
- Costs vary based on power load requirements and personal goals
- No one-size-fits-all answer; best starting point is the RV Solar Calculator on the website
- Financing available through Enhancify

TIMELINE:
- Installation: approximately 5 days on-site once parts are in hand
- Parts lead time: approximately 2 weeks
- Currently booked out 4–6 weeks in advance
- Recommend reaching out early to secure a spot

WARRANTY:
- 1-year warranty on all installation labor
- Product warranties handled directly through manufacturers

BRANDS:
- Victron Energy, Renogy, EPOCH Batteries (others available on request)

EXPERIENCE:
- 30+ years in critical infrastructure
- Deep specialization in batteries, inverters, charge controllers, and complex power systems

FREQUENTLY ASKED QUESTIONS:
Q: What services do you offer?
A: We offer free consultations, custom solar system design, professional installation, electrical troubleshooting and repair, DIY tools and training resources, and financing options.

Q: Where do you serve?
A: We serve all of Oregon with mobile installation available statewide, including Portland, Bend, Eugene, Medford, and everywhere in between. We're based in Prineville, Oregon.

Q: How much does it cost?
A: Costs vary depending on your RV's power needs and your goals. The best starting point is our free consultation or the RV Solar Calculator on our website. We also offer financing through Enhancify.

Q: How long does installation take?
A: Once all parts are in hand, installation typically takes about 5 days on-site. Parts generally have a 2-week lead time, and we're currently booked out 4 to 6 weeks, so we recommend reaching out early.

Q: Do you offer a warranty?
A: Yes. We provide a 1-year warranty on all installation labor. Product warranties are handled directly through the manufacturers.

Q: What brands do you use?
A: We primarily work with Victron Energy, Renogy, and EPOCH Batteries. We're also happy to work with other brands upon request.

Q: How do I get started?
A: The best first step is to schedule a free consultation. You can call us at (503) 919-0521 or visit our website at cascadesolarrvsolutions.com.

Q: Is financing available?
A: Yes, financing is available through our partner Enhancify. Checking your eligibility is a soft credit pull and will not affect your credit score.

Q: Can I install it myself?
A: Absolutely. After your consultation, we can design a complete system and provide a step-by-step plan for a DIY installation, or we can handle the entire installation for you.

Q: Why solar instead of a generator?
A: Generators are noisy, require fuel, need regular maintenance, and are restricted in many camping areas. Solar gives you clean, quiet, reliable power with no ongoing fuel costs — and it lets you boondock in places where generators simply aren't allowed.

CALL HANDLING RULES:
- Always greet callers warmly and professionally as Alex from Cascade RV Solar Solutions.
- Answer questions using the business information above.
- If a caller wants to speak with Jason directly, let them know Jason is currently unavailable but you'll make sure he gets their message and calls them back promptly.
- If a caller has an urgent technical issue with an existing installation, acknowledge the urgency and collect their information for a priority callback.
- Always try to capture the caller's: full name, phone number, email address, and the nature of their inquiry.
- When collecting information, ask one question at a time — do not overwhelm the caller.
- If a caller wants to leave a message, collect it and confirm you'll pass it along to Jason.
- Be concise. Phone conversations should be efficient — do not give overly long responses.
- Always end calls by thanking the caller and letting them know Jason will follow up with them shortly.
- Never make up information not listed above. If unsure, offer to have Jason call them back.

LEAD CAPTURE TRIGGER:
When you have successfully collected the caller's name, phone number, and/or email, include a special JSON block at the END of your response (after your spoken words) in this exact format — the system will strip it before speaking:
[LEAD_DATA:{"name":"...", "phone":"...", "email":"...", "inquiry":"...", "message":"..."}]

IMPORTANT: Keep all spoken responses under 3 sentences when possible. Be professional, warm, and efficient.
"""

# ─────────────────────────────────────────────
# In-memory conversation store (per call SID)
# ─────────────────────────────────────────────
conversations: dict[str, list[dict]] = {}
lead_data_store: list[dict] = []

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def get_ai_response(call_sid: str, user_input: str) -> tuple[str, Optional[dict]]:
    """Get AI response and extract any lead data."""
    if call_sid not in conversations:
        conversations[call_sid] = []

    conversations[call_sid].append({"role": "user", "content": user_input})

    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + conversations[call_sid]

    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        max_tokens=300,
        temperature=0.7,
    )

    full_response = response.choices[0].message.content.strip()

    # Extract lead data if present
    lead_data = None
    spoken_response = full_response

    if "[LEAD_DATA:" in full_response:
        try:
            start = full_response.index("[LEAD_DATA:") + len("[LEAD_DATA:")
            end   = full_response.index("]", start)
            json_str = full_response[start:end]
            lead_data = json.loads(json_str)
            spoken_response = full_response[:full_response.index("[LEAD_DATA:")].strip()
        except Exception as e:
            logger.warning(f"Failed to parse lead data: {e}")

    conversations[call_sid].append({"role": "assistant", "content": spoken_response})

    return spoken_response, lead_data


def save_lead(call_sid: str, from_number: str, lead_data: dict):
    """Save captured lead data to file and log it."""
    lead_entry = {
        "timestamp": datetime.now().isoformat(),
        "call_sid": call_sid,
        "caller_number": from_number,
        **lead_data
    }
    lead_data_store.append(lead_entry)

    leads_file = "/home/ubuntu/cascade_receptionist/leads.jsonl"
    with open(leads_file, "a") as f:
        f.write(json.dumps(lead_entry) + "\n")

    logger.info(f"Lead captured: {lead_entry}")


def build_twiml_response(text: str, gather_action: str = "/respond", timeout: int = 5) -> str:
    """Build a TwiML response with speech and gather."""
    response = VoiceResponse()
    gather = Gather(
        input="speech",
        action=gather_action,
        timeout=timeout,
        speech_timeout="auto",
        language="en-US",
    )
    gather.say(
        text,
        voice="Polly.Matthew",
        language="en-US"
    )
    response.append(gather)

    response.say(
        "I didn't catch that. Please feel free to call back, or visit our website at cascade solar RV solutions dot com. Thank you for calling Cascade RV Solar Solutions. Have a great day!",
        voice="Polly.Matthew",
        language="en-US"
    )
    return str(response)


def build_voicemail_twiml() -> str:
    """TwiML for when receptionist is OFF — plays a simple voicemail message."""
    response = VoiceResponse()
    response.say(
        "You have reached Cascade RV Solar Solutions. We are unable to take your call right now. "
        "Please leave a message after the tone, or visit our website at cascade solar RV solutions dot com. "
        "We will get back to you as soon as possible. Thank you.",
        voice="Polly.Matthew",
        language="en-US"
    )
    response.record(
        max_length=120,
        action="/recording-complete",
        finish_on_key="#",
        play_beep=True,
    )
    return str(response)


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
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
            .logo {{
                font-size: 13px;
                color: #64748b;
                text-transform: uppercase;
                letter-spacing: 2px;
                margin-bottom: 8px;
            }}
            h1 {{
                font-size: 24px;
                font-weight: 700;
                color: #f1f5f9;
                margin-bottom: 32px;
            }}
            .status-badge {{
                display: inline-flex;
                align-items: center;
                gap: 10px;
                background: #0f172a;
                border-radius: 12px;
                padding: 16px 24px;
                margin-bottom: 32px;
                width: 100%;
            }}
            .dot {{
                width: 14px;
                height: 14px;
                border-radius: 50%;
                background: {status_color};
                box-shadow: 0 0 10px {status_color};
                flex-shrink: 0;
            }}
            .status-text {{
                font-size: 15px;
                font-weight: 600;
                color: {status_color};
            }}
            .stats {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 16px;
                margin-bottom: 32px;
            }}
            .stat {{
                background: #0f172a;
                border-radius: 10px;
                padding: 16px;
                text-align: center;
            }}
            .stat-number {{
                font-size: 28px;
                font-weight: 700;
                color: #38bdf8;
            }}
            .stat-label {{
                font-size: 12px;
                color: #64748b;
                margin-top: 4px;
            }}
            .toggle-btn {{
                display: block;
                width: 100%;
                padding: 16px;
                background: {btn_color};
                color: white;
                border: none;
                border-radius: 12px;
                font-size: 16px;
                font-weight: 700;
                cursor: pointer;
                text-decoration: none;
                text-align: center;
                transition: opacity 0.2s;
                margin-bottom: 16px;
            }}
            .toggle-btn:hover {{ opacity: 0.85; }}
            .leads-btn {{
                display: block;
                width: 100%;
                padding: 14px;
                background: transparent;
                color: #38bdf8;
                border: 2px solid #38bdf8;
                border-radius: 12px;
                font-size: 15px;
                font-weight: 600;
                cursor: pointer;
                text-decoration: none;
                text-align: center;
                transition: all 0.2s;
            }}
            .leads-btn:hover {{ background: #38bdf820; }}
            .footer {{
                margin-top: 24px;
                font-size: 12px;
                color: #475569;
                text-align: center;
            }}
            .toggled-at {{
                font-size: 12px;
                color: #475569;
                margin-top: 12px;
                text-align: center;
            }}
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
                    <div class="stat-number">{len(conversations)}</div>
                    <div class="stat-label">Active Calls</div>
                </div>
            </div>

            <a href="/toggle?action={btn_action}" class="toggle-btn">{btn_label}</a>
            <a href="/leads" class="leads-btn">View Captured Leads</a>

            <div class="toggled-at">Last toggled: {toggled_at}</div>
            <div class="footer">
                (503) 919-0521 &nbsp;·&nbsp; Prineville, OR &nbsp;·&nbsp; cascadesolarrvsolutions.com
            </div>
        </div>
    </body>
    </html>
    """


@app.get("/toggle")
async def toggle_receptionist(action: str = "on"):
    """Toggle the receptionist ON or OFF."""
    receptionist_state["active"] = (action.lower() == "on")
    receptionist_state["toggled_at"] = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    status = "ACTIVE" if receptionist_state["active"] else "INACTIVE"
    logger.info(f"Receptionist toggled: {status}")
    # Redirect back to dashboard
    return Response(
        content='<html><head><meta http-equiv="refresh" content="0;url=/"></head></html>',
        media_type="text/html"
    )


@app.post("/incoming-call")
async def incoming_call(
    request: Request,
    CallSid: str = Form(...),
    From: str = Form(default="Unknown"),
    To: str = Form(default=""),
):
    """Handle new incoming call."""
    logger.info(f"Incoming call: SID={CallSid}, From={From}, To={To}")

    # If receptionist is OFF, play voicemail
    if not receptionist_state["active"]:
        logger.info("Receptionist is OFF — routing to voicemail message")
        twiml = build_voicemail_twiml()
        return Response(content=twiml, media_type="application/xml")

    # Initialize conversation
    conversations[CallSid] = []

    greeting = (
        "Thank you for calling Cascade RV Solar Solutions. "
        "My name is Alex, and I'm here to assist you today. "
        "Whether you have questions about our solar installation services, "
        "want to schedule a free consultation, or need troubleshooting support, "
        "I'm happy to help. How can I assist you today?"
    )

    twiml = build_twiml_response(greeting)
    return Response(content=twiml, media_type="application/xml")


@app.post("/respond")
async def respond(
    request: Request,
    CallSid: str = Form(...),
    From: str = Form(default="Unknown"),
    SpeechResult: str = Form(default=""),
    Confidence: str = Form(default="0"),
):
    """Handle caller speech input and return AI response."""
    logger.info(f"Speech from {From} [{CallSid}]: '{SpeechResult}' (confidence: {Confidence})")

    if not SpeechResult.strip():
        twiml = build_twiml_response(
            "I'm sorry, I didn't catch that. Could you please repeat what you said?"
        )
        return Response(content=twiml, media_type="application/xml")

    # Get AI response
    spoken_text, lead_data = get_ai_response(CallSid, SpeechResult)

    # Save lead if captured
    if lead_data:
        save_lead(CallSid, From, lead_data)

    # Check for call-ending phrases
    end_phrases = ["goodbye", "bye", "thank you", "that's all", "that is all", "no more questions", "hang up"]
    if any(phrase in SpeechResult.lower() for phrase in end_phrases):
        response = VoiceResponse()
        response.say(
            spoken_text + " Thank you for calling Cascade RV Solar Solutions. Have a wonderful day!",
            voice="Polly.Matthew",
            language="en-US"
        )
        response.hangup()
        return Response(content=str(response), media_type="application/xml")

    twiml = build_twiml_response(spoken_text)
    return Response(content=twiml, media_type="application/xml")


@app.post("/recording-complete")
async def recording_complete(
    CallSid: str = Form(...),
    RecordingUrl: str = Form(default=""),
    RecordingDuration: str = Form(default="0"),
):
    """Handle completed voicemail recordings (when receptionist is OFF)."""
    logger.info(f"Voicemail recorded: {RecordingUrl} ({RecordingDuration}s) for call {CallSid}")

    # Save voicemail record
    voicemail_entry = {
        "timestamp": datetime.now().isoformat(),
        "call_sid": CallSid,
        "type": "voicemail",
        "recording_url": RecordingUrl,
        "duration_seconds": RecordingDuration,
    }
    leads_file = "/home/ubuntu/cascade_receptionist/leads.jsonl"
    with open(leads_file, "a") as f:
        f.write(json.dumps(voicemail_entry) + "\n")

    response = VoiceResponse()
    response.say(
        "Thank you for your message. We will get back to you as soon as possible. Goodbye.",
        voice="Polly.Matthew",
        language="en-US"
    )
    response.hangup()
    return Response(content=str(response), media_type="application/xml")


@app.get("/leads")
async def get_leads():
    """Return all captured leads as JSON."""
    return {"total": len(lead_data_store), "leads": lead_data_store}


@app.post("/call-status")
async def call_status(
    CallSid: str = Form(...),
    CallStatus: str = Form(default=""),
):
    """Handle call status callbacks — clean up conversation on completion."""
    logger.info(f"Call {CallSid} status: {CallStatus}")
    if CallStatus in ("completed", "failed", "busy", "no-answer", "canceled"):
        if CallSid in conversations:
            del conversations[CallSid]
    return Response(content="OK", media_type="text/plain")


# ─────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
