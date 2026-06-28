# Cascade RV Solar Solutions — AI Phone Receptionist
## Complete Setup & Deployment Guide

---

## Overview

This AI phone receptionist answers calls on behalf of Cascade RV Solar Solutions using your existing phone number via **Conditional Call Forwarding**. 

When your phone is on **Do Not Disturb**, busy, or unanswered, calls will automatically forward to the AI Receptionist instead of your standard voicemail.

| Component | Service | Purpose |
|---|---|---|
| **Phone Number** | Twilio | Receives forwarded calls from your personal phone |
| **AI Brain** | OpenAI GPT-4o | Understands and responds to callers |
| **Voice** | Amazon Polly (via Twilio) | Professional neutral American male voice |
| **Backend** | Python + FastAPI | Runs the receptionist logic |
| **Dashboard** | Web Interface | Allows you to toggle the AI ON/OFF |

**What the receptionist does:**
- Greets callers professionally as "Alex" from Cascade RV Solar Solutions
- Answers FAQs about services, pricing, timeline, warranty, and service area
- Captures caller name, phone number, email, and inquiry details
- Takes messages for Jason
- Routes urgent calls by noting them for priority callback
- Saves all leads to a file for manual CRM entry

---

## Step 1 — Prerequisites

You will need accounts with the following services:

### 1a. OpenAI Account
1. Go to [platform.openai.com](https://platform.openai.com)
2. Sign up or log in
3. Navigate to **API Keys** → **Create new secret key**
4. Copy the key — you will need it in Step 3

**Estimated cost:** GPT-4o costs approximately $0.005 per minute of conversation. A typical 3-minute call costs about $0.015.

### 1b. Twilio Account
1. Go to [twilio.com](https://www.twilio.com) and sign up for a free account
2. Verify your phone number
3. From the Twilio Console, note your:
   - **Account SID** (starts with `AC`)
   - **Auth Token**
4. Purchase a phone number (this is the number your personal phone will forward to):
   - Go to **Phone Numbers → Manage → Buy a Number**
   - Choose an Oregon area code (503 or 541) if desired
   - Enable **Voice** capability
   - Cost: approximately $1.15/month per number

---

## Step 2 — Install the Receptionist

### Option A — Run on Your Own Computer or Server

**Requirements:** Python 3.10 or higher

```bash
# 1. Download the receptionist files to a folder
cd cascade_receptionist

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy and fill in your configuration
cp .env.example .env
# Edit .env with your actual API keys (see Step 3)

# 4. Start the server
./start.sh
```

The server will start on `http://localhost:8000`

### Option B — Deploy to a Cloud Server (Recommended for 24/7 operation)

For always-on operation, deploy to a cloud provider:

**Recommended options (free or low cost):**
- [Railway.app](https://railway.app) — Free tier available, easy deployment
- [Render.com](https://render.com) — Free tier available
- [DigitalOcean](https://digitalocean.com) — $6/month droplet

**Railway deployment (easiest):**
```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

---

## Step 3 — Configure Your .env File

Edit the `.env` file with your actual credentials:

```env
OPENAI_API_KEY=sk-...your-actual-openai-key...
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=your-actual-twilio-auth-token
TWILIO_PHONE_NUMBER=+15031234567
OWNER_PHONE=+15039190521
OWNER_EMAIL=jhefley@cascadesolarrvsolutions.com
```

---

## Step 4 — Connect Twilio to Your Receptionist

1. Log in to [Twilio Console](https://console.twilio.com)
2. Go to **Phone Numbers → Manage → Active Numbers**
3. Click on your purchased Twilio phone number
4. Under **Voice & Fax** → **A Call Comes In**:
   - Set to **Webhook**
   - Enter your server URL: `https://YOUR-SERVER-URL/incoming-call`
   - Method: **HTTP POST**
5. Under **Call Status Changes**:
   - Enter: `https://YOUR-SERVER-URL/call-status`
   - Method: **HTTP POST**
6. Click **Save**

---

## Step 5 — Set Up Conditional Call Forwarding

To make your personal phone forward to the AI Receptionist when you are busy or on Do Not Disturb, you need to set up **Conditional Call Forwarding** with your carrier.

**Note:** Replace `[TWILIO_NUMBER]` with the 10-digit Twilio phone number you purchased in Step 1 (e.g., `5031234567`).

### For Verizon
- **To Activate:** Dial `*71` followed by your `[TWILIO_NUMBER]`, then press Call. Listen for a series of beeps, then hang up.
- **To Deactivate:** Dial `*73` and press Call.

### For AT&T
- **To Activate:** Dial `*92` followed by your `[TWILIO_NUMBER]`, then press `#` and Call.
- **To Deactivate:** Dial `*93#` and press Call.

### For T-Mobile
- **To Activate (No Answer):** Dial `**61*1` followed by your `[TWILIO_NUMBER]`, then press `#` and Call.
- **To Activate (Busy/DND):** Dial `**67*1` followed by your `[TWILIO_NUMBER]`, then press `#` and Call.
- **To Deactivate:** Dial `##004#` and press Call.

### For iPhone Users (Alternative Method)
If the carrier codes do not work, you can set up standard forwarding:
1. Go to **Settings > Phone > Call Forwarding**
2. Toggle it ON and enter your Twilio number.
*(Note: This forwards ALL calls immediately, rather than just unanswered ones).*

### For Android Users (Alternative Method)
1. Open the **Phone** app.
2. Tap the three dots (Menu) > **Settings > Call Forwarding** (or Supplementary Services).
3. Select **Forward when busy** and **Forward when unanswered**.
4. Enter your Twilio number for both.

---

## Step 6 — How to Use the Dashboard

Your receptionist comes with a web dashboard accessible at your server URL (e.g., `https://YOUR-SERVER-URL/`).

**From the dashboard, you can:**
1. **Toggle the Receptionist ON/OFF:** 
   - When **ACTIVE**, the AI will answer forwarded calls and converse with the customer.
   - When **INACTIVE**, the system will simply play a standard voicemail greeting ("We are unable to take your call...") and record a message, bypassing the AI.
2. **View Leads:** Click "View Captured Leads" to see a list of all callers who provided their information or left a voicemail.

### The Workflow
1. Keep the AI Receptionist toggled **ON** in the dashboard.
2. When you are working on a project, put your personal phone in **Do Not Disturb (DND)**.
3. Incoming calls to your personal phone will automatically forward to the Twilio number.
4. The AI Receptionist will answer, assist the customer, and take a message/lead.
5. When you are done, turn off DND on your phone to resume taking calls normally.

---

## Support

For questions about this AI receptionist setup, refer to:
- [Twilio Documentation](https://www.twilio.com/docs)
- [OpenAI API Documentation](https://platform.openai.com/docs)
- [FastAPI Documentation](https://fastapi.tiangolo.com)

---

*Built for Cascade RV Solar Solutions | Prineville, Oregon | (503) 919-0521*
