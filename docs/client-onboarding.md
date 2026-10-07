# Client Onboarding & Campaign Launch Process

Repeatable process for onboarding new native-ads and performance-marketing clients.
Follow each section in order; tick every checkbox before moving to the next stage.

---

## Stage 1 — Client Intake

**Owner:** Account lead  
**Goal:** Capture everything needed to build and launch campaigns.

### Intake form fields

| Field | Notes |
|---|---|
| Company name & URL | |
| Primary contact (name, email, phone) | |
| Invoice contact & billing details | |
| Monthly budget (€ / £ / $, currency) | Split by platform if known |
| Primary KPI | e.g. purchase, lead, app install |
| Secondary KPIs | e.g. ROAS, CAC, CPL |
| Target geographies | Country + language |
| Target audiences | Demographics, interests, lookalikes, exclusions |
| Product / offer description | What is being sold, USP, price point |
| Landing page URL(s) | One per offer/funnel |
| Brand assets | Logo, colours (hex), fonts, brand guidelines URL |
| Creative preferences | Tone of voice, prohibited claims, legal disclaimers |
| Competitor domains | For negative targeting and benchmarking |
| Attribution tool | GetKlar, Triple Whale, Northbeam, GA4, etc. |
| CRM / e-commerce platform | Shopify, WooCommerce, etc. |
| Existing ad accounts | Platform + account ID for each |
| Existing pixels / datasets | Meta Pixel ID, GA4 Measurement ID, etc. |
| Go-live date | Hard deadline if any |

### Intake checklist

- [ ] Intake form completed and signed off by client
- [ ] NDA / MSA executed (or noted as not required)
- [ ] Budget and payment terms confirmed in writing
- [ ] Attribution window agreed (default: 28-day click, 1-day view)

---

## Stage 2 — Account & Pixel Setup

**Owner:** Performance Marketing Engineer  
**Goal:** All tracking and ad-account infrastructure in place before creatives are built.

### Ad account access

- [ ] Client grants agency admin access to all existing ad accounts (Meta, Google Ads, Outbrain, Taboola)
- [ ] New ad accounts created where needed (platform + currency correct)
- [ ] Account IDs recorded in `SYNC_ACCOUNTS` env var (see `.env.example`)
- [ ] Billing verified — credit card or invoicing attached, spending limit set

### Meta (Facebook/Instagram) setup

- [ ] Business Manager access granted (`Business Settings → People → Add People`)
- [ ] Meta Pixel created or confirmed existing (`Events Manager`)
- [ ] Pixel base code installed on all landing pages (verify with Meta Pixel Helper)
- [ ] Standard events firing correctly:
  - [ ] `PageView` on all pages
  - [ ] `ViewContent` on product / landing pages
  - [ ] `InitiateCheckout`
  - [ ] `Purchase` with `value` and `currency` parameters
- [ ] CAPI (Conversions API) configured for server-side deduplication
- [ ] Pixel ID and dataset ID added to `.env` / secrets

### Google Ads setup

- [ ] Google Ads account linked to MCC (manager account)
- [ ] Google Ads Developer Token and OAuth2 credentials generated
- [ ] Conversion actions created (purchase, lead, etc.) with correct values
- [ ] Google Tag / gtag.js installed and verified in Tag Assistant
- [ ] Customer ID (without dashes) added to `SYNC_ACCOUNTS`

### Attribution tool setup

- [ ] Attribution platform connected to all ad accounts and pixel/CAPI sources
- [ ] GetKlar (or equivalent) attribution token added to `.env`
- [ ] Attribution model confirmed with client (default: marketing mix 28-day)
- [ ] Daily spend report pipeline verified (`python -m loaders.getklar_daily_report`)

### Tracking QA

- [ ] Test purchase / lead conversion fires correctly end-to-end
- [ ] Attribution platform shows the test conversion
- [ ] Deduplication working (server + browser events not double-counted)

---

## Stage 3 — Campaign Setup

**Owner:** Performance Marketing Engineer + Creative team  
**Goal:** Campaigns built, reviewed, and ready to launch.

### Creative assets

- [ ] At least 3–5 ad creatives per platform delivered (images + copy)
- [ ] All creatives meet platform size and policy requirements:
  - Meta: 1080×1080 or 1080×1920, < 20 % text overlay
  - Google: responsive display assets (up to 15 images, 5 logos, 5 videos)
  - Outbrain/Taboola: 1200×628 PNG, headline ≤ 65 chars
- [ ] Advertorials / landing pages reviewed for compliance (no banned claims)
- [ ] Brand safety review passed (no misleading before/after, no prohibited health claims)

### Campaign structure

- [ ] Campaign naming convention followed: `{PRODUCT}_{GEO}_{BUDGET}_{DATE}` (e.g. `KOLLAGEN_DE_AT_300_2026-10`)
- [ ] Ad sets / ad groups segmented by audience and placement as agreed
- [ ] Budget set correctly (daily or lifetime, currency matches account)
- [ ] Bid strategy set (CBO recommended for Meta; tCPA/tROAS for Google)
- [ ] Start date set to next day (never same-day)
- [ ] End date set only if there is a hard cutoff; otherwise leave open

### Uploader checklist (native ads — Outbrain / Taboola)

- [ ] PNG creatives downloaded from Slack via `python -m loaders.slack_downloader`
- [ ] Template campaign ID confirmed in `.env`
- [ ] Upload dry-run reviewed: `python -m loaders.upload_native_ads --dry-run`
- [ ] Live upload executed and campaign IDs logged

---

## Stage 4 — Launch QA

**Owner:** Performance Marketing Engineer  
**Goal:** Verify everything is correct before spending starts.

### Pre-spend checklist

- [ ] All campaigns in `PAUSED` status — do not activate until QA passes
- [ ] Campaign settings verified:
  - [ ] Correct geo targeting (no accidental global targeting)
  - [ ] Correct language targeting
  - [ ] Correct audience / exclusions applied
  - [ ] Budget matches agreed amount
  - [ ] Bid strategy and bid cap set correctly
  - [ ] No accidental overlap between ad sets (audience fragmentation check)
- [ ] Ad previews reviewed — copy, images, landing page URL correct
- [ ] UTM parameters present on all destination URLs: `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`
- [ ] Landing page loads correctly and passes the checkout flow
- [ ] Pixel fires on the landing page (Meta Pixel Helper / Tag Assistant)

### Go-live

- [ ] Client sign-off received (screenshot or email confirmation)
- [ ] Campaigns activated — note exact time and who activated
- [ ] First-hour check: impressions and clicks flowing, no policy disapprovals
- [ ] First-day check: spend pacing as expected, no account-level flags

---

## Stage 5 — Reporting Setup

**Owner:** Performance Marketing Engineer  
**Goal:** Automated daily reporting live before end of day 1.

- [ ] Client added to reporting dashboard (add row to `clients` table in Postgres)
- [ ] Ad accounts linked in `ad_accounts` table with correct `platform` and `external_id`
- [ ] Daily sync confirmed working: `python -m loaders.sync_ad_platforms --platform all`
- [ ] GetKlar daily report scheduled (GitHub Actions workflow enabled)
- [ ] Dashboard URL shared with client (read-only login credentials)
- [ ] Weekly/monthly report cadence agreed and calendared

---

## Appendix — Credential Checklist

Quick reference for all secrets needed per new client:

| Variable | Where to get it |
|---|---|
| `GOOGLE_ADS_DEVELOPER_TOKEN` | [Google Ads API Center](https://ads.google.com/aw/apicenter) |
| `GOOGLE_ADS_CLIENT_ID` / `CLIENT_SECRET` | Google Cloud Console → OAuth 2.0 client |
| `GOOGLE_ADS_REFRESH_TOKEN` | OAuth2 flow (`google-ads-python` refresh token script) |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | MCC account ID (digits only, no dashes) |
| `META_ACCESS_TOKEN` | Business Manager → System Users → Generate token (ads_read, ads_management) |
| `OUTBRAIN_API_KEY` | Outbrain Amplify → Account Settings → API |
| `OUTBRAIN_ACCOUNT_ID` | Amplify → Account ID in URL |
| `TABOOLA_CLIENT_ID` / `CLIENT_SECRET` | Taboola Backstage → Account → API Access |
| `TABOOLA_ACCOUNT_ID` | Backstage account name slug |
| `GETKLAR_REFRESH_TOKEN` | Klar Frontend → Store Settings → Attribution API |
| `TEAMS_WEBHOOK_URL` | MS Teams channel → Connectors → Incoming Webhook |
