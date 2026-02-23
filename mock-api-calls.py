import json, random, time, subprocess, uuid
from datetime import datetime, timedelta, timezone

BASE_URL = "https://kue-intelligence-tuyb.vercel.app/v1/ingestion/google/oauth/callback/mock"
TENANT_ID = "tenant_demo"
USER_ID = "user_demo"
TRACE_ID = f"trace_realistic_{uuid.uuid4().hex[:10]}"

random.seed(42)

first_names = ["Alan","Rachel","Olivia","Harun","Maya","Priya","Nina","Ethan","Liam","Sophia","Noah","Emma","Ava","Mason","Lucas","Amelia","Isla","Zoe","Arjun","Leah"]
last_names = ["Turing","Lee","Torres","Rashid","Patel","Chen","Nguyen","Carter","Wright","Kim","Brown","Davis","Miller","Wilson","Clark","Anderson","Thomas","Martin","Moore","Taylor"]
companies = ["Stripe","Google","OpenAI","Databricks","Anthropic","Scale AI","Venture Labs","Kue","Meta","Microsoft","Amazon","Figma"]
titles = ["Partner","Engineering Manager","Founder","Staff Engineer","Principal Engineer","Product Lead","VC","CTO","Director","Research Scientist"]

def mk_email(fn, ln, domain):
    return f"{fn.lower()}.{ln.lower()}@{domain}"

domains = {
    "Stripe":"stripe.com","Google":"google.com","OpenAI":"openai.com","Databricks":"databricks.com",
    "Anthropic":"anthropic.com","Scale AI":"scale.com","Venture Labs":"venturelabs.com","Kue":"kue.ai",
    "Meta":"meta.com","Microsoft":"microsoft.com","Amazon":"amazon.com","Figma":"figma.com"
}

N_CONTACTS = 120
N_THREADS = 220
N_EVENTS = 90

people = []
for i in range(N_CONTACTS):
    fn = random.choice(first_names)
    ln = random.choice(last_names)
    co = random.choice(companies)
    title = random.choice(titles)
    email = mk_email(fn, ln, domains[co])
    people.append({
        "id": f"people/c_{i+1}",
        "name": f"{fn} {ln}",
        "fn": fn, "ln": ln,
        "email": email,
        "company": co,
        "title": title,
    })

core = [
    {"id":"people/c_harun", "name":"Harun Rashid", "fn":"Harun","ln":"Rashid","email":"harun@example.com","company":"Kue","title":"Founder"},
    {"id":"people/c_alan", "name":"Alan Turing", "fn":"Alan","ln":"Turing","email":"alan@venturelabs.com","company":"Venture Labs","title":"Partner"},
    {"id":"people/c_rachel","name":"Rachel Lee", "fn":"Rachel","ln":"Lee","email":"rachel@stripe.com","company":"Stripe","title":"Engineering Manager"},
]
people[:3] = core

now = datetime.now(timezone.utc)

contacts_payload = {"source_type":"contacts","tenant_id":TENANT_ID,"user_id":USER_ID,"trace_id":TRACE_ID,"payload":{"connections":[]}}
for p in people:
    upd = (now - timedelta(days=random.randint(0,180), minutes=random.randint(0,1440))).isoformat().replace("+00:00","Z")
    contacts_payload["payload"]["connections"].append({
        "resourceName": p["id"],
        "etag": f"%E{uuid.uuid4().hex[:20]}",
        "names": [{"displayName": p["name"], "givenName": p["fn"], "familyName": p["ln"]}],
        "emailAddresses": [{"value": p["email"]}],
        "organizations": [{"name": p["company"], "title": p["title"]}],
        "metadata": {"sources":[{"type":"CONTACT","id":f"src_{p['id']}", "updateTime": upd}]}
    })

gmail_payload = {"source_type":"gmail","tenant_id":TENANT_ID,"user_id":USER_ID,"trace_id":TRACE_ID,"payload":{"messages":[]}}
subjects = ["Intro request","Quick follow-up","Investor update","Hiring discussion","Partnership idea","Meeting notes","Product feedback","Warm introduction"]
for i in range(N_THREADS):
    a, b = random.sample(people, 2)
    cc = random.choice(people)
    dt = now - timedelta(days=random.randint(0,120), hours=random.randint(0,23), minutes=random.randint(0,59))
    subj = random.choice(subjects) + f" #{i+1}"
    if i in (2,3,4):
        a, b, cc = core[1], core[0], core[2]
        subj = f"Intro to Rachel at Stripe #{i+1}"
    gmail_payload["payload"]["messages"].append({
        "id": f"msg_{i+1}",
        "threadId": f"thread_{(i//3)+1}",
        "labelIds": ["INBOX","CATEGORY_PERSONAL"],
        "snippet": f"{a['fn']} discussing {subj.lower()} with {b['fn']}.",
        "historyId": str(900000000+i),
        "internalDate": str(int(dt.timestamp()*1000)),
        "payload": {"mimeType":"text/plain","headers":[
            {"name":"Date","value":dt.strftime("%a, %d %b %Y %H:%M:%S +0000")},
            {"name":"Subject","value":subj},
            {"name":"From","value":f"{a['name']} <{a['email']}>"},
            {"name":"To","value":f"{b['name']} <{b['email']}>"},
            {"name":"Cc","value":f"{cc['name']} <{cc['email']}>"}
        ]}
    })

calendar_payload = {"source_type":"calendar","tenant_id":TENANT_ID,"user_id":USER_ID,"trace_id":TRACE_ID,"payload":{"items":[]}}
summaries = ["Coffee chat","Warm intro call","Portfolio sync","Hiring sync","Product review","Investor meeting"]
for i in range(N_EVENTS):
    attendees = random.sample(people, 3)
    start = now - timedelta(days=random.randint(0,90), hours=random.randint(0,23))
    end = start + timedelta(minutes=random.choice([30,45,60]))
    summary = random.choice(summaries)
    if i in (1,2):
        attendees = [core[0], core[1], core[2]]
        summary = "Warm intro call: Harun, Alan, Rachel"
    calendar_payload["payload"]["items"].append({
        "id": f"cal_{i+1}",
        "status": "confirmed",
        "summary": summary,
        "created": (start - timedelta(days=3)).isoformat().replace("+00:00","Z"),
        "updated": (start - timedelta(hours=2)).isoformat().replace("+00:00","Z"),
        "start": {"dateTime": start.isoformat().replace("+00:00","Z"), "timeZone":"UTC"},
        "end": {"dateTime": end.isoformat().replace("+00:00","Z"), "timeZone":"UTC"},
        "organizer": {"email": attendees[0]["email"]},
        "attendees": [{"email": x["email"], "responseStatus":"accepted"} for x in attendees],
        "location": random.choice(["Google Meet","San Francisco","Zoom"])
    })

def post(payload):
    p = subprocess.run(
        ["curl","-sS","-X","POST",BASE_URL,"-H","Content-Type: application/json","-d",json.dumps(payload)],
        capture_output=True, text=True
    )
    print(p.stdout)

print("TRACE_ID:", TRACE_ID)
print("Posting contacts..."); post(contacts_payload); time.sleep(1)
print("Posting gmail..."); post(gmail_payload); time.sleep(1)
print("Posting calendar..."); post(calendar_payload)

print("\nNow check:")
print("Inngest run for event: kue/user.mock_connected")
print(f"GET /v1/ingestion/pipeline/run/{TRACE_ID}")
print(f"GET /v1/ingestion/raw-events/{TRACE_ID}")
print(f"GET /v1/ingestion/layer3/events/{TRACE_ID}")
