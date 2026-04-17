import os
import sys
import json
import time
import hashlib
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from dotenv import load_dotenv

# Force UTF-8 output so emojis don't crash on Windows cp1252 terminals
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

# Pathing setup for Agent Core
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_AG_ROOT  = os.path.dirname(_THIS_DIR)
load_dotenv(os.path.join(_AG_ROOT, ".env"))

# Configuration
SPREADSHEET_ID = "1xYfHw94_5nk2RC-z0Vvl3hQx8vdgw_zF5usXQj2sWeU"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CLIENT = OpenAI(api_key=OPENAI_API_KEY)

#  EVALUATION LENSES (DIRECTIVE)
LENSES = """
1. Alignment vs Misalignment: Gap between claim and happening.
2. Structure as a Reflection of Values: What structure reveals about value.
3. Stewardship vs Extraction: Built to last or to extract?
4. Clarity as a Strategic Asset: Confusion mistaken for complexity.
5. Designing for Participation and Trust: Who is included or left out?
"""

#  RESEARCH ANCHORS
ANCHORS = """
1. Structural Alignment: Organization, governance, system design.
2. Human-System Tension: Value vs behavior gaps.
3. Long-Term Stewardship: Sustainability, impact, community.
"""

def log_event(msg, level="INFO"):
    print(f"[{level}] {msg}")

def get_sheets():
    creds_path = os.path.join(_AG_ROOT, "credentials.json")
    token_path = os.path.join(_AG_ROOT, "token.json")
    
    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"Missing credentials.json at {_AG_ROOT}")
    
    gc = gspread.oauth(
        credentials_filename=creds_path,
        authorized_user_filename=token_path,
    )
    return gc.open_by_key(SPREADSHEET_ID)

def get_or_create_worksheet(sh, title, expected_headers):
    """
    Returns a worksheet. Creates it with headers if missing.
    If it exists, it returns it as is (respecting user formatting).
    """
    try:
        ws = sh.worksheet(title)
        return ws
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=len(expected_headers))
        ws.update([expected_headers], "A1")
        # Default Agent styling for NEW sheets only
        num_cols = len(expected_headers)
        end_col = chr(ord('A') + num_cols - 1)
        ws.format(f"A1:{end_col}1", {"textFormat": {"bold": True}})
        ws.freeze(rows=1)
        return ws

def map_row_data(data_dict, headers):
    """
    Dynamically maps a dictionary of data to a row based on header names.
    Uses case-insensitive matching and stripping to stay robust against manual sheet edits.
    """
    row = [None] * len(headers)
    
    # Create a normalized mapping of data_dict keys
    norm_data = {str(k).strip().lower(): v for k, v in data_dict.items()}
    
    for i, header in enumerate(headers):
        h_norm = str(header).strip().lower()
        if h_norm in norm_data:
            row[i] = norm_data[h_norm]
    return row

def discover_topics():
    log_event("Simulating Academic Research Search (site:edu, site:gov)...")
    
    prompt = f"""
    You are the Insight Research Agent agent. 
    Current Date: {datetime.now().strftime("%Y-%m-%d")}

    Step 1: Identify 5-8 trending academic topics or institutional reports from 2025-2026 
    related to organizational design, sustainability, governance, or system ethics.
    Only use Reputable Institutional/Academic sources (Universities, Think Tanks).

    Step 2: Return a JSON object with a single key "topics" containing a list of objects with these fields:
    - title
    - source_name
    - source_url
    - summary
    - anchor_potential
    """

    response = CLIENT.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": "You are a professional academic researchers. Return ONLY JSON."},
                  {"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    
    raw_res = response.choices[0].message.content
    data = json.loads(raw_res)
    
    topics = data.get("topics")
    if not topics:
        for val in data.values():
            if isinstance(val, list):
                topics = val
                break
    
    if not topics:
        log_event(f"Debug: AI returned no topics.", "WARNING")
        return []
        
    return topics

def evaluate_topic(topic, existing_content):
    prompt = f"""
    Internal Content Baseline (Recent Ideas): {json.dumps(existing_content)}
    
    Topic to Evaluate: 
    Title: {topic['title']}
    Source: {topic['source_name']}
    Summary: {topic['summary']}

    Evaluation Rules:
    1. Anchors: Must fit at least one: {ANCHORS}
    2. Lenses: Map to one of these: {LENSES}
    3. Mandatory Questions:
       - Does this clearly connect to at least one theme?
       - Which theme does it map to?
       - Can the connection be explained without stretching?
    4. Duplicate Check: Is it a hard duplicate (matches approved content in last 45 days)?

    Return JSON:
    {{
       "approved": bool,
       "rejection_reason": "off-anchor|duplicate|weak-source" or null,
       "lens_mapped": "string",
       "connection_explanation": "string",
       "why_it_matters": "string",
       "post_angle": "string",
       "confidence": "Low|Medium|High",
       "duplicate_status": "new|related"
    }}
    """
    
    response = CLIENT.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": "Evaluate for brand alignment within a professional research ecosystem."},
                  {"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    return json.loads(response.choices[0].message.content)

def main():
    log_event(" Starting Research Discovery Cycle...")
    
    try:
        sh = get_sheets()
    except Exception as e:
        log_event(f"Sheet access failed: {e}", "ERROR")
        return

    # 1. READ CURRENT HEADERS (Respect User Formatting)
    topic_ws_obj = get_or_create_worksheet(sh, "Topic_Ideas", ["Approved Topic", "Date Added", "Topic ID", "Topic Title", "Why it Matters", "Connection", "Lens", "Post Angle", "Confidence"])
    topic_headers = topic_ws_obj.row_values(1)
    
    source_ws_obj = get_or_create_worksheet(sh, "Sources", ["Topic ID", "Topic Title", "Source Name", "Source Link"])
    source_headers = source_ws_obj.row_values(1)
    
    rejected_ws_obj = get_or_create_worksheet(sh, "Rejected_Log", ["Date", "Topic", "Source", "Rejection Type", "Explanation"])
    rejected_headers = rejected_ws_obj.row_values(1)

    # 2. Ingest baseline from Topic_Ideas
    log_event("Ingesting internal baseline (Header Aware)...")
    all_rows = topic_ws_obj.get_all_values()
    
    # Dynamically find the index of the columns we need
    try:
        approved_col_idx = topic_headers.index("Approved Topic")
        title_col_idx = topic_headers.index("Topic Title")
    except ValueError as e:
        log_event(f"Mandatory Header Missing: {e}", "ERROR")
        return

    approved_titles = [row[title_col_idx] for row in all_rows[1:] if row[approved_col_idx].upper() == 'TRUE']
    log_event(f"Found {len(approved_titles)} previously approved topics.")

    # 3. Discovery
    raw_topics = discover_topics()
    log_event(f"Found {len(raw_topics)} potential raw topics.")

    # 4. Evaluation & Dynamic Persistence
    valid_idea_rows = []
    source_rows = []
    rejected_rows = []
    
    now_ts = datetime.now()
    date_str = now_ts.strftime("%Y-%m-%d")

    for idx, rt in enumerate(raw_topics):
        log_event(f"Critiquing: {rt['title']}...")
        result = evaluate_topic(rt, approved_titles)
        
        if result['approved']:
            log_event(f"  [ACCEPTED] - Lens: {result['lens_mapped']}", "SUCCESS")
            topic_id = f"T-{now_ts.strftime('%y%m%d')}-{idx:02d}"
            
            # Source Data
            s_url = rt.get('source_url', '#')
            s_name = rt.get('source_name', 'Institution')
            source_link = f'=HYPERLINK("{s_url}", "{s_name}")'
            
            # Map Data to Columns (Using User's Order)
            topic_data = {
                "Approved Topic": "FALSE",
                "Date Added": date_str,
                "Topic ID": topic_id,
                "Topic Title": rt['title'],
                "Why it Matters": result['why_it_matters'],
                "Connection": result['connection_explanation'],
                "Lens": result['lens_mapped'],
                "Post Angle": result['post_angle'],
                "Confidence": result['confidence']
            }
            valid_idea_rows.append(map_row_data(topic_data, topic_headers))
            
            source_data = {
                "Topic ID": topic_id,
                "Topic Title": rt['title'],
                "Source Name": s_name,
                "Source Link": source_link
            }
            source_rows.append(map_row_data(source_data, source_headers))
        else:
            log_event(f"  [REJECTED] - Reason: {result['rejection_reason']}", "WARNING")
            reject_data = {
                "Date": date_str,
                "Topic": rt['title'],
                "Source": rt.get('source_name', 'Unknown'),
                "Rejection Type": result['rejection_reason'],
                "Explanation": result['connection_explanation']
            }
            rejected_rows.append(map_row_data(reject_data, rejected_headers))

    # 5. Save & Apply Dynamic Checkboxes
    if valid_idea_rows:
        start_row = len(topic_ws_obj.get_all_values()) + 1
        end_row = start_row + len(valid_idea_rows) - 1
        topic_ws_obj.append_rows(valid_idea_rows, value_input_option="USER_ENTERED")
        
        # Determine Checkbox column (in case user moved it)
        try:
            check_col_idx = topic_headers.index("Approved Topic")
            sheet_id = topic_ws_obj._properties['sheetId']
            body = {
                "requests": [{
                    "setDataValidation": {
                        "range": {"sheetId": sheet_id, "startRowIndex": start_row-1, "endRowIndex": end_row, "startColumnIndex": check_col_idx, "endColumnIndex": check_col_idx+1},
                        "rule": {"condition": {"type": "BOOLEAN", "values": []}, "showCustomUi": True, "strict": True}
                    }
                }]
            }
            sh.batch_update(body)
        except: pass # Don't crash if they renamed 'Approved Topic'
        
        source_ws_obj.append_rows(source_rows, value_input_option="USER_ENTERED")
        log_event(f"Saved {len(valid_idea_rows)} ideas and {len(source_rows)} sources.")
    
    if rejected_rows:
        rejected_ws_obj.append_rows(rejected_rows, value_input_option="USER_ENTERED")
        log_event(f"Logged {len(rejected_rows)} rejections.")

    log_event(" Cycle complete (Respecting Manual Formatting).")

if __name__ == "__main__":
    main()
