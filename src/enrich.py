"""
==================================================================
BUDGET ENRICHMENT MODULE
------------------------------------------------------------------
This module is responsible for transforming Budget Allocation 
data with standardized metadata, mappings, business logic to 
produce cleaned data for downstream usage.

By centralizing enrichment rules, this module ensures transparency, 
consistency and maintainability across the Budget Allocation 
pipeline.

✔️ Merges budget data with dimension tables for unified identifiers  
✔️ Standardizes program track, type and group naming conventions  
✔️ Extracts and normalizes key performance metrics across campaigns  
✔️ Cleans and validates data to ensure schema and field consistency  
✔️ Reduces payload size by removing redundant or raw fields

⚠️ This module focuses only on enrichment and transformation logic.  
It does not handle data fetching, ingestion or staging.
==================================================================
"""

# Add root directory to sys.path for absolute imports of internal modules
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

# Add Python datetime utilities for integration
from datetime import datetime

# Add Python logging ultilities for integraton
import logging

# Add Python regular expression operations ultilities for integraton
import re

# Add Python timezone ultilities for integration
import pytz

# Add Python time ultilities for integration
import time

# Add Python IANA time zone ultilities for integration
from zoneinfo import ZoneInfo

# Add Python Pandas libraries for integration
import pandas as pd

# Get environment variable for Company
COMPANY = os.getenv("COMPANY") 

# Get environment variable for Google Cloud Project ID
PROJECT = os.getenv("PROJECT")

# Get environment variable for Platform
PLATFORM = os.getenv("PLATFORM")

# Get environmetn variable for Department
DEPARTMENT = os.getenv("DEPARTMENT")

# Get environment variable for Account
ACCOUNT = os.getenv("ACCOUNT")

# Get nvironment variable for Layer
LAYER = os.getenv("LAYER")

# Get environment variable for Mode
MODE = os.getenv("MODE")

# 1. ENRICH BUDGET ALLOCATION FROM STAGING PHASE

# 1.1. Enrich budget allocation from staging phase
def enrich_budget_fields(enrich_df_input: pd.DataFrame, enrich_table_id: str) -> pd.DataFrame:
    print(f"🚀 [ENRICH] Starting to enrich staging Budget Allocation for {len(enrich_df_input)} row(s)...")
    logging.info(f"🚀 [ENRICH] Starting to enrich staging Budget Allocation for {len(enrich_df_input)} row(s)...")

    # 1.1.1. Start timing the staging Budget Allocation enrichment
    ICT = ZoneInfo("Asia/Ho_Chi_Minh")    
    enrich_time_start = time.time()   
    enrich_sections_status = {}
    enrich_sections_time = {}
    print(f"🔍 [ENRICH] Proceeding to enrich Budget Allocation for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")
    logging.info(f"🔍 [ENRICH] Proceeding to enrich Budget Allocation for {len(enrich_df_input)} row(s) at {datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S")}...")

    # 1.1.2. Validate input for the staging Budget Allocation enrichment
    enrich_section_name = "[ENRICH] Validate input for the staging Budget Allocation enrichment"
    enrich_section_start = time.time()    
    
    try:
        if enrich_df_input.empty:
            enrich_sections_status[enrich_section_name] = "failed"
            print("⚠️ [ENRICH] Empty staging Budget Allocation provided then enrichment is suspended.")
            logging.warning("⚠️ [ENRICH] Empty staging Budget Allocation provided then enrichment is suspended.")
        else:
            enrich_sections_status[enrich_section_name] = "succeed"
            print("✅ [ENRICH] Successfully validated input for staging Budget Allocation enrichment.")
            logging.info("✅ [ENRICH] Successfully validated input for staging Budget Allocation enrichment.")
    
    finally:
        enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    try:

    # 1.1.3. Enrich column name by unicode accent removal
        enrich_section_name = "[ENRICH] Enrich column name by unicode accent removal"
        enrich_section_start = time.time()      
        
        try:
            print(f"🔄 [FETCH] Enrich column name for {len(enrich_df_input.columns)} column(s) of Budget Allocation by unicode accent removal...")
            logging.info(f"🔄 [FETCH] Enrich column name for {len(enrich_df_input.columns)} column(s) of Budget Allocation by unicode accent removal...")
            enrich_df_accent = enrich_df_input.copy()    
            vietnamese_accents_mapping = {
                'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                'ă': 'a', 'ắ': 'a', 'ằ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
                'â': 'a', 'ấ': 'a', 'ầ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
                'đ': 'd',
                'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                'ê': 'e', 'ế': 'e', 'ề': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
                'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                'ô': 'o', 'ố': 'o', 'ồ': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
                'ơ': 'o', 'ớ': 'o', 'ờ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
                'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                'ư': 'u', 'ứ': 'u', 'ừ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
                'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
            }
            vietnamese_cases_upper = {k.upper(): v.upper() for k, v in vietnamese_accents_mapping.items()}
            vietnamese_characters_all = {**vietnamese_accents_mapping, **vietnamese_cases_upper}
            enrich_df_accent.columns = [
                ''.join(vietnamese_characters_all.get(c, c) for c in col) if isinstance(col, str) else col
                for col in enrich_df_accent.columns
            ]
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched for {len(enrich_df_accent.columns)} column(s) of Budget Allocation by unicode accent removal.")
            logging.info(f"✅ [ENRICH] Successfully enriched for {len(enrich_df_accent.columns)} column(s) of Budget Allocation by unicode accent removal.")            
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [FETCH] Failed to remove unicode accents from Budget Allocation column name due to {e}.")
            logging.error(f"❌ [FETCH] Failed to remove unicode accents from Budget Allocation column name due to {e}.")
        
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)   

    # 1.1.4. Enrich table fields for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich table fields for staging Budget Allocation"
        enrich_section_start = time.time()            
        
        try: 
            print(f"🔍 [ENRICH] Enriching table fields for staging Budget Allocation with {len(enrich_df_accent)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching table fields for staging Budget Allocation with {len(enrich_df_accent)} row(s)...")
            enrich_df_table = enrich_df_accent.copy()
            enrich_table_name = enrich_table_id.split(".")[-1]
            match = re.search(
                r"^(?P<company>\w+)_table_(?P<platform>\w+)_(?P<department>\w+)_(?P<account>\w+)_allocation_m\d{6}$",
                enrich_table_name
            )            
            enrich_df_table = enrich_df_table.assign(
                enrich_account_platform=match.group("platform") if match else "unknown",
                enrich_account_department=match.group("department") if match else "unknown",
                enrich_account_name=match.group("account") if match else "unknown"
            )            
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched table fields for staging Budget Allocation with {len(enrich_df_table)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched table fields for staging Budget Allocation with {len(enrich_df_table)} row(s).")            
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich table fields for staging Budget Allocation due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich table fields for staging Budget Allocation due to {e}.")
        
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.5. Enrich campaign fields for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich campaign fields for staging Budget Allocation"
        enrich_section_start = time.time()            
        
        try:
            print(f"🔍 [ENRICH] Enriching campaign fields for staging Budget Allocation with {len(enrich_df_table)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching campaign fields for staging Budget Allocation with {len(enrich_df_table)} row(s)...")
            enrich_df_campaign = enrich_df_table.copy()
            enrich_df_campaign["enrich_budget_actual"] = enrich_df_campaign["raw_budget_initial"] + enrich_df_campaign["raw_budget_adjusted"] + enrich_df_campaign["raw_budget_additional"]
            enrich_df_campaign["enrich_budget_marketing"] = (enrich_df_campaign["raw_budget_group"] == "KP") * enrich_df_campaign["enrich_budget_actual"]
            enrich_df_campaign["enrich_budget_supplier"] = (enrich_df_campaign["raw_budget_group"] == "NC") * enrich_df_campaign["enrich_budget_actual"]
            enrich_df_campaign["enrich_budget_retail"] = (enrich_df_campaign["raw_budget_group"] == "KD") * enrich_df_campaign["enrich_budget_actual"]
            enrich_df_campaign["enrich_budget_customer"] = (enrich_df_campaign["raw_budget_group"] == "CS") * enrich_df_campaign["enrich_budget_actual"]
            enrich_df_campaign["enrich_budget_recruitment"] = (enrich_df_campaign["raw_budget_group"] == "HC") * enrich_df_campaign["enrich_budget_actual"]
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched campaign fields for staging Budget Allocation with {len(enrich_df_campaign)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched campaign fields for staging Budget Allocation with {len(enrich_df_campaign)} row(s).")
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich campaign fields for staging Budget Allocation due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich campaign fields for staging Budget Allocation due to {e}.")
        
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2)

    # 1.1.6. Enrich date fields for staging Budget Allocation
        enrich_section_name = "[ENRICH] Enrich date fields for staging Budget Allocation"
        enrich_section_start = time.time()            
        
        try:
            print(f"🔍 [ENRICH] Enriching date fields for staging Budget Allocation with {len(enrich_df_campaign)} row(s)...")
            logging.info(f"🔍 [ENRICH] Enriching date fields for staging Budget Allocation with {len(enrich_df_campaign)} row(s)...")
            today = pd.to_datetime("today").normalize()
            enrich_df_date = enrich_df_campaign.copy()
            enrich_df_date = enrich_df_date.assign(
                last_updated_at=lambda _: datetime.utcnow().replace(tzinfo=pytz.UTC),
            )
            enrich_df_date["raw_date_start"] = pd.to_datetime(enrich_df_date.get("raw_date_start"), errors="coerce")
            enrich_df_date["raw_date_end"] = pd.to_datetime(enrich_df_date.get("raw_date_end"), errors="coerce")
            enrich_df_date["enrich_time_total"] = (enrich_df_date["raw_date_end"] - enrich_df_date["raw_date_start"]).dt.days
            enrich_df_date["enrich_time_passed"] = ((today - enrich_df_date["raw_date_start"]).dt.days.clip(lower=0))            
            enrich_sections_status[enrich_section_name] = "succeed"
            print(f"✅ [ENRICH] Successfully enriched date fields for staging Budget Allocation with {len(enrich_df_date)} row(s).")
            logging.info(f"✅ [ENRICH] Successfully enriched date fields for staging Budget Allocation with {len(enrich_df_date)} row(s).")    
        except Exception as e:
            enrich_sections_status[enrich_section_name] = "failed"
            print(f"❌ [ENRICH] Failed to enrich date fields for staging Budget Allocation due to {e}.")
            logging.error(f"❌ [ENRICH] Failed to enrich date fields for staging Budget Allocation due to {e}.")
        
        finally:
            enrich_sections_time[enrich_section_name] = round(time.time() - enrich_section_start, 2) 

    # 1.1.7. Summarize enrichment results for staging Budget Allocation
    finally:
        enrich_time_elapsed = round(time.time() - enrich_time_start, 2)
        enrich_df_final = enrich_df_date.copy() if not enrich_df_date.empty else pd.DataFrame()
        enrich_sections_total = len(enrich_sections_status)
        enrich_sections_failed = [k for k, v in enrich_sections_status.items() if v == "failed"]
        enrich_sections_succeeded = [k for k, v in enrich_sections_status.items() if v == "succeed"]
        enrich_rows_input = len(enrich_df_input)
        enrich_rows_output = len(enrich_df_final)
        enrich_sections_summary = list(dict.fromkeys(
            list(enrich_sections_status.keys()) +
            list(enrich_sections_time.keys())
        ))
        enrich_sections_detail = {
            enrich_section_summary: {
                "status": enrich_sections_status.get(enrich_section_summary, "unknown"),
                "time": enrich_sections_time.get(enrich_section_summary, None),
            }
            for enrich_section_summary in enrich_sections_summary
        }        
        if enrich_sections_failed:
            enrich_status_final = "enrich_failed_all"
            print(f"❌ [ENRICH] Failed to complete Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
            logging.error(f"❌ [ENRICH] Failed to complete Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) due to section(s) {', '.join(enrich_sections_failed)} in {enrich_time_elapsed}s.")
        elif enrich_rows_output == enrich_rows_input:
            enrich_status_final = "enrich_succeed_all"
            print(f"🏆 [ENRICH] Successfully completed Budget Allocations enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.info(f"🏆 [ENRICH] Successfully completed Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")            
        else:
            enrich_status_final = "enrich_succeed_partial"
            print(f"⚠️ [ENRICH] Partially completed Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")
            logging.warning(f"⚠️ [ENRICH] Partially completed Budget Allocation enrichment with {enrich_rows_output}/{enrich_rows_input} enriched row(s) in {enrich_time_elapsed}s.")                      
        enrich_results_final = {
            "enrich_df_final": enrich_df_final,
            "enrich_status_final": enrich_status_final,
            "enrich_summary_final": {
                "enrich_time_elapsed": enrich_time_elapsed,
                "enrich_sections_total": enrich_sections_total,
                "enrich_sections_succeed": enrich_sections_succeeded,
                "enrich_sections_failed": enrich_sections_failed,
                "enrich_sections_detail": enrich_sections_detail,
                "enrich_rows_input": enrich_rows_input,
                "enrich_rows_output": enrich_rows_output,
            },
        }
    return enrich_results_final