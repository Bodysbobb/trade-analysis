"""
Author: Pattawee Puangchit
Purpose: Automates the downloading, merging, and summarizing of trade data from UN Comtrade 
and macroeconomic indicators from the World Bank API, as well as pre-downloaded CEPII data (if needed), 
into unified CSV and Excel outputs.

For more information, please refer to https://www.pattawee-pp.com/blog/2025/trade-research-api/
"""

# pip install pandas wbgapi tqdm comtradeapicall openpyxl
import os
import pandas as pd
import wbgapi as wb
import concurrent.futures
import comtradeapicall
from tqdm import tqdm
import numpy as np
import openpyxl 

# ==========================================================
# CONFIGURATION
# ==========================================================

"""
# Variables Explanation:
# - YEARS: List or range of years to download, e.g., range(2015,2021) or single year [2015].
# - REPORTERS: List of ISO3 reporter countries, e.g., ["THA","MYS"] or ["all"].
# - PARTNERS: List of ISO3 partner countries or ["all"].
# - OUTPUT_DIR: Output directory for saving results.
# - MERGE_DATA: Merge UN Comtrade & World Bank data automatically (True/False).
# - MAPPING_PATH: Optional Excel file for ISO ↔ numeric code mapping.
# - WB_VARIABLES: List of World Bank indicator codes to extract.
# - WB_MAPPING_SHEET: Sheet name in mapping file with readable WB variable names.
# - USE_WB_VARIABLE_NAME: False = rename WB codes to readable names using mapping file.
# - UN_PARAMS: Parameters passed directly to the UN Comtrade API.
# - CEPII_PATH: Path to the CEPII Gravity V202211 or similar CSV file.
# - CEPII_VARIABLES: List of CEPII variables to include, or ["all"].
"""
CONFIG = {
    # === COMMON SETTINGS ===
    "YEARS": [2015],
    "REPORTERS": ["THA", "USA"],
    "PARTNERS": ["THA", "USA"],
    "OUTPUT_DIR": r"YOUR\PATH\TO\OUTPUT",
    "MAPPING_PATH": r"YOUR\PATH\TO\MAPPING\API_Mapping.xlsx",
    "CEPII_PATH": r"YOUR\PATH\TO\CEPII\CEPII_Symmetric.csv",
    "MERGE_DATA": True,

    # === WORLD BANK SETTINGS ===
    "WB_VARIABLES": [ 
        'NY.GDP.MKTP.CD',
        'NY.GDP.MKTP.KD',
        'NY.GDP.PCAP.KD',
        'FP.CPI.TOTL.ZG',
        'SP.POP.TOTL',
        'NV.IND.MANF.ZS',
        'AG.LND.AGRI.ZS',
        'NE.EXP.GNFS.ZS',
        'NE.IMP.GNFS.ZS'
    ],
    "WB_MAPPING_SHEET": "WorldBank_DataList",
    "USE_WB_VARIABLE_NAME": False,

    # === UN COMTRADE SETTINGS ===
    "UN_PARAMS": {
        "subscription_key": "YOUR_SUBSCRIPTION_KEY",
        "typeCode": "C",
        "freqCode": "A",
        "clCode": "HS",
        "cmdCode": "AG2",
        "flowCode": "M",
        "partner2Code": "0",
        "customsCode": "C00",
        "motCode": "0",
        "maxRecords": 500000,
        "format_output": "JSON",
        "aggregateBy": None,
        "breakdownMode": "plus",
        "countOnly": None,
        "includeDesc": True
    },
    
    # === CEPII SETTINGS ===
    "CEPII_VARIABLES": ["distw_harmonic", "contig", "comlang_off"] # or ["all"]
}

os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)


# ====================================================================
# DO NOT CHANGE BELOW THIS LINE UNLESS YOU ARE SURE WHAT YOU ARE DOING
# ====================================================================
HARDCODED_COMTRADE_MAP = {
    "USA": "842", "THA": "764", "CHN": "156", "DEU": "276", "JPN": "392", 
    "AFG": "004", "ALB": "008", "DZA": "012", "ASM": "016", "AND": "020", 
    "AGO": "024", "ATG": "028", "AZE": "031", "ARG": "032", "AUS": "036", 
    "AUT": "040", "BHS": "044", "BHR": "048", "BGD": "050", "ARM": "051", 
    "BRB": "052", "BEL": "056", "BMU": "060", "BTN": "064", "BOL": "068", 
    "BIH": "070", "BWA": "072", "BRA": "076", "BLZ": "084", "SLB": "090", 
    "BRN": "096", "BGR": "100", "MMR": "104", "BDI": "108", "BLR": "112", 
    "KHM": "116", "CMR": "120", "CAN": "124", "CPV": "132", "CAF": "140", 
    "LKA": "144", "TCD": "148", "CHL": "152", "COL": "170", "COM": "174", 
    "COG": "178", "CRI": "188", "HRV": "191", "CUB": "192", "CYP": "196", 
    "CZE": "203", "BEN": "204", "DNK": "208", "DOM": "214", "ECU": "218", 
    "EGY": "818", "SLV": "222", "GNQ": "226", "ERI": "232", "EST": "233", 
    "ETH": "231", "FRO": "234", "FIN": "246", "FRA": "250", "GAB": "266", 
    "GMB": "270", "GEO": "268", "GHA": "288", "GRC": "300", "GRL": "304", 
    "GRD": "308", "GTM": "320", "GIN": "324", "GUY": "328", "HTI": "332", 
    "HND": "340", "HKG": "344", "HUN": "348", "ISL": "352", "IND": "356", 
    "IDN": "360", "IRN": "364", "IRQ": "368", "IRL": "372", "ISR": "376", 
    "ITA": "380", "JAM": "388", "JPN": "392", "KAZ": "398", "JOR": "400", 
    "KEN": "404", "PRK": "408", "KOR": "410", "KWT": "414", "KGZ": "417", 
    "LAO": "418", "LVA": "428", "LBN": "422", "LSO": "426", "LBR": "430", 
    "LBY": "434", "LTU": "440", "LUX": "442", "MAC": "446", "MDG": "450", 
    "MWI": "454", "MYS": "458", "MDV": "462", "MLI": "466", "MLT": "470", 
    "MRT": "478", "MUS": "480", "MEX": "484", "MNG": "496", "MAR": "504", 
    "MOZ": "508", "NAM": "516", "NPL": "524", "NLD": "528", "NZL": "554", 
    "NIC": "558", "NER": "562", "NGA": "566", "NOR": "578", "OMN": "512", 
    "PAK": "586", "PAN": "591", "PNG": "598", "PRY": "600", "PER": "604", 
    "PHL": "608", "POL": "616", "PRT": "620", "QAT": "634", "ROU": "642", 
    "RUS": "643", "RWA": "646", "WSM": "882", "SMR": "674", "SAU": "682", 
    "SEN": "686", "SYC": "690", "SLE": "694", "SGP": "702", "SVK": "703", 
    "SVN": "705", "SOM": "706", "ZAF": "710", "ESP": "724", "SDN": "729", 
    "SUR": "740", "SWE": "752", "CHE": "756", "SYR": "760", "TJK": "762", 
    "TGO": "768", "TTO": "780", "TUN": "788", "TUR": "792", "TKM": "795", 
    "UGA": "800", "UKR": "804", "ARE": "784", "GBR": "826", "TZA": "834", 
    "URY": "858", "UZB": "860", "VEN": "862", "VNM": "704", "YEM": "887", 
    "ZMB": "894", "ZWE": "716", "TWN": "158", "ESH": "732"
}

# ==========================================================
# HELPER: read mapping and convert ISO → numeric
# ==========================================================
def load_country_mapping():
    path = CONFIG.get("MAPPING_PATH")
    
    loaded_map = {} 

    # 1. Try to load mapping from the external file
    if path and os.path.exists(path):
        try:
            map_df = pd.read_excel(path, sheet_name="UNComtrade", dtype=str)
            map_df['CountryCode'] = map_df['CountryCode'].str.strip().str.zfill(3)
            map_df['ISO3'] = map_df['ISO3'].str.strip().str.upper()
            loaded_map = dict(zip(map_df['ISO3'], map_df['CountryCode']))
            print("Successfully loaded map from Excel.")
        except Exception as e:
            print(f"⚠ Failed to load mapping file from Excel: {e}.")
    else:
        print("⚠ Mapping file not found or path missing.")

    # 2. Overlay the hardcoded map to guarantee correct reporter codes
    if loaded_map:
        loaded_map.update(HARDCODED_COMTRADE_MAP)
        print("Hardcoded map applied over Excel map for reliable reporting codes.")
    else:
        loaded_map = HARDCODED_COMTRADE_MAP
        print("Using hardcoded map as primary source.")

    return loaded_map

COUNTRY_MAP = load_country_mapping()

# helper: list of numeric reporter codes
def get_reporter_codes():
    if CONFIG["REPORTERS"] == ["all"]:
        # Use all values from the loaded map
        return sorted(set(v for v in COUNTRY_MAP.values()))
    
    out = []
    for iso in CONFIG["REPORTERS"]:
        iso_upper = iso.upper() 
        if iso_upper in COUNTRY_MAP:
            out.append(COUNTRY_MAP[iso_upper])
        else:
            print(f"⚠ Reporter ISO {iso} not found in country code mapping.")
    return sorted(set(out))

# helper: list of ISO3 partners to filter later
def get_partner_iso_list():
    if CONFIG["PARTNERS"] == ["all"]:
        return None
    return CONFIG["PARTNERS"]

# helper: get WB column name map from Excel file
def get_wb_column_map():
    path = CONFIG.get("MAPPING_PATH")
    if path and os.path.exists(path) and not CONFIG["USE_WB_VARIABLE_NAME"]:
        try:
            desc_df = pd.read_excel(path, sheet_name=CONFIG["WB_MAPPING_SHEET"])
            
            # Filter to only include variables requested in CONFIG
            desc_df = desc_df[desc_df["ID"].isin(CONFIG["WB_VARIABLES"])]
            
            # Select and rename columns for the summary table
            return desc_df[["ID", "Description"]].rename(columns={"ID": "WB_Code", "Description": "WB_Description"})
        except Exception:
            # If mapping fails or file isn't found, create a table with only the WB codes
            data = {"WB_Code": CONFIG["WB_VARIABLES"], "WB_Description": ["(Mapping Failed/Code Used)" for _ in CONFIG["WB_VARIABLES"]]}
            return pd.DataFrame(data)
    
    # If using default variable names or file not available, return list of codes
    data = {"WB_Code": CONFIG["WB_VARIABLES"], "WB_Description": ["(Default Code Used)" for _ in CONFIG["WB_VARIABLES"]]}
    return pd.DataFrame(data)

# ==========================================================
# 0. CEPII DATA PREPARATION
# ==========================================================
def prepare_cepii_data():
    """Loads CEPII data, processes it into a symmetric (order-invariant) format, 
    and selects variables based on CONFIG["CEPII_VARIABLES"].
    """
    input_path = CONFIG.get("CEPII_PATH")
    
    # === CRUCIAL SKIP CHECK ===
    if not input_path or not os.path.exists(input_path):
        print("⚠ CEPII input file not found or path missing. Skipping CEPII merge.")
        return pd.DataFrame()

    print("Processing CEPII data...")
    try:
        df = pd.read_csv(input_path, dtype=str)
    except Exception as e:
        print(f"Error reading CEPII file: {e}. Skipping CEPII merge.")
        return pd.DataFrame()
    
    # Define all relevant CEPII columns
    all_cepii_cols = [
        "iso3_o", "iso3_d",
        "distw_harmonic", "distw_arithmetic", "distw_harmonic_jh", "distw_arithmetic_jh", "dist",
        "main_city_source_o", "main_city_source_d", "distcap",
        "contig", "diplo_disagreement", "scaled_sci_2021",
        "comlang_off", "comlang_ethno",
        "comcol", "col45",
        "legal_old_o", "legal_old_d", "legal_new_o", "legal_new_d",
        "comleg_pretrans", "comleg_posttrans", "transition_legalchange",
        "comrelig",
        "heg_o", "heg_d",
        "col_dep_ever", "col_dep", "col_dep_end_year", "col_dep_end_conflict",
        "empire",
        "sibling_ever", "sibling", "sever_year", "sib_conflict",
    ]
    
    # Determine columns to keep based on config
    cepii_vars_to_keep = CONFIG["CEPII_VARIABLES"]
    if cepii_vars_to_keep == ["all"]:
        # Keep all non-ISO and non-directional columns that exist in the file
        cols_to_keep = [c for c in all_cepii_cols if c not in ["iso3_o", "iso3_d"] and c in df.columns]
    else:
        # Keep only the requested columns that exist in the file
        cols_to_keep = [c for c in cepii_vars_to_keep if c in df.columns]
    
    cols_to_keep_with_iso = ["iso3_o", "iso3_d"] + cols_to_keep
    
    # 1. Load and Trim
    existing_cols = [c for c in cols_to_keep_with_iso if c in df.columns]
    if not ("iso3_o" in existing_cols and "iso3_d" in existing_cols):
        print("CEPII file missing required 'iso3_o' or 'iso3_d' columns. Skipping CEPII merge.")
        return pd.DataFrame()
        
    df = df[existing_cols].copy()
    
    # Identify numeric columns among the kept variables
    num_cols_all = [c for c in all_cepii_cols if c not in ["iso3_o", "iso3_d"]]
    num_cols = [c for c in existing_cols if c in num_cols_all]
    
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # 2. Create Order-Invariant Keys
    df["key_a"] = np.where(df["iso3_o"] < df["iso3_d"], df["iso3_o"], df["iso3_d"])
    df["key_b"] = np.where(df["iso3_o"] < df["iso3_d"], df["iso3_d"], df["iso3_o"])
    
    # 3. Aggregate by (key_a, key_b) using first_nonnull
    def first_nonnull(series):
        # return the first value that is not NaN
        for v in series:
            if pd.notna(v):
                return v
        return np.nan

    agg_dict = {}
    for c in cols_to_keep:
        if c in df.columns:
            agg_dict[c] = first_nonnull

    if not agg_dict:
        print("No CEPII variables selected or found. Returning empty DataFrame.")
        return pd.DataFrame()
        
    collapsed = df.groupby(["key_a", "key_b"], as_index=False).agg(agg_dict)
    
    # Create the symmetric key
    collapsed["cepii_key"] = collapsed["key_a"] + "-" + collapsed["key_b"]
    
    # Keep only the key and the CEPII variables
    final_cols = ["cepii_key"] + cols_to_keep
    final_df = collapsed[final_cols].copy()
    
    print(f"Processed CEPII data for {len(final_df)} unique country pairs.")
    return final_df

# ==========================================================
# 1. WORLD BANK
# ==========================================================
def fetch_world_bank_data():
    print("Fetching World Bank data...")
    wb.db = 2
    wb_mapping_available = CONFIG.get("MAPPING_PATH") and os.path.exists(CONFIG["MAPPING_PATH"])

    # Determine the list of ISO codes to fetch WB data for
    all_iso = list(COUNTRY_MAP.keys())

    reporters_iso = all_iso if CONFIG["REPORTERS"] == ["all"] else CONFIG["REPORTERS"]
    partners_iso  = all_iso if CONFIG["PARTNERS"] == ["all"] else CONFIG["PARTNERS"]
    wb_iso_list = sorted(set([iso.upper() for iso in reporters_iso + partners_iso if iso.upper() != "ALL"]))
    years = list(CONFIG["YEARS"])

    print(f"World Bank request: {len(wb_iso_list)} unique ISO codes")
    print(f"Years: {years[0]}–{years[-1]}")

    df = wb.data.DataFrame(
        series=CONFIG["WB_VARIABLES"],
        economy=wb_iso_list,
        time=years,
        labels=False,
        columns="series",
        index=["economy", "time"]
    ).reset_index()

    df = df.rename(columns={"economy": "countryISO", "time": "Year"})
    df["Year"] = df["Year"].astype(str).str.replace("YR", "", regex=False).astype(int)

    # Original logic for renaming WB variables using the external file
    if wb_mapping_available and not CONFIG["USE_WB_VARIABLE_NAME"]:
        try:
            desc_map = pd.read_excel(CONFIG["MAPPING_PATH"], sheet_name=CONFIG["WB_MAPPING_SHEET"])
            rename_map = dict(zip(desc_map["ID"], desc_map["Description"]))
            df = df.rename(columns=rename_map)
        except Exception:
            print("⚠ Failed to load mapping sheet. Using default WB variable names.")

    out_path = os.path.join(CONFIG["OUTPUT_DIR"], "WorldBank_Data.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved World Bank data → {out_path}")
    return df

# ==========================================================
# 2. UN COMTRADE 
# ==========================================================
def fetch_un_comtrade_data():
    print("Fetching UN Comtrade data...")

    reporter_codes = get_reporter_codes()
    if not reporter_codes:
        print("⚠ No reporter codes found from mapping (REPORTERS).")
        return pd.DataFrame()

    years = list(CONFIG["YEARS"])
    params = CONFIG["UN_PARAMS"]
    hs_level = params["cmdCode"]
    base_path = CONFIG["OUTPUT_DIR"]

    def fetch_single(year, reporter_code):
        print(f"Fetching data for year {year}, reporter {reporter_code}...")
        try:
            df = comtradeapicall.getFinalData(
                params["subscription_key"],
                typeCode=params["typeCode"],
                freqCode=params["freqCode"],
                clCode=params["clCode"],
                period=str(year),
                reporterCode=str(reporter_code),
                cmdCode=hs_level,
                flowCode=params["flowCode"],
                partnerCode=None, 
                partner2Code=params["partner2Code"],
                customsCode=params["customsCode"],
                motCode=params["motCode"],
                maxRecords=params["maxRecords"],
                format_output=params["format_output"],
                aggregateBy=params["aggregateBy"],
                breakdownMode=params["breakdownMode"],
                countOnly=params["countOnly"],
                includeDesc=params["includeDesc"]
            )
            
            if df is not None and len(df) > 0:
                print(f"Fetched {len(df)} records for reporter {reporter_code}")
                
                # Robust JSON/dict response handling
                if isinstance(df, dict) and "dataset" in df:
                    df = pd.json_normalize(df["dataset"])
                elif isinstance(df, list):
                    df = pd.json_normalize(df)
                
                if isinstance(df, pd.DataFrame):
                    return df
                else:
                    print(f"Data for {reporter_code} could not be converted to DataFrame.")
                    return pd.DataFrame()
            else:
                print(f"No data returned for reporter {reporter_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error for reporter {reporter_code}: {e}")
            return pd.DataFrame()

    all_years_data = []

    for year in years:
        print(f"\nProcessing data for year {year}...")
        year_data = []
        group_size = 20
        reporter_groups = [reporter_codes[i:i + group_size] for i in range(0, len(reporter_codes), group_size)]
        
        for i, group in enumerate(reporter_groups):
            print(f"Processing group {i+1}/{len(reporter_groups)}: Reporters {group[0]} to {group[-1]}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_single, year, code): code for code in group}
                for f in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc=f"Year {year}"):
                    df_chunk = f.result()
                    if not df_chunk.empty:
                        year_data.append(df_chunk)
                        
        if year_data:
            combined = pd.concat(year_data, ignore_index=True)
            out_path = os.path.join(base_path, f"UNComtrade_{hs_level}_{year}.csv")
            combined.to_csv(out_path, index=False)
            print(f"Saved {len(combined):,} rows → {out_path}")
            all_years_data.append(combined)
        else:
            print("No data returned for this year.")
        print(f"Year {year} finished.\n")

    if not all_years_data:
        print("No UN Comtrade data fetched.")
        return pd.DataFrame()

    merged = pd.concat(all_years_data, ignore_index=True)

    partner_keep_list = get_partner_iso_list()
    if partner_keep_list is not None and "partnerISO" in merged.columns:
        merged = merged[merged["partnerISO"].isin(partner_keep_list)]

    merged_path = os.path.join(CONFIG["OUTPUT_DIR"], f"UNComtrade_{hs_level}_Merged.csv")
    merged.to_csv(merged_path, index=False)
    print(f"Merged file saved → {merged_path}")
    return merged

# ==========================================================
# 3. MERGE ALL DATASETS
# ==========================================================
def merge_datasets(df_wb, df_trade, df_cepii):
    print("Merging datasets...")
    if df_trade.empty:
        print("Trade data is empty, skipping merge.")
        return df_trade
    
    if "period" in df_trade.columns:
        df_trade = df_trade.rename(columns={"period": "year"})
    df_trade["year"] = pd.to_numeric(df_trade["year"], errors="coerce").astype("Int64")
    
    # 3.1 Merge World Bank Data
    wb_rep = df_wb.add_suffix("_reporter").rename(columns={"countryISO_reporter": "reporterISO", "Year_reporter": "year"})
    wb_par = df_wb.add_suffix("_partner").rename(columns={"countryISO_partner": "partnerISO", "Year_partner": "year"})
    
    merged = df_trade.merge(wb_rep, on=["reporterISO", "year"], how="left")
    merged = merged.merge(wb_par, on=["partnerISO", "year"], how="left")
    
    # 3.2 Merge CEPII Data (Bilateral Symmetric Merge)
    if not df_cepii.empty and "reporterISO" in merged.columns and "partnerISO" in merged.columns:
        print("Merging CEPII data symmetrically...")
        
        # Create the symmetric key for the trade data: min(R, P) - max(R, P)
        merged["cepii_merge_key"] = np.where(
            merged["reporterISO"] < merged["partnerISO"],
            merged["reporterISO"] + "-" + merged["partnerISO"],
            merged["partnerISO"] + "-" + merged["reporterISO"]
        )
        
        # Merge the CEPII data using the symmetric key
        # The key column in df_cepii is 'cepii_key'
        merged = merged.merge(df_cepii, left_on="cepii_merge_key", right_on="cepii_key", how="left")
        
        # Drop temporary keys
        merged = merged.drop(columns=["cepii_merge_key", "cepii_key"], errors='ignore')
        print("CEPII merge completed.")
    else:
        print("Skipping CEPII merge (CEPII data is empty or trade data columns missing).")

    merged.to_csv(os.path.join(CONFIG["OUTPUT_DIR"], "Merged_Trade_WB_CEPII.csv"), index=False)
    print("Merged data file saved.")
    return merged

# ==========================================================
# 4. SUMMARY REPORT
# ==========================================================
def export_summary(df):
    print("Generating summary report...")
    if df.empty:
        print("Summary skipped (no data).")
        return
    out_path = os.path.join(CONFIG["OUTPUT_DIR"], "Summary_Report.xlsx")
    
    # Identify numeric columns for statistical summaries
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    with pd.ExcelWriter(out_path, engine='openpyxl') as w:
        
        # Original Sheets
        if 'year' in df.columns and 'reporterISO' in df.columns:
            df.groupby('year')['reporterISO'].nunique().reset_index(name='UniqueReporters').to_excel(
                w, sheet_name='Yearly_Reporters', index=False
            )
            pd.crosstab(df['reporterISO'], df['year']).to_excel(
                w, sheet_name='CountryYearMatrix'
            )
        
        # Missing Data Sheet (Updated to include CEPII)
        if 'year' in df.columns:
            missing_data_df = df.isna().groupby(df['year']).sum()
            missing_data_df.to_excel(
                w, sheet_name='MissingData'
            )
            
        if 'cmdCode' in df.columns and 'year' in df.columns:
            df.groupby(['year','cmdCode']).size().reset_index(
                name='ObsCount'
            ).to_excel(w, sheet_name='ProductSummary', index=False)
        
        # New Sheet 1: World Bank Variable Mapping
        wb_map_df = get_wb_column_map()
        wb_map_df.to_excel(
            w, sheet_name='WB_Variable_Map', index=False
        )

        # New Sheet 2: Unique Country-Product Combinations per Year
        if 'year' in df.columns and 'reporterISO' in df.columns and 'partnerISO' in df.columns and 'cmdCode' in df.columns:
            df.groupby('year')[['reporterISO', 'partnerISO', 'cmdCode']].apply(
                lambda x: len(x.drop_duplicates())
            ).reset_index(name='Unique_ReporterPartnerCmd_Combos').to_excel(
                w, sheet_name='Unique_Combos_Year', index=False
            )
        
        # New Sheet 3: Statistics per Reporter (Fixed FutureWarning)
        if 'reporterISO' in df.columns and numeric_cols:
            df.groupby('reporterISO')[numeric_cols].describe().stack(future_stack=True).unstack(level=1).to_excel(
                w, sheet_name='Stats_by_Reporter'
            )
            
        # New Sheet 4: Statistics per Year (Fixed FutureWarning)
        if 'year' in df.columns and numeric_cols:
            df.groupby('year')[numeric_cols].describe().stack(future_stack=True).unstack(level=1).to_excel(
                w, sheet_name='Stats_by_Year'
            )
            
        # New Sheet 5: Statistics per Product Code (Fixed FutureWarning)
        if 'cmdCode' in df.columns and numeric_cols:
            df.groupby('cmdCode')[numeric_cols].describe().stack(future_stack=True).unstack(level=1).to_excel(
                w, sheet_name='Stats_by_Product'
            )

    print(f"Summary saved → {out_path}")


# ==========================================================
# MAIN
# ==========================================================
def main():
    print("=== Trade Data Routine Started ===")
    
    # 0. CEPII data processing (handles missing file and skips)
    cepii_df = prepare_cepii_data()
    
    # 1. World Bank data
    wb_df = fetch_world_bank_data()
    
    # 2. UN Comtrade data
    trade_df = fetch_un_comtrade_data()
    
    if CONFIG["MERGE_DATA"]:
        # 3. Merge all data (including CEPII, which is skipped if empty)
        merged_df = merge_datasets(wb_df, trade_df, cepii_df)
        
        # 4. Export summary
        export_summary(merged_df)
        
    print("=== Done ===")

if __name__ == "__main__":
    main()