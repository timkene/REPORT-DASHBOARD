import polars as pl
from datetime import datetime, timedelta
import pandas as pd
import os


def load_data():
    # Load main data
    # Define the folder path
    folder_path = "/Users/kenechukwuchukwuka/Downloads/streamlit/DATADUMP"
    print("Loading data from:", folder_path)

    # Create full file paths by joining the folder path with filenames
    print("Reading parquet files...")
    main_df = pl.read_parquet(os.path.join(folder_path, "MEDICLOUD_Total_PA_Procedures.parquet"))
    provider = pl.read_parquet(os.path.join(folder_path, "MEDICLOUD_all_providers.parquet"))
    benefit = pl.read_parquet(os.path.join(folder_path, "NEW_BENEFIT.parquet"))
    debit = pl.read_parquet(os.path.join(folder_path, "EACOUNT_DEBIT_Note.parquet"))
    ledger = pl.read_parquet(os.path.join(folder_path, "EACOUNT_FIN_GL.parquet"))
    group_plan = pl.read_parquet(os.path.join(folder_path, "MEDICLOUD_group_plan.parquet"))
    group = pl.read_parquet(os.path.join(folder_path, "MEDICLOUD_all_group.parquet"))
    E_ACCT_GROUP = pl.read_parquet(os.path.join(folder_path, "MEDICLOUD_e_account_group.parquet"))
    print("All parquet files loaded successfully")

    # Cast columns to appropriate types
    print("Processing debit data...")
    debit = debit.with_columns([
        pl.col("Amount").cast(pl.Float64),  # Add this line to ensure amount is numeric
        pl.col("From").cast(pl.Date)  # Ensure dates are correctly typed
    ])

    # Filter DEBIT for dates within the last 12 months
    today = datetime.now().date()
    one_year_ago = today - timedelta(days=365)
    debit = debit.filter(
            pl.col('From').is_not_null() & 
            (pl.col('From') >= pl.lit(one_year_ago)) &
            (pl.col('From') <= pl.lit(today))
        )
    debit = debit.rename({"CompanyName": "groupname"})
    
    # Convert ID_Company and code columns to string type for joining
    # Cast columns to appropriate types
    print("Processing ledger data...")
    ledger = ledger.with_columns([
        pl.col("GLDate").cast(pl.Datetime),
        pl.col("GLAmount").cast(pl.Float64)  # Add this line to ensure granted is numeric
    ])
    E_ACCT_GROUP = E_ACCT_GROUP.with_columns(pl.col("ID_Company").cast(pl.Utf8))
    ledger = ledger.with_columns(pl.col("code").cast(pl.Utf8))
    GL = ledger.join(
        E_ACCT_GROUP.select(['ID_Company', 'CompanyName']),
        left_on='code',
        right_on='ID_Company',
        how='left'
    ).rename({"CompanyName": "groupname"})
    
    final_ledger = GL.filter(
        pl.col("acctype").is_in(["CURRENT ASSETS", "CASH"])
    ).group_by(
        ["code", "groupname", "GLDate", "GLDesc"]
    ).agg([
        pl.col("GLAmount").sum().alias("total_amount"),
        pl.col("GLAmount").alias("amounts"),
        pl.col("acctype").alias("acctypes")
    ])
    
    # Filter for groups where:
    # 1. Total amount is 0 (debits and credits match)
    # 2. There are exactly 2 entries (CURRENT ASSET and CASH)
    # 3. Contains both CURRENT ASSET and CASH entries
    matching_entries = final_ledger.filter(
        (pl.col("total_amount") == 0) & 
        (pl.col("amounts").list.len() == 2) &
        (pl.col("acctypes").list.contains("CURRENT ASSETS")) &
        (pl.col("acctypes").list.contains("CASH"))
    )
    
    # Extract the positive amount and create the result list
    ledger_result = matching_entries.select([
        "GLDesc",
        "groupname",
        "GLDate",
        pl.col("amounts").list.max().alias("GLAmount")
    ])

    # DATA MODIFICATION 
    print("Processing plan data...")
    groupname_plan = group_plan.join(
        group.select(["groupid", "groupname"]),  # Select only the columns we need from group
        on="groupid",                            # Join on the common column
        how="left")

    groupname_plan = groupname_plan.with_columns([
        pl.col("planid").cast(pl.Utf8),  # Ensure planid is string for grouping
        (pl.col('countoffamily') * pl.col('familyprice') + 
         pl.col('countofindividual') * pl.col('individualprice')).alias('PREMIUM')
    ])
    
    # Join provider data to main dataframe
    print("Processing main data...")
    main_dfff = main_df.join(
        provider.select(["providertin", "providername"]),
        left_on="providerid",
        right_on="providertin",
        how="left"
    )
    main_dff = main_dfff.join(
        benefit.select(["Benefit", "Code"]),
        left_on="code",
        right_on="Code",
        how="left"
    )
    
    # Print schema to help debug the granted column issue
    print("Main DataFrame schema:")
    for name, dtype in main_dff.schema.items():
        print(f"  {name}: {dtype}")
        
    # Print sample of the granted column
    print("\nSample of 'granted' column:")
    try:
        print(main_dff.select("granted").head(5))
    except Exception as e:
        print(f"Could not select granted column: {e}")
    
    # Process granted column - try multiple approaches in case it's already numeric
    print("\nProcessing granted column...")
    try:
        # First try direct casting if it's already numeric
        main_dff = main_dff.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("granted").cast(pl.Float64).alias("granted_numeric"),
            (pl.col("granted").cast(pl.Float64) * 1.4).alias("granted_with_markup")
        ])
        print("Processed granted column as numeric")
    except Exception as e:
        print(f"Error processing as numeric, trying as string: {e}")
        try:
            # Second try - handle as string with comma formatting
            main_dff = main_dff.with_columns([
                pl.col("requestdate").cast(pl.Datetime),
                pl.col("granted").str.replace(",", "").cast(pl.Float64).alias("granted_numeric"),
                (pl.col("granted").str.replace(",", "").cast(pl.Float64) * 1.4).alias("granted_with_markup")
            ])
            print("Processed granted column as string")
        except Exception as e2:
            print(f"Both approaches failed: {e2}")
            # Last resort - create dummy columns
            print("Creating dummy columns as fallback")
            main_dff = main_dff.with_columns([
                pl.col("requestdate").cast(pl.Datetime),
                pl.lit(0.0).alias("granted_numeric"),
                pl.lit(0.0).alias("granted_with_markup")
            ])
            print("WARNING: Using dummy values for granted column")

    return main_dff, ledger_result, debit, groupname_plan

def filter_by_date_range(df, start_date, end_date, date_column="requestdate"):
    # Convert string dates to datetime objects
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    return df.filter(
        (pl.col(date_column) >= start_dt) &
        (pl.col(date_column) <= end_dt)
    )

def generate_report():
    # Get user inputs
    print("\nHealth Insurance Analytics Report Generator")
    print("------------------------------------------")
    
    # Load data
    try:
        main_dff, ledger_result, debit, groupname_plan = load_data()
        print("Data loaded successfully!")
    except Exception as e:
        print(f"Error loading data: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Get client names for selection
    client_names = main_dff.select("groupname").unique().to_series().sort()
    
    # Print available clients
    print("\nAvailable Clients:")
    for i, name in enumerate(client_names, 1):
        print(f"{i}. {name}")
        
    # Get user selections    
    selected_client = input("\nEnter client name from the list above: ")
    start_date = input("Enter start date (YYYY-MM-DD): ")
    end_date = input("Enter end date (YYYY-MM-DD): ")
    
    # Get separate date range for debit data
    print("\nEnter date range for Debit analysis (can be different from above):")
    debit_start_date = input("Enter debit start date (YYYY-MM-DD): ")
    debit_end_date = input("Enter debit end date (YYYY-MM-DD): ")
    
    # Filter data for main analysis
    filtered_df = main_dff.filter(pl.col("groupname") == selected_client)
    filtered_df = filter_by_date_range(filtered_df, start_date, end_date)
    
    # Filter ledger data for the same client and date range
    filtered_ledger = ledger_result.filter(pl.col("groupname") == selected_client)
    filtered_ledger = filter_by_date_range(filtered_ledger, start_date, end_date, "GLDate")
    
    # Filter premium data for the selected client (no date filter needed)
    filtered_premium = groupname_plan.filter(pl.col("groupname") == selected_client)
    
    # Filter debit data with separate date range
    filtered_debit = debit.filter(pl.col("groupname") == selected_client)
    filtered_debit = filter_by_date_range(filtered_debit, debit_start_date, debit_end_date, "From")
    
    # Generate reports
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"reports_{selected_client}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Original reports
    # 1. Top customers by cost
    top_customers = (filtered_df
        .group_by("IID")
        .agg(pl.col("granted_with_markup").sum().alias("total_cost"))
        .sort("total_cost", descending=True)
        .head(20))
    top_customers.write_excel(f"{output_dir}/top_customers.xlsx")

    # 2. Top plans by cost
    top_plans = (filtered_df
        .group_by("plancode")
        .agg(pl.col("granted_with_markup").sum().alias("total_cost"))
        .sort("total_cost", descending=True)
        .head(20))
    top_plans.write_excel(f"{output_dir}/top_plans.xlsx")
    
    # 3. Hospital analysis
    hospital_metrics = (filtered_df
        .group_by("providername")
        .agg([
            pl.col("granted_with_markup").sum().alias("total_cost"),
            pl.n_unique("requestdate").alias("visit_count"),
            pl.n_unique("IID").alias("unique_patients"),
            (pl.col("granted_with_markup").sum() / pl.n_unique("requestdate")).alias("avg_cost_per_visit")
        ])
        .sort("total_cost", descending=True))
    hospital_metrics.write_excel(f"{output_dir}/hospital_analysis.xlsx")
    
    # 4. Time analysis
    time_analysis = (filtered_df
        .with_columns(pl.col("requestdate").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg([
            pl.col("granted_with_markup").sum().alias("total_cost"),
            pl.col("granted_with_markup").mean().alias("avg_cost"),
            pl.n_unique("IID").alias("unique_patients"),
            pl.n_unique("requestdate").alias("visit_count")
        ])
        .sort("month"))
    time_analysis.write_excel(f"{output_dir}/time_analysis.xlsx")
    
    # 5. Benefit type analysis
    benefit_analysis = (filtered_df
        .group_by("Benefit")
        .agg([
            pl.col("granted_with_markup").sum().alias("total_cost"),
            pl.n_unique("IID").alias("unique_patients"),
            pl.n_unique("requestdate").alias("visit_count")
        ])
        .sort("total_cost", descending=True))
    benefit_analysis.write_excel(f"{output_dir}/benefit_analysis.xlsx")
    
    # NEW REPORTS

    # 6. Ledger Analysis
    ledger_total = filtered_ledger.select(pl.col("GLAmount").sum()).item()
    ledger_analysis = (filtered_ledger
        .with_columns(pl.col("GLDate").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg([
            pl.col("GLAmount").sum().alias("total_amount"),
            pl.n_unique("GLDesc").alias("transaction_count")
        ])
        .sort("month"))
    ledger_analysis.write_excel(f"{output_dir}/ledger_analysis.xlsx")
    
    # 7. Premium Analysis - Total premium
    total_premium = filtered_premium.select(pl.col("PREMIUM").sum()).item()
    
    # 8. Premium Analysis by Plan
    premium_by_plan = (filtered_premium
        .group_by(["planid"])
        .agg([
            pl.col("PREMIUM").sum().alias("total_premium"),
            pl.n_unique("groupid").alias("group_count"),
            pl.sum("countofindividual").alias("total_individuals"),
            pl.sum("countoffamily").alias("total_families")
        ])
        .sort("total_premium", descending=True))
    premium_by_plan.write_excel(f"{output_dir}/premium_by_plan.xlsx")
    
    # 9. Debit Analysis
    debit_total = filtered_debit.select(pl.col("Amount").sum()).item()
    debit_analysis = (filtered_debit
        .with_columns(pl.col("From").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg([
            pl.col("Amount").sum().alias("total_amount")
        ])
        .sort("month"))
    debit_analysis.write_excel(f"{output_dir}/debit_analysis.xlsx")
    
    # 10. Enhanced Summary metrics
    summary_metrics = {
        "Total Cost": filtered_df.select(pl.col("granted_with_markup").sum()).item(),
        "Unique Customers": filtered_df.select(pl.n_unique("IID")).item(),
        "Unique Hospitals": filtered_df.select(pl.n_unique("providername")).item(),
        "Total Visits": filtered_df.select(pl.n_unique("requestdate")).item(),
        "Average Cost per Visit": filtered_df.select(pl.col("granted_with_markup").mean()).item(),
        "Total number of plan": filtered_df.select(pl.n_unique("plancode")).item(),
        "Total Ledger Amount": ledger_total,
        "Total Premium": total_premium,
        "Total Debit Amount": debit_total,
        "Debit Period": f"{debit_start_date} to {debit_end_date}"
    }
    pd.DataFrame([summary_metrics]).to_excel(f"{output_dir}/summary_metrics.xlsx")
    
    # 11. Financial Overview - Combined view of all financial metrics
    financial_overview = {
        "Metric": [
            "Total Claims Cost", 
            "Total Ledger Amount", 
            "Total Premium", 
            "Total Debit Amount",
            "Premium - Claims Difference",
            "Date Range (Main)",
            "Date Range (Debit)"
        ],
        "Amount": [
            filtered_df.select(pl.col("granted_with_markup").sum()).item(),
            ledger_total,
            total_premium,
            debit_total,
            total_premium - filtered_df.select(pl.col("granted_with_markup").sum()).item(),
            f"{start_date} to {end_date}",
            f"{debit_start_date} to {debit_end_date}"
        ]
    }
    pd.DataFrame(financial_overview).to_excel(f"{output_dir}/financial_overview.xlsx")
    
    # Export filtered raw data
    filtered_df.write_excel(f"{output_dir}/filtered_raw_data.xlsx")
    filtered_ledger.write_excel(f"{output_dir}/filtered_ledger_data.xlsx")
    filtered_premium.write_excel(f"{output_dir}/filtered_premium_data.xlsx")
    filtered_debit.write_excel(f"{output_dir}/filtered_debit_data.xlsx")
    
    print(f"\nReports generated successfully in folder: {output_dir}")
    print("The following reports were created:")
    print("1. top_customers.xlsx - Top 20 customers by total cost")
    print("2. top_plans.xlsx - Top plans by total cost")
    print("3. hospital_analysis.xlsx - Detailed metrics for each hospital")
    print("4. time_analysis.xlsx - Monthly trends and metrics")
    print("5. benefit_analysis.xlsx - Analysis by benefit type")
    print("6. ledger_analysis.xlsx - Monthly ledger transactions and amounts")
    print("7. premium_by_plan.xlsx - Premium analysis by plan")
    print("8. debit_analysis.xlsx - Monthly debit transactions and amounts")
    print("9. summary_metrics.xlsx - Overall summary statistics")
    print("10. financial_overview.xlsx - Combined financial metrics")
    print("11. filtered_raw_data.xlsx - Complete filtered datasets (claims, ledger, premium, debit)")

if __name__ == "__main__":
    generate_report()