import calendar
import pandas as pd
from datetime import datetime
import streamlit as st
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
import io
import base64

def _generate_contract_months(start_month, end_month=None):
    """
    Helper function to generate months between start and end month.
    
    Args:
        start_month (str): The starting month name (e.g., 'January', 'February', etc.)
        end_month (str, optional): The ending month name. If None, generates 12 months from start.
    
    Returns:
        list: A list of months in sequence
    """
    # Dictionary to map month names to their numbers
    month_to_num = {month: index for index, month in enumerate(calendar.month_name) if month}
    
    # Get the starting month number
    start_month_num = month_to_num[start_month]
    
    # If end_month is provided, calculate number of months needed
    if end_month:
        end_month_num = month_to_num[end_month]
        if end_month_num < start_month_num:
            end_month_num += 12
        num_months = end_month_num - start_month_num + 1
    else:
        num_months = 12
    
    # Generate the list of months
    contract_months = []
    for i in range(num_months):
        month_num = (start_month_num + i) % 12
        if month_num == 0:  # Handle December (12)
            month_num = 12
        contract_months.append(calendar.month_name[month_num])
    
    return contract_months

def _get_installment_months(months, pattern):
    """
    Split months into installments based on pattern.
    
    Args:
        months (list): List of month names
        pattern (str): Payment pattern ('monthly', 'quarterly', 'triannually', 'biannually', 'annually')
    
    Returns:
        dict: Dictionary with installment numbers as keys and lists of months as values
    """
    pattern_months = {
        'monthly': 1,
        'quarterly': 3,
        'triannually': 4,
        'biannually': 6,
        'annually': 12
    }
    
    months_per_installment = pattern_months.get(pattern.lower(), 1)
    installments = {}
    
    for i in range(0, len(months), months_per_installment):
        installment_num = (i // months_per_installment) + 1
        installments[installment_num] = months[i:i + months_per_installment]
    
    return installments

def get_company_plans_months(df, company_name, pattern_df):
    """
    Get contract months for all plans of a specific company.
    
    Args:
        df (pandas.DataFrame): DataFrame containing company contract information
        company_name (str): Name of the company to look up
        pattern_df (pandas.DataFrame): DataFrame containing company payment patterns
    
    Returns:
        dict: Dictionary with plan names as keys and their month DataFrames as values
    """
    # Convert dates to datetime if they aren't already
    df['startdate'] = pd.to_datetime(df['startdate'])
    df['enddate'] = pd.to_datetime(df['enddate'])
    
    # Get all plans for the company
    company_plans = df[df['groupname'] == company_name]
    
    # Get company's contract period
    company_data = company_plans.iloc[0]
    company_start_month = company_data['startdate'].strftime('%B')
    company_end_month = company_data['enddate'].strftime('%B')
    company_start_date = company_data['startdate']
    company_end_date = company_data['enddate']
    
    # Get company's payment pattern
    company_pattern = pattern_df[pattern_df['groupname'] == company_name]['PATTERN'].iloc[0]
    
    # Generate all months for the company's contract period
    all_contract_months = _generate_contract_months(company_start_month, company_end_month)
    
    # Get installment periods
    installment_months = _get_installment_months(all_contract_months, company_pattern)
    
    # Create cash received DataFrame
    # Filter GLT for cash transactions for this company within contract period
    company_cash = GLT[
        (GLT['CompanyName'] == company_name) & 
        (GLT['DESCRIPTION'].isin(Cash_keywords)) &
        (GLT['GLDate'] >= company_start_date) &
        (GLT['GLDate'] <= company_end_date)
    ].copy()
    
    # Create a DataFrame with all months in the contract period
    cash_received_df = pd.DataFrame({
        'Month Number': range(1, len(all_contract_months) + 1),
        'Month Name': all_contract_months
    })
    
    # Extract month from GLDate and group by month to get cash received
    company_cash['Month'] = company_cash['GLDate'].dt.month
    monthly_cash = company_cash.groupby('Month')['GLAmount'].sum().reset_index()
    
    # Create a mapping of month numbers to cash received
    cash_map = dict(zip(monthly_cash['Month'], monthly_cash['GLAmount']))
    
    # Add cash received to the DataFrame
    cash_received_df['Cash Received'] = cash_received_df['Month Number'].apply(
        lambda x: cash_map.get(x, 0)
    )
    
    # Dictionary to store results for each plan
    plan_months = {}
    
    # Process each plan
    for _, plan in company_plans.iterrows():
        plan_name = plan['planname']
        
        # Check if plan has a delayed start (contains "-")
        if "-" in plan_name:
            try:
                # Extract the month number from plan name (e.g., "Basic-3" -> 3)
                start_month_num = int(plan_name.split("-")[-1])
                # Convert number to month name (1=January, 2=February, etc.)
                start_month = calendar.month_name[start_month_num]
            except (ValueError, IndexError):
                # If parsing fails, use company start month
                start_month = company_start_month
        else:
            # If no "-" in plan name, use company start month
            start_month = company_start_month
        
        # Generate months for this plan
        contract_months = _generate_contract_months(start_month, company_end_month)
        
        # Calculate number of months for this plan
        num_months = len(contract_months)
        
        # Calculate monthly premium
        individual_price_per_month = plan['individualprice'] / num_months
        family_price_per_month = plan['familyprice'] / num_months
        
        individual_monthly_total = individual_price_per_month * plan['countofindividual']
        family_monthly_total = family_price_per_month * plan['countoffamily']
        
        monthly_premium = individual_monthly_total + family_monthly_total
        
        # Create DataFrame for this plan
        months_df = pd.DataFrame({
            'Month Number': range(1, len(contract_months) + 1),
            'Month Name': contract_months,
            'Month Premium': [monthly_premium] * len(contract_months)  # Same premium for all months
        })
        
        # Add installment information
        months_df['Installment'] = months_df['Month Name'].apply(
            lambda x: next((inst_num for inst_num, months in installment_months.items() if x in months), None)
        )
        
        # Create separate DataFrames for each installment
        installment_dfs = {}
        for inst_num, inst_months in installment_months.items():
            inst_df = months_df[months_df['Month Name'].isin(inst_months)].copy()
            if not inst_df.empty:
                installment_dfs[f'Installment {inst_num}'] = inst_df
        
        # Store both the full months DataFrame and the installment DataFrames
        plan_months[plan_name] = {
            'all_months': months_df,
            'installments': installment_dfs
        }
    
    # Add cash received DataFrame to the return value
    plan_months['cash_received'] = cash_received_df
    
    return plan_months

def generate_pdf(company_name, inst_df, inst_name, month_range, total_premium, total_cash):
    """
    Generate a PDF invoice for a specific installment.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    
    # Add company logo
    logo_path = "Clearline.png"
    img = Image(logo_path, width=2*inch, height=1*inch)
    elements.append(img)
    elements.append(Spacer(1, 0.5*inch))
    
    # Add company name and installment info
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=30
    )
    
    elements.append(Paragraph(f"Invoice for {company_name}", title_style))
    elements.append(Paragraph(f"{inst_name} ({month_range})", styles['Heading2']))
    elements.append(Spacer(1, 0.25*inch))
    
    # Convert DataFrame to list of lists for the table
    table_data = [inst_df.columns.tolist()] + inst_df.values.tolist()
    
    # Add totals row
    table_data.append(['', '', '', '', '', f'Total Premium: ₦{total_premium:,.2f}'])
    table_data.append(['', '', '', '', '', f'Total Cash Received: ₦{total_cash:,.2f}'])
    
    # Create table
    table = Table(table_data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ALIGN', (-1, -1), (-1, -1), 'RIGHT'),
    ]))
    
    elements.append(table)
    elements.append(Spacer(1, 0.5*inch))
    
    # Add company details
    company_details = [
        "clearline international limited",
        "polaris bank",
        "123456890"
    ]
    
    for detail in company_details:
        elements.append(Paragraph(detail, styles['Normal']))
    
    # Build PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer

def display_invoice(company_name, df, pattern_df):
    """
    Display invoice information for a selected company.
    
    Args:
        company_name (str): Name of the company to display
        df (pandas.DataFrame): DataFrame containing company contract information
        pattern_df (pandas.DataFrame): DataFrame containing company payment patterns
    """
    # Get company data
    plan_months = get_company_plans_months(df, company_name, pattern_df)
    
    # Get company's contract period
    company_plans = df[df['groupname'] == company_name]
    company_data = company_plans.iloc[0]
    start_date = company_data['startdate'].strftime('%B %Y')
    end_date = company_data['enddate'].strftime('%B %Y')
    
    # Display header
    st.header(f"Invoice for {company_name}")
    st.subheader(f"Contract Period: {start_date} to {end_date}")
    
    # Get all installments
    all_installments = set()
    for plan_name, plan_data in plan_months.items():
        if plan_name != 'cash_received':
            for inst_name in plan_data['installments'].keys():
                all_installments.add(inst_name)
    
    # Sort installments
    all_installments = sorted(all_installments, key=lambda x: int(x.split()[-1]))
    
    # Get installment months mapping
    installment_months = _get_installment_months(
        _generate_contract_months(company_data['startdate'].strftime('%B'), 
                               company_data['enddate'].strftime('%B')),
        pattern_df[pattern_df['groupname'] == company_name]['PATTERN'].iloc[0]
    )
    
    # Display each installment
    for inst_name in all_installments:
        inst_num = int(inst_name.split()[-1])
        inst_months = installment_months[inst_num]
        month_range = f"{inst_months[0]} to {inst_months[-1]}"
        
        st.subheader(f"{inst_name} ({month_range})")
        
        # Create a list to store installment data
        installment_data = []
        
        # Get plans in this installment
        for plan_name, plan_data in plan_months.items():
            if plan_name != 'cash_received' and inst_name in plan_data['installments']:
                plan_info = company_plans[company_plans['planname'] == plan_name].iloc[0]
                inst_df = plan_data['installments'][inst_name]
                
                installment_data.append({
                    'Plan': plan_name,
                    'Individual Count': plan_info['countofindividual'],
                    'Family Count': plan_info['countoffamily'],
                    'Individual Price': plan_info['individualprice'],
                    'Family Price': plan_info['familyprice'],
                    'Total Premium': inst_df['Month Premium'].sum()
                })
        
        # Create DataFrame for this installment
        if installment_data:
            inst_df = pd.DataFrame(installment_data)
            
            # Calculate totals
            total_premium = inst_df['Total Premium'].sum()
            
            # Get cash received for this installment period
            cash_df = plan_months['cash_received']
            total_cash = cash_df[cash_df['Month Name'].isin(inst_months)]['Cash Received'].sum()
            
            # Display installment table
            st.dataframe(inst_df, use_container_width=True)
            
            # Display totals
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Premium", f"₦{total_premium:,.2f}")
            with col2:
                st.metric("Total Cash Received", f"₦{total_cash:,.2f}")
            
            # Add print button
            if st.button(f"Print {inst_name}", key=f"print_{inst_name}"):
                # Generate PDF
                pdf_buffer = generate_pdf(
                    company_name,
                    inst_df,
                    inst_name,
                    month_range,
                    total_premium,
                    total_cash
                )
                
                # Create download button for PDF
                st.download_button(
                    label="Download PDF",
                    data=pdf_buffer,
                    file_name=f"{company_name}_{inst_name}_invoice.pdf",
                    mime="application/pdf"
                )
            
            st.divider()

# Example usage:
if __name__ == "__main__":
    # Get the data from auto_invoice
    G_PLAN_with_dates = pd.read_excel("DATADUMP/output.xlsx")
    
    # Read the pattern file
    pattern_df = pd.read_excel("DATADUMP/pattern.xlsx")

    #Read the 2024 and 2025ledger file
    GL2024 = pd.read_excel("DATADUMP/GL2024.xlsx")
    GL2025 = pd.read_parquet("DATADUMP/MEDICLOUD_FIN_GL.parquet")
    GLSETUP = pd.read_parquet("DATADUMP/MEDICLOUD_FIN_AccSetup.parquet")
    E_ACCT_GROUP = pd.read_parquet("DATADUMP/MEDICLOUD_e_account_group.parquet")

    # Concatenate GL2024 and GL2025 vertically using pandas
    GL_combined = pd.concat([GL2024, GL2025], axis=0)

    # Select only the specified columns from GL_combined
    GL_combined = GL_combined[['acctype', 'AccCode', 'GLDesc', 'GLDate', 'GLAmount', 'code']]

    # Convert amount column to numeric type
    GL_combined['GLAmount'] = pd.to_numeric(GL_combined['GLAmount'])

    # Convert AccCode to string in both DataFrames and remove .0 from GLSETUP
    GL_combined['AccCode'] = GL_combined['AccCode'].astype(str)
    GLSETUP['AccCode'] = GLSETUP['AccCode'].astype(str).str.replace(r'\.0$', '', regex=True)

    # Remove .0 from AccCode if present
    GL_combined['AccCode'] = GL_combined['AccCode'].str.replace(r'\.0$', '', regex=True)

    # Perform a left join to add AccDesc from GLSETUP to GL using AccCode as the key
    GLL = pd.merge(
        GL_combined,
        GLSETUP[['AccCode', 'AccDesc']],
        on='AccCode',
        how='left'
    ).rename(columns={'AccDesc': 'DESCRIPTION'})

    # Convert GLDate to datetime format
    GLL['GLDate'] = pd.to_datetime(GLL['GLDate'])
    
    # Extract month and year from GLDate
    GLL['Month'] = GLL['GLDate'].dt.month
    GLL['Year'] = GLL['GLDate'].dt.year

    # Convert ID_Company and code columns to string type for joining
    E_ACCT_GROUP['ID_Company'] = E_ACCT_GROUP['ID_Company'].astype(str)
    GLL['code'] = GLL['code'].astype(str)

    # Join with E_ACCT_GROUP
    GLT = pd.merge(
        GLL,
        E_ACCT_GROUP[['ID_Company', 'CompanyName']],
        left_on='code',
        right_on='ID_Company',
        how='left'
    )

    #NEGATE AMOUNT
    # First ensure GLAmount is numeric
    GLT['GLAmount'] = pd.to_numeric(GLT['GLAmount'], errors='coerce')

    # Convert to absolute values 
    GLT['GLAmount'] = GLT['GLAmount'].abs()

    Cash_keywords = ["POLARIS BANK 1", "KEYSTONE BANK", "MICROCRED MFB", "ASTRAPOLARIS MFB", "ALERT MICRO BANK", "MONEYFIELD MFB", "KAYVEE  MFB"]
    
    # Convert to pandas DataFrame if it's a polars DataFrame
    if hasattr(G_PLAN_with_dates, 'to_pandas'):
        df = G_PLAN_with_dates.to_pandas()
    else:
        df = G_PLAN_with_dates
    
    # Create Streamlit interface
    st.title("Company Invoice Generator")
    
    # Get unique company names
    company_names = sorted(df['groupname'].unique())
    
    # Create company selection dropdown
    selected_company = st.selectbox(
        "Select Company",
        company_names,
        index=None,
        placeholder="Choose a company..."
    )
    
    if selected_company:
        display_invoice(selected_company, df, pattern_df)
