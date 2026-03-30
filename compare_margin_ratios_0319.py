import pandas as pd
import numpy as np

def compare_margin_ratios(excel_file, ref_sheet, output_file):
    """
    Compare margin ratios across different sheets in an Excel file.

    Args:
        excel_file (str): Path to the Excel file.
        ref_sheet (str): The sheet to use as a reference for the stock list.
        output_file (str): Path to save the comparison report.
    """
    xls = pd.ExcelFile(excel_file)
    sheet_names = sorted(xls.sheet_names)

    # Load all sheets into a dictionary of dataframes
    all_sheets = {sheet: pd.read_excel(xls, sheet_name=sheet) for sheet in sheet_names}

    # Get the list of stocks from the reference sheet
    if ref_sheet not in all_sheets:
        print(f"Error: Reference sheet '{ref_sheet}' not found in the excel file.")
        return
        
    ref_stocks = all_sheets[ref_sheet]['code'].unique()

    # Define columns to compare
    columns_to_compare = [
        'is_long_permit', 'is_short_permit', 'alert_long_ratio',
        'alert_short_ratio', 'im_long_ratio', 'mcm_long_ratio',
        'mm_long_ratio', 'im_short_ratio', 'mcm_short_ratio', 'mm_short_ratio'
    ]

    # Store comparison results
    comparison_results = []

    # Iterate through sheets and compare
    for i in range(len(sheet_names) - 1):
        sheet1_name = sheet_names[i]
        sheet2_name = sheet_names[i+1]
        
        df1 = all_sheets[sheet1_name].drop_duplicates(subset='code', keep='first')
        df2 = all_sheets[sheet2_name].drop_duplicates(subset='code', keep='first')

        # Set 'code' as index for easy lookup
        df1 = df1.set_index('code')
        df2 = df2.set_index('code')

        for stock in ref_stocks:
            if stock in df1.index and stock in df2.index:
                row1 = df1.loc[stock]
                row2 = df2.loc[stock]
                
                # Check for any modifications
                # To handle NaN values properly, we fill them with a value that is not in the data
                # and then compare.
                if not row1[columns_to_compare].fillna(-9999).equals(row2[columns_to_compare].fillna(-9999)):
                    changes = {
                        'code': stock,
                        'sheet_from': sheet1_name,
                        'sheet_to': sheet2_name,
                    }
                    for col in columns_to_compare:
                        val1 = row1[col]
                        val2 = row2[col]
                        # Treat numpy.nan as equal
                        if val1 is not val2 and not (pd.isna(val1) and pd.isna(val2)) and val1 != val2 :
                            changes[f'{col}_from'] = val1
                            changes[f'{col}_to'] = val2
                    comparison_results.append(changes)

    # Create a DataFrame from the results and save to CSV
    if comparison_results:
        report_df = pd.DataFrame(comparison_results)
        report_df.to_csv(output_file, index=False)
        print(f"Comparison report saved to {output_file}")
    else:
        print("No changes found.")

if __name__ == '__main__':
    compare_margin_ratios(
        excel_file='futu_comparison_0319.xlsx',
        ref_sheet='0120',
        output_file='comparison_report.csv'
    )
