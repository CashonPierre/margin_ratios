
import pandas as pd
import argparse
import glob
import os
from datetime import datetime




def find_latest_files(pattern, num_files):
    """Finds the most recent files matching a glob pattern."""
    files = glob.glob(pattern)
    if not files:
        print(f"No files found matching pattern: {pattern}")
        return []
    # Sort by modification time, newest first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[:num_files]

def compare_margin_files(file_paths, output_file=None):
    if len(file_paths) < 2:
        print("Error: At least two files are required for comparison.")
        return

    # 1. Sort files chronologically (Oldest to Newest) 
    # Since your find_latest_files uses reverse=True, we reverse it back here.
    file_paths.sort(key=os.path.getmtime) 

    print(f"Comparing sequence:\n" + " -> ".join(os.path.basename(fp) for fp in file_paths))
    
    all_diffs = []
    
    # Load all DataFrames into a dictionary first
    try:
        dfs = {os.path.basename(fp): pd.read_csv(fp) for fp in file_paths}
    except Exception as e:
        print(f"Error reading files: {e}")
        return

    print("\n--- Day-to-Day Comparison Report ---")
    
    # 2. Loop through the list comparing pairs (i and i+1)
    for i in range(len(file_paths) - 1):
        base_filename = os.path.basename(file_paths[i])
        compare_filename = os.path.basename(file_paths[i+1])
        
        print(f"\n[Step {i+1}] Comparing '{base_filename}' -> to -> '{compare_filename}':")
        
        # Prepare DataFrames for this specific pair
        df_old = dfs[base_filename].copy().set_index('code')
        df_new = dfs[compare_filename].copy().set_index('code')
        
        # --- Logic: Added/Removed ---
        added_codes = df_new.index.difference(df_old.index)
        removed_codes = df_old.index.difference(df_new.index)

        for code in added_codes:
            print(f"  [+] ADDED: {code}")
            all_diffs.append({'base_file': base_filename, 'compare_file': compare_filename, 
                              'code': code, 'change_type': 'ADDED'})

        for code in removed_codes:
            print(f"  [-] REMOVED: {code}")
            all_diffs.append({'base_file': base_filename, 'compare_file': compare_filename, 
                              'code': code, 'change_type': 'REMOVED'})

        # --- Logic: Modified ---
        common_codes = df_old.index.intersection(df_new.index)
        for code in common_codes:
            row_old = df_old.loc[code]
            row_new = df_new.loc[code]
            diff_mask = row_old.ne(row_new)
            
            if diff_mask.any():
                changed_cols = diff_mask[diff_mask].index.tolist()
                for col in changed_cols:
                    print(f"  [*] MODIFIED '{code}': {col} | {row_old[col]} -> {row_new[col]}")
                    all_diffs.append({
                        'base_file': base_filename, 'compare_file': compare_filename,
                        'code': code, 'change_type': 'MODIFIED', 'column': col,
                        'old_value': row_old[col], 'new_value': row_new[col]
                    })
    
    # Save logic remains the same
    if output_file and all_diffs:
        pd.DataFrame(all_diffs).to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\nFull report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Compare Futu margin ratio CSV files and optionally save the report.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    default_name = f"comparison_report_{timestamp}.csv"

    parser.add_argument(
        'files',
        nargs='*',
        help="Paths to the CSV files to compare. If empty, the script will automatically find the 5 most recent 'futu_margin_ratios_all_*.csv' files."
    )
    parser.add_argument(
        '--output',
        type=str,
        default=default_name,
        help=f"Path to save the report. Defaults to '{default_name}'"
    )
    args = parser.parse_args()

    if args.files:
        file_paths_to_compare = args.files
    else:
        print("No files provided. Searching for the 5 most recent margin ratio files...")
        file_paths_to_compare = find_latest_files('futu_margin_ratios_all_*.csv', 5)
        if not file_paths_to_compare:
            print("Could not find any files to compare. Exiting.")
            return

    compare_margin_files(file_paths_to_compare, args.output)

if __name__ == "__main__":
    main()
