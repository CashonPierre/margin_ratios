import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os

def generate_var_boxplots():
    # 1. Load the Margin Analysis file
    margin_file = 'long_margin_analysis.csv'
    if not os.path.exists(margin_file):
        print(f"Error: {margin_file} not found. Ensure it's in the current directory.")
        return

    margin_df = pd.read_csv(margin_file)
    
    # 2. Bucket the Initial Margin (IM_long)
    # Target: 40-49, 50-59, ..., 90-99
    def get_bucket(im):
        if pd.isna(im) or im < 40: return None
        low = int((im // 10) * 10)
        high = low + 9
        return f"{low}-{high}"

    margin_df['IM_bucket'] = margin_df['IM_long'].apply(get_bucket)
    
    # Create an ordered category to ensure plots go from 40-49 to 90-99
    bucket_order = [f"{i}-{i+9}" for i in range(40, 100, 10)]
    margin_df['IM_bucket'] = pd.Categorical(
        margin_df['IM_bucket'], 
        categories=bucket_order, 
        ordered=True
    )

    # 3. Find all VaR result files in the current directory
    var_files = glob.glob('var_results/VaR_*.csv')
    
    if not var_files:
        print("No VaR files found matching 'VaR_*.csv'.")
        return

    print(f"Found {len(var_files)} files. Generating plots...")

    # 4. Iterate and Plot
    sns.set_style("whitegrid")
    
    for file_path in var_files:
        try:
            # Load VaR data
            var_df = pd.read_csv(file_path)
            
            # Merge on ticker symbols
            # Key: 'Symbol' (VaR file) -> 'ticker_LB' (Margin file)
            df = pd.merge(
                var_df, 
                margin_df[['ticker_LB', 'IM_bucket']], 
                left_on='Symbol', 
                right_on='ticker_LB'
            )
            
            if df.empty:
                print(f"Skipping {file_path}: No matching tickers found.")
                continue

            # Create Plot
            plt.figure(figsize=(12, 7))
            
            # Boxplot of VaR_% grouped by IM_bucket
            ax = sns.boxplot(
                data=df, 
                x='IM_bucket', 
                y='VaR_%', 
                palette='viridis',
                showfliers=True # Set to False if you want to hide extreme outliers
            )
            
            # Clean up title by removing timestamp
            title_clean = os.path.basename(file_path).replace('.csv', '').split('_2026')[0]
            
            plt.title(f"VaR Distribution by Initial Margin Bucket\nSource: {title_clean}", fontsize=14)
            plt.xlabel("Initial Margin Bucket (%)", fontsize=12)
            plt.ylabel("Value at Risk (%)", fontsize=12)
            
            # Add sample counts (n=) above each box
            counts = df.groupby('IM_bucket')['VaR_%'].count()
            for i, bucket in enumerate(bucket_order):
                if bucket in counts and counts[bucket] > 0:
                    ax.text(i, ax.get_ylim()[1], f"n={int(counts[bucket])}", 
                            ha='center', va='bottom', fontsize=9, color='blue')

            # Save the result
            output_name = f"box_plots/plot_{title_clean}.png"
            plt.tight_layout()
            plt.savefig(output_name)
            plt.close()
            print(f"Successfully saved: {output_name}")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    generate_var_boxplots()