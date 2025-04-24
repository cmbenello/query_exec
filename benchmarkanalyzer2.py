#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
import os

def create_two_main_graphs(csv_file, output_dir):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the CSV file
    print(f"Reading data from {csv_file}")
    df = pd.read_csv(csv_file)
    
    # Group by the relevant parameters and calculate means
    groupby_cols = ['tuple_mode', 'threads', 'target_runcount', 'num_tuples']
    groupby_cols = [col for col in groupby_cols if col in df.columns]
    
    # Choose performance metric (merge_duration_s or parallel_merge_duration_s)
    # Using parallel_merge_duration_s as the main metric of interest
    metric = 'parallel_merge_duration_s' if 'parallel_merge_duration_s' in df.columns else 'merge_duration_s'
    
    # Group and calculate means
    mean_df = df.groupby(groupby_cols)[metric].mean().reset_index()
    print(f"Created aggregated data with {len(mean_df)} unique parameter combinations")
    
    # Get all unique target run counts
    all_run_counts = sorted(mean_df['target_runcount'].unique())
    print(f"Available target run counts: {all_run_counts}")
    
    # GRAPH 1: Variable N (N scales with thread count)
    variable_df = mean_df[mean_df['tuple_mode'] == 'variable']
    if not variable_df.empty:
        create_scaling_plot(
            variable_df, 
            metric, 
            all_run_counts,
            'Time vs Threads (N scales with thread count)',
            os.path.join(output_dir, 'variable_tuples_scaling.png')
        )
    
    # GRAPH 2: Fixed N (constant total tuples)
    fixed_df = mean_df[mean_df['tuple_mode'] == 'fixed']
    if not fixed_df.empty:
        create_scaling_plot(
            fixed_df, 
            metric, 
            all_run_counts,
            'Time vs Threads (Fixed N = SF10 = 60M tuples)',
            os.path.join(output_dir, 'fixed_tuples_scaling.png')
        )
    
    print(f"Created 2 main graphs in {output_dir}")

def create_scaling_plot(df, metric, target_run_counts, title, output_path):
    """Create a thread scaling plot with multiple curves for different run counts."""
    plt.figure(figsize=(12, 8))
    
    # Set line styles and markers for different run counts
    # Using a colormap to generate distinct colors for many run counts
    import matplotlib.cm as cm
    
    # Get a colormap that works well with many categories
    cmap = cm.get_cmap('tab20', len(target_run_counts))
    markers = ['o', 's', '^', 'D', 'v', 'p', '*', 'h', '+', 'x']
    
    # Add a curve for each target run count
    for i, run_count in enumerate(target_run_counts):
        color = cmap(i)
        marker = markers[i % len(markers)]
        
        # Filter data for this run count
        run_data = df[df['target_runcount'] == run_count]
        
        if len(run_data) < 2:
            continue
            
        # Sort by thread count
        run_data = run_data.sort_values('threads')
        
        # Plot this curve
        plt.plot(
            run_data['threads'], 
            run_data[metric], 
            marker=marker,
            linestyle='-',
            linewidth=2,
            markersize=8,
            label=f'k={run_count}',
            color=color
        )
    
    # Configure the plot
    plt.xlabel('Thread Count', fontsize=14)
    plt.ylabel('Time (seconds)', fontsize=14)
    plt.title(title, fontsize=16)
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Format x-axis with specific thread values
    thread_values = sorted(df['threads'].unique())
    plt.xticks(thread_values)
    
    # Create legend with two columns for better readability with many items
    plt.legend(fontsize=12, ncol=2, loc='best', framealpha=0.8)
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create two main scaling graphs')
    parser.add_argument('csv_file', help='Path to the CSV file containing benchmark results')
    parser.add_argument('--output-dir', default='main_graphs', 
                        help='Directory to save generated graphs (default: main_graphs)')
    
    args = parser.parse_args()
    create_two_main_graphs(args.csv_file, args.output_dir)