import subprocess
import csv
import os
import math
import sys
from datetime import datetime
import git
import argparse
import matplotlib.pyplot as plt
import pandas as pd

def get_git_hash():
    """Get the current git commit hash."""
    repo = git.Repo(search_parent_directories=True)
    return repo.head.object.hexsha[:8]

def calculate_num_tuples(data_source, sf):
    """Calculate number of tuples based on data source and scale factor."""
    if data_source == "TPCH":
        return 6005720 * sf
    elif data_source == "GENSORT":
        return sf
    else:
        raise ValueError(f"Unknown data source: {data_source}")

def get_bp_dir(data_source, sf, distribution="pointers"):
    """Generate buffer pool directory name."""
    if data_source == "TPCH":
        return f"bp-dir-tpch-sf-{sf}"
    elif data_source == "GENSORT":
        return f"bp-dir-gensort-sf-{sf}-{distribution}"
    else:
        raise ValueError(f"Unknown data source: {data_source}")

def parse_benchmark_output(output):
    """Parse benchmark output for times and stats, handling both output formats."""
    generation_time = None
    merge_duration = None
    stats_after = None
    merge_steps = None
    fan_ins = []
    total_records = None
    
    for line in output.splitlines():
        # Common parsing for both formats
        if "generation duration" in line:
            time_str = line.split("generation duration ")[1].strip().rstrip('s')
            generation_time = convert_time_to_seconds(time_str)
        elif "merge duration" in line:
            time_str = line.split("merge duration ")[1].strip().rstrip('s')
            merge_duration = convert_time_to_seconds(time_str)
        elif "stats after" in line:
            stats_after = line.strip("stats after ").strip("()").split(", ")
        
        # Parse the hierarchical merge output format
        elif "total merge steps = " in line:
            merge_steps = int(line.split("total merge steps = ")[1].strip())
        elif "Fan-ins per step: " in line:
            fan_ins_str = line.split("Fan-ins per step: ")[1].strip("[]")
            if fan_ins_str:
                fan_ins = [int(x.strip()) for x in fan_ins_str.split(",")]
        
        # Look for the total records count in both formats
        elif "Finished parallel merge in" in line and "total" in line:
            # Format: "Finished parallel merge in X.XXs (total NNNNNNN records)"
            total_str = line.split("total ")[1].split(" ")[0]
            try:
                total_records = int(total_str.replace(",", ""))
            except ValueError:
                pass
        
        # For the hierarchical merge stats at end
        elif "Total records:" in line:
            try:
                total_records = int(line.split("Total records:")[1].strip().split(" ")[0])
            except (ValueError, IndexError):
                pass
    
    # If merge_steps wasn't explicitly found but we have fan_ins, calculate it
    if merge_steps is None and fan_ins:
        merge_steps = len(fan_ins)
    
    # If stats_after is None and we have records count, create a minimal stats 
    # record with just the total records
    if stats_after is None and total_records is not None:
        stats_after = [str(total_records)]
    
    return generation_time, merge_duration, stats_after, merge_steps, fan_ins, total_records

def get_default_configs():
    """Return default configuration settings."""
    return {
        "TPCH": {
            "data_source": "TPCH",
            "machine": "Roscoe",
            "quantile_method": "Parallel_BSS",
            "memory_type": "mnt/nvme",
            "query_options": [100],
            "sf_options": [10],
            # "working_mem_options": [20, 50, 100, 200, 600, 1135],
            "working_mem_options": [60000],
            "bp_sizes": [2500000],
            "num_threads_options": [1,2,4,8,16,20,30,40],
            # "num_threads_options": [2, 4, 10, 12, 16]
            # "num_threads_options": [8, 16]
        },
        "GENSORT": {
            "data_source": "GENSORT",
            "machine": "Roscoe",
            "quantile_method": "Parallel_BSS",
            "memory_type": "tank/local",
            "query_options": [],
            "sf_options": [10576511],
            "working_mem_options": [450],
            "bp_sizes": [150000],
            "num_threads_options": [1, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        }
    }

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run benchmarks with optional parameters')
    parser.add_argument('-d', '--data-source', choices=['TPCH', 'GENSORT'],
                        help='Data source (TPCH or GENSORT)')
    parser.add_argument('-s', '--scale-factor', type=int,
                        help='Scale factor')
    parser.add_argument('-q', '--query', type=int,
                        help='Query number')
    parser.add_argument('-t', '--threads', type=int,
                        help='Number of threads')
    parser.add_argument('-w', '--working-mem', type=int,
                        help='Working memory size')
    parser.add_argument('-b', '--buffer-pool', type=int,
                        help='Buffer pool size')
    parser.add_argument('-m', '--machine',
                        help='Machine name')
    parser.add_argument('-i', '--iterations', type=int, default=1,
                        help='Number of iterations to run each benchmark configuration')
    parser.add_argument('--plot', action='store_true',
                        help='Generate performance plots')
    parser.add_argument('--auto', action='store_true',
                        help='Run in automated mode with default configurations')
    return parser.parse_args()

def plot_thread_scaling(csv_path):
    """Generate performance plots from benchmark results."""
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Create the plot with specific figure size
    plt.figure(figsize=(12, 8))
    
    # Create the plot
    plt.plot(df['Num Threads (Run Merge)'], df['Merge Time'], 'bo-', linewidth=2, markersize=8)
    
    # Customize the plot
    plt.title(f'Thread Scaling Analysis\n{df["Data Source"].iloc[0]} Query {df["Query Num"].iloc[0]} SF={df["SF"].iloc[0]}',
              pad=20)
    
    # Set x-axis
    plt.xlabel('Number of Threads')
    threads = df['Num Threads (Run Merge)'].unique()
    plt.xticks(threads, threads)  # Just use the actual thread numbers
    
    # Set y-axis
    plt.ylabel('Merge Time (seconds)')
    ymin = 0
    ymax = df['Merge Time'].max() * 1.2
    plt.ylim(ymin, ymax)
    
    # Add grid
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Add point labels
    for x, y in zip(df['Num Threads (Run Merge)'], df['Merge Time']):
        plt.annotate(f'{y:.2f}s', 
                    (x, y),
                    textcoords="offset points",
                    xytext=(0, 10),
                    ha='center',
                    fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = csv_path.rsplit('.', 1)[0] + '_thread_scaling.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Plot saved as {plot_path}")

def convert_time_to_seconds(time_str):
    """Convert time string to seconds, handling both seconds and milliseconds formats."""
    if isinstance(time_str, (int, float)):
        return float(time_str)
    
    # If string ends with 'm', it's in milliseconds
    if str(time_str).endswith('m'):
        return float(time_str.rstrip('m')) / 1000
    # Otherwise it's already in seconds
    return float(time_str)

def create_run_configs(args):
    """Create run configurations based on args and defaults."""
    default_configs = get_default_configs()
    
    # If no data source specified, run all default configurations
    if not args.data_source:
        return default_configs
    
    # Start with the default config for the specified data source
    config = default_configs[args.data_source].copy()
    
    # Override with any specified command line arguments
    if args.scale_factor is not None:
        config["sf_options"] = [args.scale_factor]
    if args.query is not None:
        config["query_options"] = [args.query]
    if args.threads is not None:
        config["num_threads_options"] = [args.threads]
    if args.working_mem is not None:
        config["working_mem_options"] = [args.working_mem]
    if args.buffer_pool is not None:
        config["bp_sizes"] = [args.buffer_pool]
    if args.machine is not None:
        config["machine"] = args.machine
    
    return {args.data_source: config}

def run_benchmark(config, is_manual=False):
    """Run a single benchmark with given configuration."""
    # Set up environment variables
    env = os.environ.copy()
    env["NUM_THREADS"] = str(config["num_threads"])
    env["WORKING_MEM"] = str(config["working_mem"])
    env["DATA_SOURCE"] = config["data_source"]
    env["SF"] = str(config["sf"])
    env["QUERY_NUM"] = str(config["query"])
    env["NUM_TUPLES"] = str(calculate_num_tuples(config["data_source"], config["sf"]))

    bp_dir = get_bp_dir(config["data_source"], config["sf"])
    
    # Clean up previous runs
    subprocess.run(f"rm -rf {bp_dir}/0/??*", shell=True)

    # Run the benchmark script
    cmd = [
        "./run_benchmark.sh",
        config["data_source"],
        str(config["bp_size"]),
        str(config["num_threads"]),
        str(config["working_mem"]),
        str(config["query"]),
        str(config["sf"])
    ]

    if is_manual:
        # For manual runs, just execute and let output go to terminal
        print("Running command:", " ".join(cmd))
        result = subprocess.run(cmd, env=env)
        return result.returncode == 0
    else:
        # For automated runs, capture output for parsing
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        return result

def calculate_throughput(num_records, generation_time, merge_duration, num_threads):
    """Calculate throughput in GB/s based on record count, time, and threads."""
    # Assume each record is 100 bytes
    record_size_bytes = 100
    total_data_gb = (num_records * record_size_bytes) / (1024 * 1024 * 1024)
    
    # Calculate total processing time
    total_time = generation_time + merge_duration
    
    # Calculate throughput metrics
    if total_time > 0:
        throughput_gb_s = total_data_gb / total_time
        throughput_per_thread = throughput_gb_s / num_threads
    else:
        throughput_gb_s = 0
        throughput_per_thread = 0
        
    return throughput_gb_s, throughput_per_thread

def run_single_benchmark_iteration(config, output_file, iteration, machine):
    """Run a single benchmark iteration and append the results to the output file."""
    # Run the benchmark
    result = run_benchmark(config, is_manual=False)
    
    if result.returncode != 0:
        print(f"Benchmark failed in iteration {iteration}!")
        return False
        
    # Parse the benchmark output
    generation_time, merge_duration, stats_after, merge_steps, fan_ins, total_records = parse_benchmark_output(result.stdout)
    
    if generation_time is None or merge_duration is None:
        print(f"Failed to parse benchmark output in iteration {iteration}")
        return False
        
    # Calculate additional metrics
    num_merge_steps = merge_steps if merge_steps is not None else 0
    
    # Calculate throughput metrics
    num_records = total_records if total_records is not None else (
        calculate_num_tuples(config["data_source"], config["sf"])
    )
    throughput_gb_s, throughput_per_thread = calculate_throughput(
        num_records, generation_time, merge_duration, config["num_threads"]
    )
    
    # Set default values for when stats_after is not available
    new_pages = stats_after[0] if stats_after and len(stats_after) > 0 else "N/A"
    pages_read = stats_after[1] if stats_after and len(stats_after) > 1 else "N/A"
    pages_written = stats_after[2] if stats_after and len(stats_after) > 2 else "N/A"
    
    # Append results to CSV
    with open(output_file, mode="a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            machine, config["data_source"], "Parallel_BSS", "tank/local",
            config["query"], config["sf"], config["bp_size"],
            config["working_mem"], config["num_threads"], iteration,
            generation_time, merge_duration, num_merge_steps,
            new_pages, pages_read, pages_written,
            f"{throughput_gb_s:.4f}", f"{throughput_per_thread:.4f}"
        ])
    
    return True

def run_manual_benchmark(config, args):
    """Run a manual benchmark for a specific configuration."""
    # Get the number of iterations
    iterations = args.iterations if args.iterations > 0 else 1
    
    # Create output directory and CSV
    git_hash = get_git_hash()
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = f"benchmark_results/{git_hash}/{config['data_source']}_QID-{config['query']}_SF-{config['sf']}_TIME-{current_time}.csv"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Create CSV and write header
    with open(output_csv, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Machine", "Data Source", "Quantile Method", "Memory Type", "Query Num", "SF", "Bp_Size",
            "Working mem size", "Num Threads (Run Merge)", "Iteration",
            "Generation Time", "Merge Time", "Num Merge steps", 
            "New pages", "Pages read", "Pages written",
            "Throughput (GB/s)", "Throughput per Thread (GB/s)"
        ])
    
    # Run the benchmark for the specified number of iterations
    print(f"Running benchmark with {iterations} iterations...")
    for iteration in range(1, iterations + 1):
        print(f"\nIteration {iteration}/{iterations}:")
        run_single_benchmark_iteration(config, output_csv, iteration, args.machine or "Lincoln")
    
    print(f"Benchmark results saved to {output_csv}")
    
    # Generate plot if requested
    if args.plot:
        plot_thread_scaling(output_csv)
    
    return True

def run_automated_benchmarks(configs, iterations=1, plot=False):
    """Run automated benchmarks for all configurations with multiple iterations."""
    git_hash = get_git_hash()
    
    # Create benchmark_results directory if it doesn't exist
    os.makedirs("benchmark_results", exist_ok=True)
    os.makedirs(f"benchmark_results/{git_hash}", exist_ok=True)

    # Process each configuration
    for data_source, base_config in configs.items():
        for query in base_config["query_options"]:
            for sf in base_config["sf_options"]:
                # Generate output filename
                current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_csv = f"benchmark_results/{git_hash}/{data_source}_QID-{query}_SF-{sf}_TIME-{current_time}.csv"
                print("writing to ", output_csv)
                
                # Create CSV and write header
                with open(output_csv, mode="w", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        "Machine", "Data Source", "Quantile Method", "Memory Type", "Query Num", "SF", "Bp_Size",
                        "Working mem size", "Num Threads (Run Merge)", "Iteration",
                        "Generation Time", "Merge Time", "Num Merge steps", 
                        "New pages", "Pages read", "Pages written",
                        "Throughput (GB/s)", "Throughput per Thread (GB/s)"
                    ])

                # Iterate through all parameter combinations
                for working_mem in base_config["working_mem_options"]:
                    for bp_size in base_config["bp_sizes"]:
                        for num_threads in base_config["num_threads_options"]:
                            print(f"Running benchmark for {data_source} (Query {query}, SF {sf}) "
                                  f"with BP={bp_size}, Threads={num_threads}, WorkingMem={working_mem}")

                            # Create run configuration
                            run_config = {
                                "bp_size": bp_size,
                                "num_threads": num_threads,
                                "working_mem": working_mem,
                                "data_source": data_source,
                                "query": query,
                                "sf": sf
                            }

                            # Run benchmark for each iteration
                            for iteration in range(1, iterations + 1):
                                print(f"  Iteration {iteration}/{iterations}")
                                success = run_single_benchmark_iteration(
                                    run_config, output_csv, iteration, base_config["machine"]
                                )
                                if not success:
                                    print(f"  Failed to run iteration {iteration}")

                print(f"Benchmark results saved to {output_csv}")
                
                # Generate plot if requested
                if plot:
                    plot_thread_scaling(output_csv)

def main():
    args = parse_args()
    
    # Determine the number of iterations
    iterations = max(1, args.iterations)
    
    # Check if we should run in automated mode with default configurations
    if args.auto or len(sys.argv) == 1:
        print("Running in automated mode with default configurations")
        configs = get_default_configs()
        run_automated_benchmarks(configs, iterations=iterations, plot=args.plot)
        return
    
    # If specific arguments are provided but not --auto, run in manual mode
    if len(sys.argv) > 1:
        # If data source is provided, run with configurations from that data source
        if args.data_source:
            # Get the configurations for the specified data source
            configs = create_run_configs(args)
            data_source = args.data_source
            base_config = configs[data_source]
            
            # Create output directory
            git_hash = get_git_hash()
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_csv = f"benchmark_results/{git_hash}/{data_source}_autorun_TIME-{current_time}.csv"
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            
            # Create CSV and write header
            with open(output_csv, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([
                    "Machine", "Data Source", "Quantile Method", "Memory Type", "Query Num", "SF", "Bp_Size",
                    "Working mem size", "Num Threads (Run Merge)", "Iteration",
                    "Generation Time", "Merge Time", "Num Merge steps", 
                    "New pages", "Pages read", "Pages written",
                    "Throughput (GB/s)", "Throughput per Thread (GB/s)"
                ])
            
            print(f"Running benchmarks for {data_source} with multiple configurations")
            
            # Iterate through all parameter combinations for the data source
            for query in base_config["query_options"]:
                for sf in base_config["sf_options"]:
                    for working_mem in base_config["working_mem_options"]:
                        for bp_size in base_config["bp_sizes"]:
                            for num_threads in base_config["num_threads_options"]:
                                print(f"Running benchmark for {data_source} (Query {query}, SF {sf}) "
                                      f"with BP={bp_size}, Threads={num_threads}, WorkingMem={working_mem}")
                                
                                # Create run configuration
                                run_config = {
                                    "bp_size": bp_size,
                                    "num_threads": num_threads,
                                    "working_mem": working_mem,
                                    "data_source": data_source,
                                    "query": query,
                                    "sf": sf
                                }
                                
                                # Run benchmark for each iteration
                                for iteration in range(1, iterations + 1):
                                    print(f"  Iteration {iteration}/{iterations}")
                                    success = run_single_benchmark_iteration(
                                        run_config, output_csv, iteration, args.machine or base_config["machine"]
                                    )
                                    if not success:
                                        print(f"  Failed to run iteration {iteration}")
            
            print(f"Benchmark results saved to {output_csv}")
            
            # Generate plot if requested
            if args.plot:
                plot_thread_scaling(output_csv)
        
        else:
            # Just run a single configuration with all parameters explicitly specified
            config = {
                "data_source": args.data_source or "TPCH",
                "bp_size": args.buffer_pool or 100000,
                "num_threads": args.threads or 16,
                "working_mem": args.working_mem or 1420,
                "query": args.query or 100,
                "sf": args.scale_factor or 1
            }
            
            print(f"Running benchmark for {config['data_source']} (Query {config['query']}, SF {config['sf']}) "
                  f"with BP={config['bp_size']}, Threads={config['num_threads']}, WorkingMem={config['working_mem']}")
            
            # If iterations parameter is provided, run with data collection
            if args.iterations > 1:
                run_manual_benchmark(config, args)
            else:
                # Otherwise just run with terminal output
                run_benchmark(config, is_manual=True)

if __name__ == "__main__":
    main()