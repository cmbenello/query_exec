#!/usr/bin/env python3
import subprocess
import re
import csv
import argparse
import time
import os
from datetime import datetime

def run_executable(executable_path, working_mem, target_runcount, threads, num_tuples):
    """Run the bash executable with the specified parameters and return its output."""
    cmd = [executable_path, 
           f"--working-mem={working_mem}", 
           f"--target-runcount={target_runcount}",
           f"--threads={threads}",
           f"--num-tuples={num_tuples}"]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error running executable: {result.stderr}")
        return None
    
    return result.stdout

def parse_output(output):
    """Parse the executable output to extract the metrics we want."""
    results = {}
    
    # Extract generation duration
    gen_duration_match = re.search(r'generation duration (\d+\.\d+)ms', output)
    if gen_duration_match:
        results['generation_duration_ms'] = float(gen_duration_match.group(1))
    
    # Extract number of runs merging
    runs_merging_match = re.search(r'Merging (\d+) runs', output)
    if runs_merging_match:
        results['runs_merging'] = int(runs_merging_match.group(1))
    
    # Extract parallel merge duration
    merge_duration_match = re.search(r'Finished parallel merge in (\d+\.\d+)s', output)
    if merge_duration_match:
        results['parallel_merge_duration_s'] = float(merge_duration_match.group(1))
    
    # Extract total merge steps
    merge_steps_match = re.search(r'total merge steps = (\d+)', output)
    if merge_steps_match:
        results['merge_steps'] = int(merge_steps_match.group(1))
    
    # Extract total number of tuples
    tuples_match = re.search(r'Total tuples estimated: (\d+)', output)
    if tuples_match:
        results['total_tuples'] = int(tuples_match.group(1))
    
    # Extract working mem limit
    working_mem_match = re.search(r'working mem limit (\d+)', output)
    if working_mem_match:
        results['working_mem'] = int(working_mem_match.group(1))
    
    # Extract merge duration 
    merge_duration_s_match = re.search(r'merge duration (\d+\.\d+)s', output)
    if merge_duration_s_match:
        results['merge_duration_s'] = float(merge_duration_s_match.group(1))
    
    # Extract parallel execution time
    parallel_exec_match = re.search(r'Parallel execution took (\d+\.\d+)ms', output)
    if parallel_exec_match:
        results['parallel_execution_ms'] = float(parallel_exec_match.group(1))
    
    return results

def main():
    parser = argparse.ArgumentParser(description='Run experiments with different parameter combinations.')
    parser.add_argument('executable', help='Path to the bash executable')
    parser.add_argument('--working-mem', type=int, default=1000000, 
                        help='Working memory limit (default: 1000000)')
    parser.add_argument('--iterations', type=int, default=3, 
                        help='Number of times to run each experiment (default: 3)')
    parser.add_argument('--output-dir', default='experiment_results', 
                        help='Directory to save results in (default: experiment_results)')
    parser.add_argument('--delay', type=int, default=2,
                        help='Delay in seconds between experiment runs (default: 2)') 
    
    args = parser.parse_args()
    
    # Define experiment parameters
    thread_counts = [1, 2, 4, 8, 16, 20, 30, 40]
    target_run_counts = [900, 1200, 2000, 3000, 6000, 12000]
    tuple_modes = [
        {"name": "fixed", "base": 6005720 * 10, "multiplier": 0},
        {"name": "variable", "base": 0, "multiplier": 1501530}
    ]
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Create master CSV file to record all experiment results
    master_csv_path = os.path.join(args.output_dir, "all_experiments.csv")
    fieldnames = ['timestamp', 'experiment_id', 'tuple_mode', 'working_mem', 
                  'target_runcount', 'threads', 'num_tuples', 'iteration',
                  'generation_duration_ms', 'parallel_execution_ms', 'runs_merging', 
                  'parallel_merge_duration_s', 'merge_steps', 'total_tuples', 
                  'merge_duration_s']
    
    with open(master_csv_path, 'w', newline='') as master_csv:
        master_writer = csv.DictWriter(master_csv, fieldnames=fieldnames)
        master_writer.writeheader()
    
        # Track experiment count for ID generation
        experiment_count = 0
        
        # Run through all parameter combinations
        total_experiments = len(thread_counts) * len(target_run_counts) * len(tuple_modes) * args.iterations
        current_experiment = 0
        
        for tuple_mode in tuple_modes:
            for target_runcount in target_run_counts:
                for threads in thread_counts:
                    experiment_count += 1
                    experiment_id = f"exp_{experiment_count:04d}_{tuple_mode['name']}_t{threads}_r{target_runcount}"
                    
                    # Calculate number of tuples based on mode
                    if tuple_mode["name"] == "fixed":
                        num_tuples = tuple_mode["base"]
                    else:  # variable mode
                        num_tuples = tuple_mode["multiplier"] * threads
                    
                    print(f"\n{'='*80}")
                    print(f"EXPERIMENT: {experiment_id}")
                    print(f"Tuple Mode: {tuple_mode['name']}")
                    print(f"Working Memory: {args.working_mem}")
                    print(f"Target Run Count: {target_runcount}")
                    print(f"Threads: {threads}")
                    print(f"Number of Tuples: {num_tuples}")
                    print(f"{'='*80}")
                    
                    # Create experiment-specific CSV file
                    exp_csv_path = os.path.join(args.output_dir, f"{experiment_id}.csv")
                    with open(exp_csv_path, 'w', newline='') as exp_csv:
                        exp_writer = csv.DictWriter(exp_csv, fieldnames=fieldnames)
                        exp_writer.writeheader()
                        
                        # Run experiment multiple times
                        for i in range(1, args.iterations + 1):
                            current_experiment += 1
                            progress = (current_experiment / total_experiments) * 100
                            
                            print(f"\nRunning iteration {i}/{args.iterations} (Overall progress: {progress:.1f}%)")
                            
                            # Run the executable
                            output = run_executable(
                                args.executable,
                                args.working_mem,
                                target_runcount,
                                threads,
                                num_tuples
                            )
                            
                            if output:
                                # Parse the results
                                results = parse_output(output)
                                if results:
                                    # Add metadata
                                    results['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    results['experiment_id'] = experiment_id
                                    results['tuple_mode'] = tuple_mode['name']
                                    results['working_mem'] = args.working_mem
                                    results['target_runcount'] = target_runcount
                                    results['threads'] = threads
                                    results['num_tuples'] = num_tuples
                                    results['iteration'] = i
                                    
                                    # Write to both CSVs
                                    exp_writer.writerow(results)
                                    
                                    # Write to master CSV file
                                    with open(master_csv_path, 'a', newline='') as master_append:
                                        master_append_writer = csv.DictWriter(master_append, fieldnames=fieldnames)
                                        master_append_writer.writerow(results)
                                    
                                    print("Extracted metrics:")
                                    for key, value in results.items():
                                        if key not in ['timestamp', 'experiment_id', 'tuple_mode', 
                                                      'working_mem', 'target_runcount', 'threads', 
                                                      'num_tuples', 'iteration']:
                                            print(f"  {key}: {value}")
                                else:
                                    print("Failed to parse output")
                            else:
                                print("Executable failed to run properly")
                            
                            # Sleep between iterations
                            if i < args.iterations or current_experiment < total_experiments:
                                print(f"Sleeping for {args.delay} seconds before next run...")
                                time.sleep(args.delay)
    
    print(f"\nAll experiments completed. Results saved to {args.output_dir}")
    print(f"Master results file: {master_csv_path}")

if __name__ == "__main__":
    main()
