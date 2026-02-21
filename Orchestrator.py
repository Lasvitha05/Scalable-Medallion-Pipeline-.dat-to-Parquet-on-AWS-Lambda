import time
import subprocess
import sys
import os

# --- CONFIGURATION ---
TARGET_ITERATIONS = 20   

def run_script(script_path):
    """Runs a python script and waits for it to finish."""
    print(f"\n Running {script_path}...")
    
    # Check if file exists before running
    if not os.path.exists(script_path):
        print(f" Error: File not found at {script_path}")
        return False

    # Run the script
    result = subprocess.run([sys.executable, script_path], capture_output=False)
    
    if result.returncode != 0:
        print(f" Error executing {script_path}!")
        return False
        
    return True

def main():
    print(f" Starting Nightly Batch Orchestrator...")
    print(f" Target: {TARGET_ITERATIONS} cycles.")
    
    # Verify paths before starting the loop
    if not os.path.exists("generate_data.py"):
        print(" CRITICAL: generate_data.py not found in root!")
        return
    if not os.path.exists("ProcureFlow_Ingestor/manual_ingestor.py"):
        print(" CRITICAL: ProcureFlow_Ingestor/manual_ingestor.py not found!")
        return

    for i in range(1, TARGET_ITERATIONS + 1):
        print(f"\n========================================")
        print(f"    CYCLE {i} / {TARGET_ITERATIONS}")
        print(f"========================================")
        
        # 1. Generate Data (Root folder)
        success = run_script("generate_data.py")
        if not success: 
            print(" Stopping due to Generator error.")
            break
        
        # 2. Ingest Data (Subfolder)
        # Moves data from Landing -> Raw (which triggers the Cleaner Lambda)
        success = run_script("ProcureFlow_Ingestor/manual_ingestor.py")
        if not success: 
            print("Stopping due to Ingestor error.")
            break
        
        print(f" Cycle {i} complete. Cooling down for 10 seconds...")
        time.sleep(10) # Pause to prevent rate limiting

    print("\n ORCHESTRATION COMPLETE! Check your 'clean_data/' folder for results.")

if __name__ == "__main__":
    main()