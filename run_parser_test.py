import sys
import os
import json

# Add project root to path
sys.path.append(os.getcwd())

from services.ingestion.csv_parser import CSVParser

def main():
    csv_path = "Grade 1 LEAP- Language Excellence & Assessment.csv"
    
    if not os.path.exists(csv_path):
        print(f"Error: File '{csv_path}' not found.")
        return

    print(f"Reading {csv_path}...")
    with open(csv_path, "rb") as f:
        csv_data = f.read()

    print("Initializing CSVParser...")
    try:
        parser = CSVParser()
    except Exception as e:
        print(f"Failed to initialize parser (check API keys?): {e}")
        return

    print("Parsing CSV (this may take a moment as it calls OpenAI)...")
    try:
        # Parse as JSON objects for easier printing
        submissions = parser.parse_csv(csv_data, as_json=True)
        
        print(f"\nSuccessfully parsed {len(submissions)} submissions.")
        
        if submissions:
            print("\n--- First Submission Sample ---")
            print(json.dumps(submissions[0], indent=2, default=str))
            
    except Exception as e:
        print(f"Error during parsing: {e}")

if __name__ == "__main__":
    main()
