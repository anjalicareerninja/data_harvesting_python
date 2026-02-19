import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
import json
import pandas as pd
from multiprocessing import Process

load_dotenv()

# Initialize Gemini client
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def get_gemini_testcases(question_text, question_id):
    """
    Call Gemini API with structured output to generate test cases.
    Returns test cases string.
    """
    your_prompt = f"""
 input and output respectively. You must strictly follow this format.

{question_text}

"""
    
    try:
        # Make the API call with structured output
        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=your_prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=genai.types.Schema(
                    type=genai.types.Type.OBJECT,
                    required=["test_cases"],
                    properties={
                        "test_cases": genai.types.Schema(
                            type=genai.types.Type.STRING,
                        ),
                    },
                ),
            )
        )
        
        # Extract the structured response
        result = json.loads(response.text)
        
        return result.get("test_cases", "")
    except Exception as e:
        print(f"Error processing question ID {question_id}: {str(e)}")
        return f"ERROR: {str(e)}"


def process_csv_file(csv_file, process_num):
    """Process a CSV file and generate test cases."""
    csv_file = os.path.abspath(csv_file)
    print(f"Process {process_num} started for {csv_file}")
    
    # Check if file can be written to
    try:
        with open(csv_file, 'a'):
            pass
    except PermissionError:
        print(f"PERMISSION DENIED: {csv_file} is locked!")
        return
    
    # Read the CSV file
    df = pd.read_csv(csv_file, keep_default_na=False)
    
    # Add the tests column if it doesn't exist
    if 'tests' not in df.columns:
        df['tests'] = ''
    
    # Process each row
    total_rows = len(df)
    for idx in range(total_rows):
        # Get the question from the 'output' column
        question_text = str(df.loc[idx, 'output'])
        
        print(f"Process {process_num}: Row {idx+1}/{total_rows}...")
        
        test_cases = get_gemini_testcases(question_text, idx+1)
        
        # Fill the tests column
        df.at[idx, 'tests'] = test_cases
        
        # Save after each row
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"Process {process_num}: Done row {idx+1}/{total_rows}")
    
    print(f"Process {process_num} finished")


def main():
    csv_files = [
        "outputs_1_test.csv",
        "outputs_2_test.csv",
        "outputs_3_test.csv",
        "outputs_4_test.csv",
        "outputs_5_test.csv"
    ]
    
    # Create and start processes
    processes = []
    for i, csv_file in enumerate(csv_files, 1):
        p = Process(target=process_csv_file, args=(csv_file, i))
        processes.append(p)
        p.start()
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    print("All processes completed!")


if __name__ == "__main__":
    main()
