import os
import csv
import argparse
from multiprocessing import Process
from dotenv import load_dotenv
from google import genai

load_dotenv()

prompt = """
You are an expert dataset creator for code reasoning benchmarks.

Your task is to generate Python programming problems that closely follow the style, structure, and difficulty distribution of the HumanEval benchmark, 
CRITICAL: YOU MUST NOT BE copying or paraphrasing any existing HumanEval problem. 

STRICT REQUIREMENTS:
- The problem must define exactly ONE Python function.
- Do not make the problem overly long.
- The function signature must be explicit.
- The problem description must be in a Python docstring.
- The task must be solvable using standard Python (no external libraries).
- Avoid domain-specific problems (finance, web, ML, etc.).
- Focus on core algorithmic reasoning.

ALLOWED TASK TYPES (rotate across problems):
- List and array manipulation
- String processing
- Dictionary and set logic
- Numerical reasoning
- Prefix/suffix or sliding window logic
- State tracking across iterations
- Simple recursion or nested structures
- Edge-case handling

NOT ALLOWED:
- CRITICAL: Copying any known HumanEval problem
- Multi-function tasks
- Input/output handling (no stdin/stdout)
- Object-oriented code
- Overly trivial tasks

DIFFICULTY:
- Easy to medium (HumanEval-like)
- Requires correct logic, not tricks

OUTPUT FORMAT (STRICT):
Return ONLY a Python function stub with docstring, like this:

```python
def function_name(arguments):
    STRICT FORMAT:
    Simple, short and Clear problem description. Be crisp and to the point only. 
    Include constraints (if applicable) and MUST WITH ONLY 2-5 (based on the complexity of the question, you chose the optimal count) examples (no explanation).
    ```
"""


def generate_questions(process_num, num_questions):
    """Generate questions for a single process"""
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    
    output_file = f'outputs_{process_num}.csv'
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['output'])  # Header
        
        for i in range(num_questions):
            print(f"[Process {process_num}] Running iteration {i+1}/{num_questions}...")
            
            try:
                response = client.models.generate_content(
                    model="gemini-3-pro-preview",
                    contents=prompt,
                )
                
                # Write output to CSV
                writer.writerow([response.text])
                csvfile.flush()  # Ensure it's written immediately
                
                print(f"[Process {process_num}] Iteration {i+1} completed and saved")
            except Exception as e:
                print(f"[Process {process_num}] Error on iteration {i+1}: {e}")
                writer.writerow([f"ERROR: {e}"])
                csvfile.flush()
    
    print(f"[Process {process_num}] All {num_questions} iterations completed! Saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Generate questions in parallel')
    parser.add_argument('--processes', type=int, default=4, help='Number of parallel processes (default: 4)')
    parser.add_argument('--total', type=int, default=200, help='Total number of questions (default: 200)')
    
    args = parser.parse_args()
    
    num_processes = args.processes
    total_questions = args.total
    questions_per_process = total_questions // num_processes
    
    print(f"Starting {num_processes} processes, each generating {questions_per_process} questions...")
    print(f"Total questions to generate: {total_questions}")
    
    # Create and start processes
    processes = []
    for i in range(num_processes):
        p = Process(target=generate_questions, args=(i+1, questions_per_process))
        p.start()
        processes.append(p)
        print(f"Started process {i+1}")
    
    # Wait for all processes to complete
    for i, p in enumerate(processes):
        p.join()
        print(f"Process {i+1} finished")
    
    print(f"\n✅ All processes completed!")
    print(f"Generated {num_processes} CSV files: outputs_1.csv, outputs_2.csv, ..., outputs_{num_processes}.csv")


if __name__ == "__main__":
    main()
