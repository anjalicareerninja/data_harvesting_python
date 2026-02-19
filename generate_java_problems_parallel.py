"""
Stage 5: Generate a third Java solution (solution_c) for existing problems.
Reads java_problem_7.csv and java_problem_8.csv, and generates the 'solution_c' column 
using the question, solution_a, and solution_b as context.
"""
import os
import csv
import json
from multiprocessing import Process
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

schema = """
// Converted Java method
import java.util.HashMap;
import java.util.Map;

class EnhancedApiError {
private final ErrorResponse response;
private final ErrorCode errorCode;
private final Map<String, String> additionalDetails;

public EnhancedApiError(ErrorResponse response, ErrorCode errorCode, Map<String, String> additionalDetails) {
this.response = response;
this.errorCode = errorCode;
this.additionalDetails = new HashMap<>(additionalDetails);
}

public ErrorCode getErrorCode() {
return errorCode;
}

public String getEnhancedMessage() {
StringBuilder message = new StringBuilder(getBaseMessage());
if (!additionalDetails.isEmpty()) {
message.append("\\nAdditional Details:");
for (Map.Entry<String, String> entry : additionalDetails.entrySet()) {
message.append("\\n- ").append(entry.getKey()).append(": ").append(entry.getValue());
}
}
return message.toString();
}

public String getBaseMessage() {
return ErrorMessageResolver.resolve(errorCode);
}

public ErrorResponse getOriginalResponse() {
return response;
}

public boolean isRecoverable() {
return errorCode.getSeverity() != ErrorSeverity.CRITICAL;
}

public Map<String, String> getAdditionalDetails() {
return new HashMap<>(additionalDetails);
}
}

enum ErrorCode {
NON_KEY_ACCOUNT_BALANCE_ERROR(ErrorSeverity.HIGH, "Account balance access denied"),
INVALID_CREDENTIALS(ErrorSeverity.CRITICAL, "Invalid credentials"),
SESSION_EXPIRED(ErrorSeverity.MEDIUM, "Session expired"),
RATE_LIMIT_EXCEEDED(ErrorSeverity.MEDIUM, "Rate limit exceeded");

private final ErrorSeverity severity;
private final String defaultMessage;

ErrorCode(ErrorSeverity severity, String defaultMessage) {
this.severity = severity;
this.defaultMessage = defaultMessage;
}

public ErrorSeverity getSeverity() {
return severity;
}

public String getDefaultMessage() {
return defaultMessage;
}
}

enum ErrorSeverity {
LOW, MEDIUM, HIGH, CRITICAL
}

class ErrorMessageResolver {
public static String resolve(ErrorCode errorCode) {
return errorCode.getDefaultMessage();
}
}

class ErrorResponse {
private final int statusCode;
private final String rawResponse;

public ErrorResponse(int statusCode, String rawResponse) {
this.statusCode = statusCode;
this.rawResponse = rawResponse;
}

public int getStatusCode() {
return statusCode;
}

public String getRawResponse() {
return rawResponse;
}
}
"""

PROMPT_SOLUTION_C_TEMPLATE = """You an expert java solution generator. Your goal is to look at the question and the current solution in detail such that you generate a solution , in a very different logic from the current one,detailed one for it basis the demo test given to you as well. Look at the example solution given to you below but the solution you generate must be considering all constraints and edge cases.

Generate an alternative or refined Java solution (solution_b) for the following problem.
Consider the previous solution (solution_a) and provide another high-quality implementation that satisfies all constraints and the demo test.

YOU MUST GENERATE A COMPLETELY NEW LOGIC FROM THE PREVIOUS SOLUTIONS. 
Using the QUESTION, SOLUTION_A, and SOLUTION_B below, generate a completely new, logical third Java solution (solution_c).

QUESTION:
{question}

DEMO_TEST:
{demo_test}

PREVIOUS SOLUTION (SOLUTION_A):
{solution_a}

SOLUTION_B:
{solution_b}

EXAMPLE SOLUTION FORMAT:
{schema}
"""

SCHEMA_SOLUTION_C = genai.types.Schema(
    type=genai.types.Type.OBJECT,
    required=["solution_c"],
    properties={
        "solution_c": genai.types.Schema(type=genai.types.Type.STRING),
    },
)

def run_solution_c_generation_process(process_num, file_path):
    """Reads existing CSV and generates solution_c for each row."""
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    print(f"[Process {process_num}] Starting Solution C Generation for {file_path}...")
    
    rows = []
    try:
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"[Process {process_num}] Error reading {file_path}: {e}")
        return

    if "solution_c" not in fieldnames:
        fieldnames.append("solution_c")

    for i, row in enumerate(rows):
        print(f"[Process {process_num}] Generating solution_c {i+1}/{len(rows)} for {row.get('question_id')}...")
        
        prompt = PROMPT_SOLUTION_C_TEMPLATE.format(
            schema=schema,
            question=row.get("question", ""),
            demo_test=row.get("demo_test_func", ""),
            solution_a=row.get("solution_a", ""),
            solution_b=row.get("solution_b", "")
        )
        
        try:
            response = client.models.generate_content(
                model="gemini-3-flash-preview",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=SCHEMA_SOLUTION_C,
                ),
            )
            data = json.loads(response.text.strip())
            row["solution_c"] = data.get("solution_c", "")
        except Exception as e:
            print(f"Error on {row.get('question_id')}: {e}")
            row["solution_c"] = "ERROR"

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)

    print(f"[Process {process_num}] Solution C generation completed for {file_path}")

def main():
    files = ["java_problem_7.csv", "java_problem_8.csv"]
    processes = []
    for i, file_path in enumerate(files, 7):
        p = Process(target=run_solution_c_generation_process, args=(i, file_path))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    print("Dual process Solution C Generation completed.")

if __name__ == "__main__":
    main()
