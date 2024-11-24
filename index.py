import ollama
from lib.bigquery import BigQueryAPI
import time
import json

dataset = "api_test"
researchersTable = "researchers"
biographiesTable = "biographies"
prompt = """
You are an expert writer tasked with crafting concise, respectful, and accurate summaries of scientific researchers' work based on their latest published articles. Using the provided name and list of articles, generate a single-paragraph summary of approximately 100 words. Focus on their research contributions, fields of study, and the broader impact of their work. Avoid personal details or assumptions about their background or gender. Use gender-neutral language, avoiding pronouns like "he" or "she." Ensure the tone is professional and the summary highlights the importance and relevance of their research. The response should consist solely of the summary without any introductory or closing remarks.
Name of the researcher: |name|
List of recent articles: |pubs|
"""
model = "llama3.2"
researchers = []
biographies = []
initial_time = time.time()
print(f"Starting time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(initial_time))}")

bq = BigQueryAPI(dataset)
batchSize = 100

biographies = bq.get_empty_researchers(researchersTable, batchSize)
while len(biographies) > 0:
    updates = []

    for bio in biographies:
        if isinstance(bio, str):
            print(bio)
            exit()
        print(f"Generating summaries for: {bio['res_id']} {bio['res_name']}")
        p = prompt.replace("|name|", bio["res_name"])
        p = p.replace("|pubs|", bio["res_top20_recent_titles"])
        response = ollama.chat(model=model, messages=[
            {
                'role': 'user',
                'content': p
            }
        ])
        if len(response['message']['content']) < 150:
            print(f"Discarding response for {bio['res_id']} {bio['res_name']}")
            continue
        data = {
            "res_id": bio["res_id"],
            "res_bio": response['message']['content']
        }
        with open("summaries.txt", "a") as f:
            f.write(json.dumps(data) + "\n")
        updates.append(data)

    print(len(updates))
    bq.update_researchers_in_bulk(biographiesTable, researchersTable, updates)
    biographies = bq.get_empty_researchers(researchersTable, batchSize)

final_time = time.time()
print(f"Time elapsed: {final_time - initial_time} seconds")