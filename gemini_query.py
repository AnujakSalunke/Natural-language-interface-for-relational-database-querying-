import os
import google.generativeai as genai

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

def format_schema_for_prompt(schema_info):
    schema_text = "Database Schema:\n"
    for table_name, info in schema_info.items():
        schema_text += f"\nTable: {table_name}\nColumns:\n"
        for col in info['columns']:
            col_name = col.get('Field', str(col))
            col_type = col.get('Type', '')
            nullable = 'NULL' if col.get('Null','') == 'YES' else 'NOT NULL'
            key = col.get('Key','')
            schema_text += f"- {col_name} ({col_type}) {nullable}"
            if key == 'PRI':
                schema_text += " PRIMARY KEY"
            schema_text += "\n"
    schema_text += "\nRelationships:\n"
    for table_name, info in schema_info.items():
        for fk in info['foreign_keys']:
            schema_text += f"- {table_name}.{fk['COLUMN_NAME']} -> {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}\n"
    return schema_text

def generate_sql(question, schema_info):
    model = genai.GenerativeModel("gemini-pro")
    schema_text = format_schema_for_prompt(schema_info)
    prompt = f"""Convert the following English question into a MySQL query.
Return only SQL.

Schema:
{schema_text}

Question: {question}
"""
    resp = model.generate_content(prompt)

    if hasattr(resp, 'parts'):
        sql = resp.parts[0].text.strip()
    elif hasattr(resp, 'text'):
        sql = resp.text.strip()
    else:
        raise Exception("Unexpected Gemini response format")
    return sql.replace('```sql','').replace('```','').strip()
