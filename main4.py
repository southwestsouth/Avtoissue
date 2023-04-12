import re
import pytesseract
import json
import pandas as pd
import os
from PIL import Image
from jira import JIRA
import psycopg2
from pathlib import Path
import patoolib
from string import Template
import zipfile

# Jira configuration
JIRA_SERVER = 'https://jira.social'
JIRA_USERNAME = 'login'
JIRA_PASSWORD = 'pass'

JQL_QUERY = 'project ='

HAR_EXTENSION = '.har'
PNG_EXTENSION = '.png'
RAR_EXTENSION = '.rar'
ZIP_EXTENSION = '.zip'

ROOT_DIR = Path('data')

TESSERACT_CMD = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD

PATH_TO_UNRAR = os.path.abspath(r'C:\Program Files\WinRAR\Rar.exe')

# Jira authentication
jira = JIRA(server=JIRA_SERVER, basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

# Searching issues
issues = jira.search_issues(JQL_QUERY)

# Connect to the database
conn = psycopg2.connect(
    host="10.1.155.6",
    port="port",
    database="database",
    user="user",
    password="password"
)

# Create a cursor object
cursor = conn.cursor()
print(cursor)

# Create the root directory if it does not exist
ROOT_DIR.mkdir(exist_ok=True)

# Create a pandas dataframe to store the results
df = pd.DataFrame(columns=["Issue Key", "Все_данные", "Решение", "Пациент_ID", "EDF_number"])

# Iterating over the issues
for issue in issues:
    # Create the issue directory
    issue_dir = ROOT_DIR / issue.key
    issue_dir.mkdir(exist_ok=True)

    # Download the first 2 attachments
    attachments = jira.issue(issue.key).fields.attachment
    word = None
    for attachment in attachments:
        if attachment.filename.endswith(PNG_EXTENSION):
            attachment_path = issue_dir / attachment.filename
            with open(attachment_path, 'wb') as f:
                f.write(attachment.get())
            # Perform OCR and extract the first word from the recognized text
            img = Image.open(attachment_path)
            width, height = img.size
            img = img.crop((70, 110, 480, 170))
            text = pytesseract.image_to_string(img, lang='rus')
            titles = text.title()
            words = titles.split()
            print(words)
            if not words:
                img = Image.open(attachment_path)
                width, height = img.size
                img = img.crop((950, 163, 1300, 220))
                text = pytesseract.image_to_string(img, lang='rus')
                titles = text.title()
                words = titles.split()
                print(words)
                if not words:
                    img = Image.open(attachment_path)
                    width, height = img.size
                    img = img.crop((100, 280, 450, 345))
                    text = pytesseract.image_to_string(img, lang='rus')
                    titles = text.title()
                    words = titles.split()
                    print(words)
            if words:
                word = words[0]
                print(word)
                break
    found_word = False
    if word:
        for attachment in attachments:
            if attachment.filename.endswith(HAR_EXTENSION):
                attachment_path = issue_dir / attachment.filename
                with open(attachment_path, 'wb') as f:
                    f.write(attachment.get())
                with open(attachment_path, "r", encoding='utf-8') as har_f:
                    har_data = json.load(har_f)
                    for entry in har_data['log']['entries']:
                        methodName = entry['request']['url'].split('/')[-1]
                        if methodName != 'identifyPatient.api' and methodName != 'getPatientList.api':
                            continue
                        if word in entry['response']['content']['text']:
                            json_data_str = json.loads(entry['response']['content']['text'])
                            if methodName == 'identifyPatient.api':
                                patient_id = json_data_str['result']['patient']['id']
                                found_word = True
                                print(patient_id)
                            else:
                                if methodName == 'getPatientList.api':
                                    patient_response = json_data_str['result']['patientResponse']
                                    for response in patient_response:
                                        patient = response['patient']
                                        print(patient)
                                        emias_id = patient['emiasId']
                                        personal_data = patient['personalData']
                                        last_name = personal_data['lastName']
                                        print(last_name)
                                        if last_name == word:
                                            patient_id = emias_id
                                            found_word = True
                                            print(patient_id)
                                            break
                            if found_word:
                                break

            elif attachment.filename.endswith(RAR_EXTENSION):
                attachment_path = issue_dir / attachment.filename
                with open(attachment_path, 'wb') as f:
                    f.write(attachment.get())
                patoolib.extract_archive(attachment_path.as_posix(), outdir=issue_dir)
                for extracted_file in issue_dir.iterdir():
                    if extracted_file.suffix == ".har":
                        with open(extracted_file, "r", encoding='utf-8') as har_f:
                            har_data = json.load(har_f)
                            for entry in har_data['log']['entries']:
                                methodName = entry['request']['url'].split('/')[-1]
                                if methodName != 'identifyPatient.api' and methodName != 'getPatientList.api':
                                    continue
                                if word in entry['response']['content']['text']:
                                    json_data_str = json.loads(entry['response']['content']['text'])
                                    if methodName == 'identifyPatient.api':
                                        patient_id = json_data_str['result']['patient']['id']
                                        found_word = True
                                        print(patient_id)
                                    else:
                                        if methodName == 'getPatientList.api':
                                            patient_response = json_data_str['result']['patientResponse']
                                            for response in patient_response:
                                                patient = response['patient']
                                                print(patient)
                                                emiasid = patient['emiasId']
                                                personal_data = patient['personalData']
                                                last_name = personal_data['lastName']
                                                print(last_name)
                                                if last_name == word:
                                                    patient_id = emiasid
                                                    found_word = True
                                                    print(patient_id)
                                                    break
                                    if found_word:
                                        break
            if found_word:
                break
            elif attachment.filename.endswith(ZIP_EXTENSION):
                attachment_path = issue_dir / attachment.filename
                with open(attachment_path, 'wb') as f:
                    f.write(attachment.get())
                with zipfile.ZipFile(attachment_path, 'r') as zip_ref:
                    zip_ref.extractall(issue_dir)
                    for extracted_file in issue_dir.iterdir():
                        if extracted_file.suffix == ".har":
                            with open(extracted_file, "r", encoding='utf-8') as har_f:
                                har_data = json.load(har_f)
                                for entry in har_data['log']['entries']:
                                    methodName = entry['request']['url'].split('/')[-1]
                                    if methodName != 'identifyPatient.api' and methodName != 'getPatientList.api':
                                        continue
                                    if word in entry['response']['content']['text']:
                                        json_data_str = json.loads(entry['response']['content']['text'])
                                        if methodName == 'identifyPatient.api':
                                            patient_id = json_data_str['result']['patient']['id']
                                            found_word = True
                                            print(patient_id)
                                        else:
                                            if methodName == 'getPatientList.api':
                                                patient_response = json_data_str['result']['patientResponse']
                                                for response in patient_response:
                                                    patient = response['patient']
                                                    print(patient)
                                                    emiasid = patient['emiasId']
                                                    personal_data = patient['personalData']
                                                    last_name = personal_data['lastName']
                                                    print(last_name)
                                                    if last_name == word:
                                                        patient_id = emiasid
                                                        found_word = True
                                                        print(patient_id)
                                                        break
                                        if found_word:
                                            break
            if found_word:
                break
    if found_word:
        # Open the SQL script file
        with open("sql.txt", "r") as f:
            sql = f.read()
            template = Template(sql)
            corr_sql = template.substitute(patient=patient_id)

            # Use the extracted ID to retrieve information from the "patients" table
        cursor.execute(corr_sql)

        # Fetch the results
        results = cursor.fetchall()
        # Check if results are not empty
        # results = cursor.fetchone()

        # Print the results
        Data = []
        for result in results:
            result_text = str(result)
            slova = re.split("\W+", result_text)
            print(slova)
            first_word = slova[1]
            print(first_word)
            third_word = slova[5]
            print(third_word)
            fourth_word = slova[4]

            df = df.append({"Issue Key": 'https://jira.mos.social/browse/' + issue.key, "Решение": first_word,
                            "Пациент_ID": third_word, "EDF_number": fourth_word, "Все_данные": result},
                           ignore_index=True)

            if result is not None:
                issue = jira.issue(issue.key)
                labels = issue.fields.labels
                labels.append('РГ.ЭЛН')
                issue.update(fields={'labels': labels})
                print("Labels updated for issue: " + issue.key)
                comment = "Решение по проблеме: " + first_word + " Пациент_id: " + third_word
                issue = jira.issue(issue.key)
                jira.add_comment(issue.key, comment)
                print("Comments added: " + issue.key)

            # Save the dataframe to an Excel file
        df.to_excel("results.xlsx", index=False)


    else:
        print(f"Слово не найдено в талоне {issue.key}")

        # Close the cursor
cursor.close()

# Close the connection
conn.close()
