# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69118442-f38c-4f27-ac49-50e1498a489b",
# META       "default_lakehouse_name": "New_Lake",
# META       "default_lakehouse_workspace_id": "84782802-80aa-4b18-8e26-c94ddb42e9a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "69118442-f38c-4f27-ac49-50e1498a489b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# notebookutils.notebook.run("Data_Profiling", 3600)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# notebookutils.notebook.run("CleanOldBronzeData", 3600)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# from datetime import datetime

# TEAMS_SUCCESS_WEBHOOK = "https://default9be904791c7f423aa809e751badc66.e0.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/31/workflows/6c21c8912e934035850c83b7211b8106/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=_sgeRHkgXkIrndBq0hy14TGoe15482BSCSXSi9yfIss"

# def send_pipeline_alert(pipeline_name, status="Succeeded", details=""):
#     color = "Good" if status == "Succeeded" else "Attention"
#     icon = "🟢" if status == "Succeeded" else "🔴"

#     payload = {
#         "type": "message",
#         "attachments": [
#             {
#                 "contentType": "application/vnd.microsoft.card.adaptive",
#                 "content": {
#                     "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
#                     "type": "AdaptiveCard",
#                     "version": "1.4",
#                     "body": [
#                         {
#                             "type": "TextBlock",
#                             "text": f"{icon} Pipeline {status}",
#                             "weight": "Bolder",
#                             "size": "Large",
#                             "color": color,
#                             "wrap": True
#                         },
#                         {
#                             "type": "FactSet",
#                             "facts": [
#                                 {"title": "Pipeline Name:", "value": pipeline_name},
#                                 {"title": "Status:", "value": status},
#                                 {"title": "Time:", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
#                             ]
#                         },
#                         {
#                             "type": "TextBlock",
#                             "text": details,
#                             "wrap": True,
#                             "isVisible": bool(details)
#                         }
#                     ]
#                 }
#             }
#         ]
#     }

#     response = requests.post(TEAMS_SUCCESS_WEBHOOK, json=payload)
#     print("Status Code:", response.status_code)
#     print("Response Text:", response.text)
#     return response


# send_pipeline_alert(
#     pipeline_name="ELT-Pipeline",
#     status="Succeeded",
#     details="Bronze and Silver layers refreshed successfully."
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import notebookutils
import traceback
from datetime import datetime

# 1. Link Failed
TEAMS_FAILURE_WEBHOOK = "https://default9be904791c7f423aa809e751badc66.e0.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/03/workflows/0780ff6302e141afa4bd05038c74dcbe/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=RjE6uDtXQSrSv9BEEO190bucOeeLW8arkkpBqeSf1Tc"

# 2. Link Succeeded
TEAMS_SUCCESS_WEBHOOK = "https://default9be904791c7f423aa809e751badc66.e0.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/31/workflows/6c21c8912e934035850c83b7211b8106/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=_sgeRHkgXkIrndBq0hy14TGoe15482BSCSXSi9yfIss"


def send_teams_notification(webhook_url, title, status, facts_dict, details=""):
    color = "Good" if status == "SUCCESS" else "Attention"
    icon = "🟢" if status == "SUCCESS" else "🔴"

    facts = [{"title": f"{k}:", "value": str(v)} for k, v in facts_dict.items()]

    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": f"{icon} {title}",
                            "weight": "Bolder",
                            "size": "Large",
                            "color": color,
                            "wrap": True
                        },
                        {
                            "type": "FactSet",
                            "facts": facts
                        },
                        {
                            "type": "TextBlock",
                            "text": details,
                            "wrap": True,
                            "isVisible": bool(details)
                        }
                    ]
                }
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=payload)
        print(f"Teams notification status: {response.status_code}")
        return response.status_code
    except Exception as e:
        print(f"Failed to send Teams notification: {str(e)}")


def run_notebook(notebook_name, timeout=3600):
    try:
        print(f"Starting {notebook_name}...")
        result = notebookutils.notebook.run(notebook_name, timeout)
        print(f"✅ {notebook_name} completed successfully.")
        return {"notebook": notebook_name, "status": "SUCCESS"}

    except Exception as e:
        error_message = str(e)
        print(f"❌ {notebook_name} FAILED.")

        send_teams_notification(
            TEAMS_FAILURE_WEBHOOK,
            title="Pipeline Failed",
            status="FAILED",
            facts_dict={
                "Notebook": notebook_name,
                "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            details=f"Error: {error_message[:300]}"
        )
        raise e


# Notebooks
notebooks = [
    "Data_Profiling",
    "CleanOldBronzeData",
    "New_Sliver",
    "Data_Validation",
    "Sliver_SCD",
    "Golden_Schema"
]

completed_notebooks = []
print("🚀 Starting Master Orchestrator Pipeline...")

try:
    for nb in notebooks:
        res = run_notebook(nb, timeout=3600)
        completed_notebooks.append(nb)

    send_teams_notification(
        TEAMS_SUCCESS_WEBHOOK,
        title="Data Pipeline Completed Successfully",
        status="SUCCESS",
        facts_dict={
            "Status": "SUCCESS",
            "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Completed Notebooks": ", ".join(completed_notebooks)
        }
    )
    print("🎉 All Notebooks completed successfully!")

except Exception as pipeline_error:
    print("🛑 Pipeline stopped due to a failure.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
