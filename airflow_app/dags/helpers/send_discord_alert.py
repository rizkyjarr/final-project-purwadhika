import requests
import json

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1338491764238979132/gxkcO89HefdilVEvU0JeNmo_Mqsyg3Rg-V_S4Rs5k1yzysSA7_el9XvcNtsS_6fUXwAU"  # 🔹 Replace with your webhook URL

def send_discord_alert(context, alert_type="failure"):

    dag_id = context.get('dag_run').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    if alert_type == "failure":
        message = f"🚨 **Airflow Task Failed! Need actions immediately** 🚨\n" \
                  f" **DAG**: `{dag_id}`\n" \
                  f" **Task**: `{task_id}`\n" \
                  f" **Execution Date**: `{execution_date}`\n" \
                  f" **Error**: `{exception}`"
    elif alert_type == "retry":
        message = f"🔄 **Airflow Task Retrying!** 🔄\n" \
                  f" **DAG**: `{dag_id}`\n" \
                  f" **Task**: `{task_id}`\n" \
                  f" **Execution Date**: `{execution_date}`\n" \
                  f" **Retrying attempt**"
    elif alert_type == "success":
        message = f"✅ **Airflow Task Completed!** ✅\n" \
                  f" **DAG**: `{dag_id}`\n" \
                  f" **Task**: `{task_id}`\n" \
                  f" **Execution Date**: `{execution_date}`\n" \
                  f" **Success**"

    payload = {"content": message}
    headers = {"Content-Type": "application/json"}

    response = requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(payload), headers=headers)
    
    if response.status_code != 204:
        print(f"Failed to send Discord alert: {response.text}")
