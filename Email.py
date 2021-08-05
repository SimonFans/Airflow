'''
1. Configure Airflow to be notified by email
<I>
Go to security.google.com/settings/security/apppasswords
Select type: Mail and device : Mac
Keep the app password
Go to airflow.cfg, search for [smtp], then 
smtp_host = smtp.gmail.com, 
smttp_user = <your gmail>, 
smtp_password = <app password>, 
smtp_port= 587,
smtp_mail_from = <your gmail>
Then restart by running docker-compose down && docker-compose up -d 

<II> define these parameters to receive email alert (fail or retry)
email:
email_on_retry: True by default
email_on_failure: True by default

2. Receive an email if a task fails
3. Customize the email alerts sent by Airflow
Go to airflow.cfg, search for [email]
Add subject_template = /opt/airflow/includes/subject_template_email.txt
Add html_content_template = /opt/airflow/includes/content_template_email.txt
Under include folder, create subject_template_email.txt and content_template_email.txt
Inside the subject_template_email.txt, enter:
Airflow alert: {{ti.task_id}}
Inside the content_template_email.txt, enter:
Try {{try_number}} out of {{max_retries + 1}}
Execution date: {{ti.execution_date}}

4. Test your Dag in the docker container of airflow scheduler
docker exec -it <container Id> /bin/bash
airflow tasks test <dag id> <task id> <execution date in the past>
'''

