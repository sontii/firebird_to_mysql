import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(filename="log/logfile.log", level=logging.INFO)

recipient = os.getenv("RECIPIENT")
envSender = os.getenv("SENDER")
envSMTP = os.getenv("SMTP")
envSmtpPass = os.getenv("SMTP_PASS")

def errorMail(err):
    body = f"Hiba - ellenőrizd a logot: \n {err}"
    msg = MIMEText(body.encode('utf-8'), "plain", "utf-8")
    msg['Subject'] = 'FB - MySQL hiba'
    msg['From'] = envSender
    msg['To'] = recipient

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login('f.ferenc@lazarteam.hu', envSmtpPass)
            server.sendmail(envSender, recipient, msg.as_string())
        print("Üzenet elküldve!")
    except smtplib.SMTPException as e:
        logging.info(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " Nem sikerült elküldeni a levelet hiba: " + f"{e}")