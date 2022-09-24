import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(filename="log/logfile.log", level=logging.INFO)

errorRecipient = []

errorRecipient.append(os.getenv("RECIPIENT"))
errorRecipientSTR = os.getenv("RECIPIENT")
envSender = os.getenv("SENDER")
envSMTP = os.getenv("SMTP")

def errorMail(err):
   sender = envSender
   recipients = errorRecipient

   message = MIMEText( f"Hiba - ellenőrizd a logot: \n {err}")

   message['From'] = envSender
   message['To'] = errorRecipientSTR
   message['Subject'] = 'FB- MySQL hiba'

   try:
      smtpObj = smtplib.SMTP(envSMTP)
      smtpObj.sendmail(sender, recipients, message.as_string())
   except smtplib.SMTPException as e:
      logging.info(" " + datetime.now().strftime('%Y.%m.%d %H:%M:%S') + " cannot send email: " + f"{e}")
