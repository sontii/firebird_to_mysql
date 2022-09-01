# Fridrich Ferenc

## Lazarteam - Laurel firebird sql to MySql

---

## Description:

This application stand for copy daily data to BI sql system.
Written in python.

---

### **How to use:**

Use with cron, daily run.
If we have missing data, we can run with specific date arguments:
python python-fb-mysql.py arg1 arg2 format(YYYY.MM.dd)
first argument is start date the second is the end date.
Start date cannot be later than end date.
Use correct date format, or it will raise error.

---

#### **Requirements:**

For install reuieqd packeges: pip3 install -r requirements.txt

- fdb==2.0.2
- mysql_connector_repackaged==0.3.1
- python-dotenv==0.20.0

---

### **Files and folders:**

- python-fb-mysql.py
- Readme.md - you are here
- reqirement.txt
- log
- .env
- .gitignore
- temp.csv

---

### **Database:**

- at .env file

---
