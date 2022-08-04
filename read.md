# Fridrich Ferenc

## Lazarteam - Laurel firebird sql to MySql

---

## Description:

This application stand for copy daily sum data to BI sql system
Written in python.

---

### **How to use:**

Use with cron daily run.
If we have missing data, we can run with specific dates arguments:
python python-fb-mysql.py YYYY.MM.dd YYYY.MM.dd
first argument is start date the second is the end date.
Start date cannot be later than end date.
Use correct date format, or it will raise error.

---

#### **Requirements:**

fdb==2.0.2
mysql_connector_repackaged==0.3.1

---

### **Files and folders:**

- python-fb-mysql.py
- Readme.md - you are here
- reqirement.txt

---

### **Database:**

- Laurel : 192.168.103.51 - D:\Program Files\Laurel Kft\AIR Application\database\ibukr.gdb (windows)
- MySQL BI: 192.168.103.101 (linux)

---
