import csv


def writeToCsv(data, fileToWrite):
    csvFile = open(fileToWrite, 'w', encoding="UTF8", newline='')
    csvWriter = csv.writer(csvFile)

    for row in data:
        csvWriter.writerow(row)

    csvFile.close()
