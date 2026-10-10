import csv


def writeToCsv(data, fileToWrite):
    csvFile = open(fileToWrite, 'w', encoding="UTF8", newline='')
    ## LOAD DATA default line terminator is \n, csv default is \r\n
    csvWriter = csv.writer(csvFile, lineterminator='\n')

    for row in data:
        csvWriter.writerow(row)

    csvFile.close()
