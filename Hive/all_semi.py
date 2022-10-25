from datetime import datetime

new_file = open("parking_violations_new.csv","w")
index_count = 0
text_file = open("Parking_Violations_Issued_-_Fiscal_Year_2017.csv", "r")
for line in text_file:
    item_list = line.replace("\n","").split(";")
    if index_count == 0:
        index_count += 1
        continue
    string_date = item_list[4]
    try:
        new_date = datetime.strptime(string_date, '%m/%d/%Y')
        new_date = new_date.date()
        item_list[4] = str(new_date)
    except Exception:
        item_list[4] = "2016-11-14"
    new_file.write(";".join(item_list))
    new_file.write("\n")
