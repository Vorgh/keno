import csv
import os
from xsorted import xsorter

HEADER_FIELDS = ['numbers', 'frequency']
HEADER = ';'.join(HEADER_FIELDS)
DAY_LIMIT = 99999


def process_row(current_row):
    results = []
    for a in range(0, 15):
        for b in range(a + 1, 16):
            for c in range(b + 1, 17):
                for d in range(c + 1, 18):
                    for e in range(d + 1, 19):
                        for f in range(e + 1, 20):
                            results.append(
                                "{},{},{},{},{},{}".format(current_row[a], current_row[b], current_row[c],
                                                           current_row[d], current_row[e], current_row[f]))

    return results


build_input = False
result_dict = {}

if build_input:
    print("Building dictionary...")

    with (open('keno.csv', "r")):
        input_csv = csv.reader(open('keno.csv'), delimiter=';')
        with (open("data_dump.csv", "w")) as data_dump:
            # Write header
            print(HEADER, file=data_dump)

            for row_number, row in enumerate(input_csv):
                if row_number > DAY_LIMIT:
                    break

                if row_number > 0 and row_number % 100 == 0:
                    for k, v in result_dict.items():
                        print("{};{}".format(k, v), file=data_dump)
                    result_dict.clear()
                    print("{} rows processed".format(row_number))

                for result in process_row(row[4:]):
                    if result not in result_dict:
                        result_dict[result] = 1
                    else:
                        result_dict[result] += 1

            for k, v in result_dict.items():
                print("{};{}".format(k, v), file=data_dump)
            result_dict.clear()
else:
    print("Skipping input csv building...")


print("Sorting results by key...")
with open("data_dump.csv", "r") as data_dump:
    reader = csv.DictReader(data_dump, delimiter=';')
    xsorter_custom = xsorter(partition_size=131072)
    sorted_items = xsorter_custom(reader, key=lambda f: f['numbers'])
    with open("data_dump_sorted.csv", "w") as data_dump_sorted:
        writer = csv.DictWriter(data_dump_sorted, reader.fieldnames, delimiter=';', lineterminator='\n')
        writer.writeheader()
        for item in sorted_items:
            writer.writerow(item)

print("Merging results...")

# Delete previous merged file if exists
if os.path.exists("merged_data.csv"):
    os.remove("merged_data.csv")

with open("data_dump_sorted.csv") as data_dump_sorted:
    data_dump_reader = csv.DictReader(data_dump_sorted, delimiter=";")
    with open("merged_data.csv", "w") as merged_data:
        merged_results_writer = csv.DictWriter(merged_data, data_dump_reader.fieldnames, delimiter=";", lineterminator='\n')
        merged_results_writer.writeheader()

        unique_key_counter = 0
        dict_for_chunks = {}

        for row_number, row in enumerate(data_dump_reader):
            key = row["numbers"]
            value = int(row["frequency"])
            if key not in dict_for_chunks:
                if unique_key_counter > 100:
                    merged_results_writer.writerows({'numbers': k, 'frequency': v} for k, v in dict_for_chunks.items())
                    unique_key_counter = 0
                    dict_for_chunks.clear()

                unique_key_counter += 1
                dict_for_chunks[key] = value
            else:
                dict_for_chunks[key] += value

        if len(dict_for_chunks) != 0:
            merged_results_writer.writerows({'numbers': k, 'frequency': v} for k, v in dict_for_chunks.items())

print("Sorting results by value...")
with open("merged_data.csv", "r") as merged_data:
    reader = csv.DictReader(merged_data, delimiter=';')
    xsorter_custom = xsorter(partition_size=131072)
    sorted_items = xsorter_custom(reader, key=lambda f: f['frequency'], reverse=True)
    with open("merged_data_sorted.csv", "w") as merged_data_sorted:
        writer = csv.DictWriter(merged_data_sorted, reader.fieldnames, delimiter=';', lineterminator='\n')
        writer.writeheader()
        for item in sorted_items:
            writer.writerow(item)

with open("merged_data_sorted.csv", "r") as final_result:
    reader = csv.DictReader(final_result, delimiter=';')
    for i, row in enumerate(reader):
        print("{} : {}".format(row['numbers'], row['frequency']))
        if i > 10:
            break


