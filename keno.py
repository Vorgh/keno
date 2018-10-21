import csv
import dask.dataframe as dd


DAY_LIMIT = 3000


def process_row(current_row):
    results = []
    for a in range(0, 16):
        for b in range(a + 1, 17):
            for c in range(b + 1, 18):
                for d in range(c + 1, 19):
                    for e in range(d + 1, 20):
                        results.append(
                            "{},{},{},{},{}".format(current_row[a], current_row[b], current_row[c],
                                                       current_row[d], current_row[e]))

    return results


def select_max(grouped_df):
    row_with_max_cnt_index = grouped_df['frequency'].idmax()
    row_with_max_cnt = grouped_df.loc[row_with_max_cnt_index]
    return row_with_max_cnt


build_input = True
result_dict = {}
input_csv = csv.reader(open('keno.csv'), delimiter=';')

row_counter = 0
values_string_list = []

if build_input:
    with (open("results.csv", "w")) as result_file:
        print("numbers;frequency", file=result_file)
        for row in input_csv:
            row_counter += 1
            if row_counter > DAY_LIMIT:
                result_dict.clear()
                break

            for result in process_row(row[4:]):
                if result not in result_dict:
                    result_dict[result] = 1
                else:
                    result_dict[result] += 1

            if row_counter % 100 == 0:
                for k, v in result_dict.items():
                    print("{};{}".format(k, v), file=result_file)

                print("{} rows processed".format(row_counter))
                result_dict.clear()
else:
    print("Skipping input csv building...")


print("Creating DataFrame...")
df = dd.read_csv("results.csv", sep=";", dtype={'numbers': str, 'frequency': int})
#input_df = pd.read_csv("results.csv", sep=";", names=["numbers", "frequency"])

print("Sorting dataframe...")
grouped_df = df.groupby("numbers").sum().reset_index()
grouped_df.set_index("frequency")
grouped_df = grouped_df.map_partitions(lambda x: x.sort_index())
print(grouped_df.compute())
print(grouped_df.head(10))
print(grouped_df.tail(10))

#sorted_results = [(k, result_dict[k]) for k in sorted(result_dict, key=result_dict.get, reverse=True)]
#for k, v in sorted_results[:10]:
    #print("{} Húzások száma: {}".format(k, v))
