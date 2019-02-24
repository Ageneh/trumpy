import os
import csv


DELIM = ";"
QUOTE = '"'
DIVISION_LEN = 1000
DIVISION_THRESHOLD = 10000

csv_header = None


_dir_root = "trumpytrump"
_dir_assets = "{}/".format("assets")
_dir_export = "{}/".format("export")
_dir_csv = "{}{}/".format(_dir_export, "csv")

_file_root = "{}/{{}}".format(_dir_root)
_file_assets = "{}{{}}".format(_dir_assets)
_file_export = "{}{{}}".format(_dir_export)

_file_csv = _file_export.format("csv/{}_output.csv")
_file_csv_total = _file_export.format("csv/total_output.csv")

_cached_data_fn = "cache_data.pkl"
_csv_fn = ".csv"


# dateinamen
base_filename = _file_assets.format("{}_{{}}.json".format("export_harnisch"))
base_filename_exp = _file_export.format("{}_{{}}.json".format("export_harnisch"))
_filename = base_filename.format("valid")
fn_german = base_filename_exp.format("valid_deutsch")
fn_german_pre = base_filename_exp.format("valid_vor")
fn_german_post = base_filename_exp.format("valid_nach")
fn_german_post_filtered = base_filename_exp.format("valid_nach_gefiltert")


suffix_filtered = "_gefiltert"


def cache_fname(fname):
	dir = ""
	if _dir_export in fname:
		spl = fname.split("/")
		dir = "/".join(spl[:-1])
		fname = spl[-1]

	if "." in fname:
		fname = fname.split(".")[0]

	return "{}{}_{}".format(_dir_export, fname, _cached_data_fn)


def csv_fname(fname):
	dir = ""
	if _dir_export in fname:
		spl = fname.split("/")
		dir = "/".join(spl[:-1])
		fname = spl[-1]

	if "." in fname:
		fname = fname.split(".")[0]

	return _file_csv.format(fname)


def filename(path, suffix=False):
	if suffix:
		return path.split("/")[-1]
	else:
		return path.split("/")[-1].split(".")[0]


def check_dir(dir):
	if os.path.isdir(dir): return
	os.mkdir(dir)

	return



################################################# CSV

def export_csv(data, filename):
	global csv_header

	check_dir(_dir_csv)
	num = 1

	try:
		categories = [c for c in sorted(data[list(data.keys())[0]]["outList"].keys())]
		category_names = categories
	except IndexError as e:
		pass

	with open(filename, mode="w", encoding="utf-8") as file:
		writer = csv.writer(file, delimiter=DELIM, quotechar=QUOTE)

		if not csv_header:
			csv_header = ["#", "title", "publishDate", "wordcount"] + category_names
		writer.writerow(csv_header)

		sorted_data = sorted(data.items(), key=lambda x: x[1]["publishDate"])

		for id, content in sorted_data:
			wc = data[id]["wc"]

			line = [num, content["title"], content["publishDate"], content["wc"]]
			categories = [100 * (content["outList"][c] / wc) for c in sorted(category_names)]

			line += list(categories)
			writer.writerow(line)

			num += 1

	return


def export_filtered_csv(data, filename):

	with open(csv_fname(filename), "w") as csv_file:
		writer = csv.writer(csv_file, delimiter=DELIM, quotechar=QUOTE)

		header = csv_header
		header.remove(header[0])
		header.insert(0, "year")
		header.insert(1, "keyword")
		writer.writerow(header)

		for year in data.keys():
			row_idx = 1
			for kw, content in data[year].items():

				for article in sorted(content, key=lambda x: x["publishDate"]):
					title = article["title"]
					date = article["publishDate"]
					wc = article["wc"]
					line = [year, kw, title, date, wc]
					line += list(map(lambda x: x[1], sorted(article["outList"].items(), key=lambda x: x[0])))

					writer.writerow(line)

	return


################################################# EXCEL


def csv_to_excel(filename, cols=None):
	from xlsxwriter.workbook import Workbook

	xlsx_fname = "{}.xlsx".format(filename.split(".")[0])
	print("CSV:", filename, ", XLSX:", xlsx_fname)

	workbook = Workbook(xlsx_fname, {'strings_to_numbers': True, 'constant_memory': True})
	worksheet = workbook.add_worksheet(name="Data")

	if not cols:
		worksheet.set_column(1, 1, 60)
		worksheet.set_column(2, 2, 22)
	else:
		for col in cols:
			worksheet.set_column(col[0], col[1], col[2])

	with open(filename, mode='r', encoding="utf-8") as csv_file:
		r = csv.reader(csv_file, delimiter=DELIM)

		for row_index, row in enumerate(r):
			for col_index, data in enumerate(row):
				worksheet.write(row_index, col_index, data)

	workbook.close()
	print("-------------------------------------------")
	print("--- .CSV to .XLSX Conversion Successful ---")
	print("-------------------------------------------\n")

	return