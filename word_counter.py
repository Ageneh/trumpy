import json
import pickle
import threading
import csv
import datetime
import os

from multiprocessing.pool import ThreadPool

from trumpytrump import fn_german, fn_german_post_filtered
from trumpytrump import _file_assets, _dir_export, _cached_data_fn, _file_csv_total, _file_csv, _dir_csv
from trumpytrump.readDict import readDict
from trumpytrump.wordCount import wordCount


LIWC_de = _file_assets.format("LIWC_de.dic")

cached_data = None
_cache = True


total_wc = 0
data = {}
total_data = {}
total_outlist = {}

DELIM = ";"
QUOTE = '"'
DIVISION_LEN = 1000
DIVISION_THRESHOLD = 10000

finalDict, catList = None, None


################################################# I/O


def get_cached(fname):
	if _cache and os.path.exists(fname):
		with open(fname, mode="rb") as stream:
			cached_data, total_wc, total_outlist = pickle.load(stream)
			return cached_data, total_wc, total_outlist
	else:
		cached_data = None
		return None, 0, {}


def get_csv(fname):

	with open(fname, "r") as csv_file:
		reader = csv.reader(csv_file, delimiter=DELIM, quotechar=QUOTE)
		return reader


	return None


def export(content, filename):
	import os
	import datetime

	if not os.path.exists("export/output/") or not os.path.isdir("export/output/"):
		os.makedirs("export/output/")

	filename = filename.split(".")

	with open("{}_{}.{}".format(filename[0],
								 str(datetime.datetime.now()),
								 filename[-1]), "w+") as outfile:
		for data in content:
			outfile.write("{},{}".format(data[0], data[1]))
			outfile.write("\n")

	return filename


def export_cache(fname):
	if not _cache: return

	with open(cache_fname(fname), "wb") as stream:
		pickle.dump((data, total_wc, total_outlist), stream)

	return


def get_filenames():
	global filenames
	dir = _dir_export
	# return ["export/export_harnisch_valid_vor.json"]
	return map(lambda x: "".join((dir, x)), filter(lambda x: x.startswith("export") and x.endswith(".json"), os.listdir(dir)))


def cache_fname(fname):
	dir = ""
	if _dir_export in fname:
		spl = fname.split("/")
		dir = "/".join(spl[:-1])
		fname = spl[-1]

	if "." in fname:
		fname = fname.split(".")[0]

	return "{}{}_{}".format(_dir_export, fname, _cached_data_fn)


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
	check_dir(_dir_csv)

	num = 1
	category_names = [c for c in sorted(data[list(data.keys())[0]]["outList"].keys())]

	with open(filename, mode="w", encoding="utf-8") as file:
		writer = csv.writer(file, delimiter=DELIM)
		writer.writerow(["#", "title", "publishDate", "wordcount"] + category_names)

		sorted_data = sorted(data.items(), key=lambda x: x[1]["publishDate"])

		for id, content in sorted_data:
			wc = data[id]["wc"]

			line = [num, content["title"], content["publishDate"], content["wc"]]
			categories = [100 * (content["outList"][c]/wc) for c in sorted(category_names)]

			line += list(categories)
			writer.writerow(line)

			num += 1

	return


################################################# EXCEL


def csv_to_excel(filename):
	from xlsxwriter.workbook import Workbook

	xlsx_fname = "{}.xlsx".format(filename.split(".")[0])
	print("CSV:", filename, ", XLSX:", xlsx_fname)

	workbook = Workbook(xlsx_fname, {'strings_to_numbers': True, 'constant_memory': True})
	worksheet = workbook.add_worksheet(name="Data")

	worksheet.set_column(1, 1, 60)
	worksheet.set_column(2, 2, 22)

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


################################################# CALC


def start():
	global finalDict, catList
	finalDict, catList = readDict(LIWC_de)

	fnames = sorted(get_filenames(), reverse=True)
	post = set()
	res_lst = []

	for idx, fname in enumerate(fnames, start=0):
		if "_gefiltert.json" in fname:
			post.add(fname)
			fnames.remove(fname)

	for lst in (fnames, post)[::-1]:
		pool = ThreadPool(processes=len(lst))
		res_lst += list(pool.map(count_data, lst))
		pool.close()
		pool.join()

	return res_lst


def set_data(data, article, wordCount):
	outList, tokens, wc, classified, percClassified = wordCount

	data[article["id"]] = {
		"title": article["title"],
		"outList": outList,
		"publishDate": article["publishDate"],
		"wc": wc,
		"classified": classified,
		"percClassified": percClassified
	}

	return data


def multithread(articles, total_wc):

	def count_multithreaded(vals):
		start = vals[0]
		end_incl = vals[1]
		division = vals[2]

		print("---- Counting Divison #{}/{} Started ----".format(division, divisons))

		total_wc = 0
		data = {}
		articles_span = articles[start:end_incl]
		data, total_wc = singlethreaded(articles_span, total_wc, multi_division=division)

		print("Divison #{}".format(division))

		msg = "---- Counting Divison #{} Successful ----".format(division)

		print("-" * len(msg))
		print(msg)
		print("-" * len(msg))

		return data, total_wc

	args = []
	divisons = int(len(articles) / DIVISION_LEN)
	length = len(articles)
	start = 0
	end = DIVISION_LEN

	for div in range(divisons+1):
		args.append([start, end, div])

		start = end
		end += DIVISION_LEN
		end = length if end >= length else end

	pool = ThreadPool(processes=divisons+1)
	res = pool.map(count_multithreaded, args)
	pool.close()
	pool.join()


	data = {}
	for r in res:
		for title, v in r[0].items():
			data[title] = v

		total_wc += r[1]

	return data, total_wc


def singlethreaded(articles, total_wc, multi_division=None):
	data = {}
	for articleNum, article in enumerate(articles):
		if multi_division and not articleNum % 25:
			print("thread-{:<3} article #{}".format(multi_division, articleNum))

		res = wordCount(article["content"], finalDict, catList)
		outList, tokens, wc, classified, percClassified = res
		total_wc += classified
		data = set_data(data, article, res)

	return data, total_wc


def count_filtered(json_f, fname):
	cached_data, total_wc, total_outlist = get_cached(cache_fname(fn_german))
	csv = get_csv(_file_csv.format(filename(fn_german_post_filtered)))

	for year, keywords in json_f.items():
		for keyword, files in keywords.items():
			print(keyword)

	return data, total_wc


def count_data(fname):
	start = datetime.datetime.now()

	is_filtered = False

	print("Thread : {}".format(fname))
	with open(fname, "r") as file:
		json_f = json.load(file)

		if fname.split("/")[-1].endswith("_gefiltert.json"):
			is_filtered = True
		else:
			try: articles = [x for x in json_f]
			except TypeError: return

			if articles == {}: return

		data, total_wc, total_outlist = get_cached(cache_fname(fname))

		if is_filtered:
			data, total_wc = count_filtered(json_f, fname)
		else:
			if not data:
				data, total_wc = multithread(articles[:], total_wc)

				for d in data.values():
					for k, v in d["outList"].items():
						total_outlist[k] = total_outlist.get(k, 0) + v

				export_cache(fname)

		csv_fname = _file_csv.format(fname.split("/")[-1].split(".")[0])
		export_csv(data, csv_fname)
		csv_to_excel(csv_fname)


	end = datetime.datetime.now()
	diff = end-start

	return "file: {:<40} len: {:<10} time: {}".format(fname.split("/")[-1], len(articles), diff)


def count_rel(total_wc, outlist=total_outlist):
	string = []
	for k, v in list(sorted(outlist.items(), key=lambda x: x[0])):
		string.append("{},{}%".format(k, str(100 * (v / total_wc))))

	string.append("{},{}".format("wordcount", total_wc))
	return "\n".join(string)


if __name__ == '__main__':
	res = start()

	for r in res:
		print(r)
