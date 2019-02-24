import json
import pickle
import datetime

from multiprocessing.pool import ThreadPool

from trumpytrump import *
from trumpytrump import _file_assets, _dir_export, _file_csv
from trumpytrump.readDict import readDict
from trumpytrump.wordCount import wordCount
from weekly_counter import WeeklyCounter


LIWC_de = _file_assets.format("LIWC_de.dic")

cached_data = None
_cache = True


total_data = {}
total_outlist = {}
categories = []

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


def export_cache(fname, data, total_wc, total_outlist):
	if not _cache: return

	with open(cache_fname(fname), "wb") as stream:
		pickle.dump((data, total_wc, total_outlist), stream)

	return


def get_filenames():
	global filenames
	dir = _dir_export
	# return ["export/export_harnisch_valid_vor.json"]
	return map(lambda x: "".join((dir, x)), filter(lambda x: x.startswith("export") and x.endswith(".json"), os.listdir(dir)))


################################################# CALC


def start():
	global finalDict, catList
	finalDict, catList = readDict(LIWC_de)

	pre = sorted(get_filenames(), reverse=True)
	post = set()
	res_lst = []

	for idx, fname in enumerate(pre, start=0):
		if "_gefiltert.json" in fname:
			post.add(fname)
			pre.remove(fname)

	pool = ThreadPool(processes=len(pre))
	res_lst += [x for x in pool.map(count_data, pre)]
	pool.close()
	pool.join()
	pool.terminate()

	pool = ThreadPool(processes=len(pre))
	res_lst += [x for x in pool.map(count_data, post)]
	pool.close()
	pool.join()
	pool.terminate()

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
		# if multi_division and not articleNum % 25:
		print("thread-{:<3} article #{}".format(multi_division, articleNum))

		res = wordCount(article["content"], finalDict, catList)
		outList, tokens, wc, classified, percClassified = res
		total_wc += classified
		data = set_data(data, article, res)

	return data, total_wc


def count_filtered(fname):
	global total_data, total_wc
	if not total_data:
		total_data, total_wc, total_outlist = get_cached(cache_fname(fn_german))

	outlist = []
	wc = 0

	json_f = json.load(open(fname, mode="rb"))

	filtered_data = {}

	for year in json_f.keys():
		row_idx = 1
		year_data = {}

		for kw, content in json_f[year].items():
			kw_data = []
			for article in content:
				kw_data.append(total_data[article["id"]])
				wc += int(total_data[article["id"]]["wc"])
				outlist += total_data[article["id"]]["outList"]


			year_data[kw] = kw_data

		filtered_data[year] = year_data

	return filtered_data, wc, outlist


def count_data(fname):
	articles = None
	start = datetime.datetime.now()

	is_filtered = False

	print("Thread : {}".format(fname))
	with open(fname, "r") as file:
		if fname.split("/")[-1].endswith("_gefiltert.json"):
			is_filtered = True
		else:
			json_f = json.load(file)

		if not is_filtered:
			try: articles = [x for x in json_f]
			except TypeError: return

			if articles == {}:
				return

		data, total_wc, total_outlist = get_cached(cache_fname(fname))

		if is_filtered:
			data, wc, outlist = count_filtered(fname)
			export_cache(fname, data, wc, outlist)
			export_filtered_csv(data, fname)
			csv_to_excel(csv_fname(fname), cols=[[2, 2, 60]])
			csv_to_excel(csv_fname(fname), cols=[[3, 3, 22]])
		else:
			if not data:
				data, total_wc = multithread(articles[:], total_wc)

				for d in data.values():
					for k, v in d["outList"].items():
						total_outlist[k] = total_outlist.get(k, 0) + v

				export_cache(fname, data, total_wc, total_outlist)

			if fn_german in fname:
				global total_data
				total_data = data

			csvFname = _file_csv.format(fname.split("/")[-1].split(".")[0])
			export_csv(data, csvFname)
			csv_to_excel(csvFname)


	end = datetime.datetime.now()
	diff = end-start

	return "file: {:<40} len: {:<10} time: {}".format(fname.split("/")[-1], len(articles) if articles else 0, diff)


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

	w = WeeklyCounter(total_data)
	w.export()
