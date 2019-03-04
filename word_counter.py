import pickle

from multiprocessing.pool import ThreadPool

from trumpytrump import *
from trumpytrump import _file_assets, _dir_export, _file_csv
from trumpytrump.readDict import readDict
from trumpytrump.wordCount import wordCount
from weekly_counter import WeeklyCounter


cached_data = None
_cache = True


total_data = {}
total_outlist = {}
categories = []

finalDict, catList = None, None

class WordCounter:

	def __init__(self, cache=True):
		self.total_data = {}
		self._cache = True
		self.total_outlist = {}
		self.category_names = []
		return

	def start(self):
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
		res_lst += [x for x in pool.map(self.count_data, pre)]
		pool.close()
		pool.join()
		pool.terminate()

		pool = ThreadPool(processes=len(post))
		res_lst += [x for x in pool.map(self.count_data, post)]
		pool.close()
		pool.join()
		pool.terminate()

		return res_lst

	def set_data(self, data, article, wordCount):
		outList, tokens, wc, classified, percClassified = wordCount

		data[article["title"]] = {
			"title": article["title"],
			"outList": outList,
			"publishDate": article["publishDate"],
			"wc": wc,
			"classified": classified,
			"percClassified": percClassified
		}

		return data

	def multithread(self, articles, total_wc):

		def count_multithreaded(vals):
			start = vals[0]
			end_incl = vals[1]
			division = vals[2]

			print("---- Counting Divison #{}/{} Started ----".format(division, divisons))

			total_wc = 0
			data = {}
			articles_span = articles[start:end_incl]
			data, total_wc = self.singlethreaded(articles_span, total_wc, multi_division=division)

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

		for div in range(divisons + 1):
			args.append([start, end, div])

			start = end
			end += DIVISION_LEN
			end = length if end >= length else end

		pool = ThreadPool(processes=divisons + 1)
		res = pool.map(count_multithreaded, args)
		pool.close()
		pool.join()

		data = {}
		for r in res:
			for title, v in r[0].items():
				data[title] = v

			total_wc += r[1]

		return data, total_wc

	def singlethreaded(self, articles, total_wc, multi_division=None):
		data = {}
		for articleNum, article in enumerate(articles):
			if multi_division and not articleNum % 25:
				print("thread-{:<3} article #{}".format(multi_division, articleNum))

			res = wordCount(article["content"], finalDict, catList)
			outList, tokens, wc, classified, percClassified = res
			total_wc += classified
			data = self.set_data(data, article, res)

		return data, total_wc

	def count_data(self, fname):
		from trumpytrump import category_names

		articles = None
		start = datetime.now()

		is_filtered = False

		print("Thread : {}".format(fname))
		with open(fname, "r") as file:
			if fname.split("/")[-1].endswith("_gefiltert.json"):
				is_filtered = True
			else:
				json_f = json.load(file)

				try:
					articles = [x for x in json_f]
				except TypeError:
					return

				if articles == {}:
					return

			data, total_wc, total_outlist = get_cached(cache_fname(fname))

			if not self.category_names:
				self.category_names = sorted(total_outlist.keys())

			if is_filtered:
				data, wc, outlist = self.count_filtered(fname)
				export_cache(fname, data, wc, outlist)
				export_filtered_csv(data, fname, category_names=self.category_names)
				csv_to_excel(csv_fname(fname), cols=[[2, 2, 60]])
			else:
				if not data:
					data, total_wc = self.multithread(articles[:], total_wc)

					for d in data.values():
						for k, v in d["outList"].items():
							total_outlist[k] = total_outlist.get(k, 0) + v

					for k, v in data.items():
						wc = int(v["wc"])
						for cat, count in v["outList"].items():
							count = int(count)
							data[k]["outList"][cat] = 100 * (count / wc)

					export_cache(fname, data, total_wc, total_outlist)

				if fn_german in fname:
					global total_data
					self.total_data = data
					self.total_outlist = total_outlist

				csvFname = _file_csv.format(fname.split("/")[-1].split(".")[0])
				export_csv(data, csvFname, category_names=self.category_names)
				csv_to_excel(csvFname)

		end = datetime.now()
		diff = end - start

		return "file: {:<40} len: {:<10} time: {}".format(fname.split("/")[-1], len(articles) if articles else 0, diff)

	def count_filtered(self, fname):
		global total_data, total_wc
		if not total_data:
			total_data, total_wc, total_outlist = get_cached(cache_fname(fn_german))

		outlist = []
		wc = 0

		json_f = json.load(open(fname, mode="rb"))

		filtered_data = {}

		for year in json_f.keys():
			year_data = {}

			for kw, content in json_f[year].items():
				kw_data = []
				for article in content:
					kw_data.append(total_data[article["title"]])
					wc += int(total_data[article["title"]]["wc"])
					outlist += total_data[article["title"]]["outList"]

				year_data[kw] = kw_data

			filtered_data[year] = year_data

		return filtered_data, wc, outlist

	def count_rel(self, total_wc, outlist=None):
		if not outlist:
			outlist = self.total_outlist

		string = []
		for k, v in list(sorted(outlist.items(), key=lambda x: x[0])):
			string.append("{},{}%".format(k, str(100 * (v / total_wc))))

		string.append("{},{}".format("wordcount", total_wc))
		return "\n".join(string)


################################################# I/O


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
	return map(lambda x: "".join((dir, x)), filter(lambda x: x.startswith("export") and x.endswith(".json"), os.listdir(dir)))


if __name__ == '__main__':
	counter = WordCounter()
	res = counter.start()

	w = WeeklyCounter(total_data)
	w.start()
	w.export()

	for r in res: print(r)
