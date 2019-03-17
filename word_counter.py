from multiprocessing.pool import ThreadPool
from trumpytrump import *
from trumpytrump import _file_assets, _dir_export, _file_csv, _file_export
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
		self._cache = cache is not None
		self.total_outlist = {}

		# speichere alle möglichen Kategorie-Titel
		global finalDict, catList
		finalDict, catList = readDict(LIWC_de)
		self.category_names = sorted(wordCount("", finalDict, catList)[0].keys())

		return

	def start(self):
		"""
		Bereitet die Dateinamen und die Prozesse vor welche ausgeführt werden.
		Erstellt die benötigten Listen pre und post damit beim errechnen keine Konflikte entstehen, falls benötigte
		Daten nicht vorliegen sollten.

		pre: eine Liste mit den Dateien welche zu erst verrechnet werden sollen.
		post: eine Liste mit den Dateien welche nach pre verrechnet werden.
		res_lst: eine Liste mit den Ergebnissen.
		"""

		self.parse_argv()
		pre = sorted(get_filenames(), reverse=True)
		post = set()
		res_lst = []

		for idx, fname in enumerate(pre, start=0):
			if "_gefiltert.json" in fname:
				# die nach keywords gefilterten Dateien sollen erst zum Schluss verrechnet werden
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
		'''
		Teilt die komplette Artikel-Liste in mehrere Teile auf und errechnet die Häufigkeiten für jeden Teil.

		:param articles: Die gesamte Artikel-Liste.
		:param total_wc: Die gesamte Wortanzahl.
		:return: Gibt das gesamte Resultat und total_wc zurück.
		'''

		def count_multithreaded(vals):
			'''
			Ruft count() mit dem jeweiligen Teil auf.
			'''

			start = vals[0]
			end_incl = vals[1]
			division = vals[2]

			print("---- Counting Divison #{}/{} Started ----".format(division, divisons))

			total_wc = 0
			data = {}
			# ein Ausschnitt/eine bestimmte Menge von Artikeln aus articles
			articles_span = articles[start:end_incl]
			# rufe singlethreaded zum berechnen der Häufigkeiten der Artikel des Ausschnitts auf
			data, total_wc = self.count(articles_span, total_wc, multi_division=division)

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

		# summiere die Ergebnisse der einzelnen Teile
		data = {}
		for r in res:
			for title, v in r[0].items():
				data[title] = v

			total_wc += r[1]

		return data, total_wc

	def count(self, articles, total_wc, multi_division=None):
		'''
		Durchläuft alle gegebenen Artikel und ruft für jeden Artikel wordCount auf.

		:param articles: Eine Liste mit den Artikeln.
		:param total_wc: Die gesamte Wort-Anzahl.
		:param multi_division: Die Nummer der Teilung.
		:return: Gibt einen tuple mit dem Resultat für die Artikel und die gesamte Wort-Anzahl zurück.
		'''

		data = {}
		l = len(articles)

		for articleNum, article in enumerate(articles):
			if multi_division and not articleNum % 25:
				print("Thread-{:<3} article {}/{}".format(multi_division, articleNum, l), flush=True)

			# rufe wordCount auf
			res = wordCount(article["content"], finalDict, catList)
			outList, tokens, wc, classified, percClassified = res
			total_wc += classified
			data = self.set_data(data, article, res)

		return data, total_wc

	def count_data(self, fname):
		'''
		Erwartet einen Dateinamen einer JSON-Datei. Diese wird dann gelesen und je nachdem welche Datei gegeben ist,
		wird diese analysiert. Sollte für die gegebene Datei bereits eine cache Datei vorhanden sein, so wird die
		cache-Datei verwendet. In der cache-Datei sind die relativen Häufigkeiten und Werte bereitsberechnet und müssen
		nur noch eingelesen werden. Sollte dies jedoch nicht der Fall sein, so wird, nachdem das Resultat - sprich die
		relativen Häufigkeiten - errechnet wurde die cache Datei erstellt.
		Zudem wird auch das Erstellen der CSV- und Excel-Dateien hier initiiert.

		:param fname: Der Dateiname einer JSON Datei.
		:return:
		'''

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

			# lade, falls vorhanden, die cache-Dateien ein
			data, total_wc, total_outlist = get_cached(cache_fname(fname))

			# Fallunterscheidung: wenn die aktuelle Datei nach Jahr und Keyword gefiltert ist so nutze count_filtered()
			# im die Häufigkeiten zu erhalten.
			# ansonsten: nutze multithread()
			if is_filtered:
				data, wc, outlist = self.count_filtered(fname)

				export_cache(fname, data, wc, outlist)
				export_filtered_csv(data, fname, category_names=self.category_names)
				csv_to_excel(csv_fname(fname), cols=[[2, 2, 60]])
			else:
				# Fallunterscheidung:
				# wenn eine cache-Datei vorhanden ist und damit data nicht None ist werden die Werte einfach eingelesen
				# ansonsten: berechne die Häufigkeiten mit multithread() und schreibe anschließend das Ergebnis in die cache
				if not data:
					data, total_wc = self.multithread(articles[:], total_wc)

					# summieren der einzelnen Häufigkeiten
					for d in data.values():
						for k, v in d["outList"].items():
							total_outlist[k] = total_outlist.get(k, 0) + v

					# relativiere die absoluten Häufigkeiten
					for k, v in data.items():
						wc = int(v["wc"])
						for cat, count in v["outList"].items():
							count = int(count)
							data[k]["outList"][cat] = 100 * (count / wc)

					export_cache(fname, data, total_wc, total_outlist)

				if fn_german in fname:
					# speichere das Ergebnis mit allen deutschen Arikeln ab
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
		'''
		Eine gesonderte Zähl-Funktion für die Artikel, welche nach Keyword pro Jahr gefiltert sind.
		Wird benötigt, da das Format der JSON-Datei für die gefilterten Artikel nicht wie das der restlichen JSONs ist
		und dadurch anders behandelt wird.

		:param fname: Der Dateipfad der gefilterten JSON.
		:return: Gibt die Häufigkeiten für die gefilterten Artikel zurück.
		'''

		global total_data, total_wc
		if not total_data:
			# sollte die Variable für alle deutschen Arikel nicht gegeben sein, ließ die Werte aus der cache-Datei ein
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

	def parse_argv(self):
		def rm_cache():
			if os.path.exists(fn_german):
				for file in os.listdir(_dir_export):
					path = _file_export.format(file)
					if path.endswith(".pkl"):
						os.remove(path)
			return

		if "-recount" in argv:
			rm_cache()

		return


################################################# I/O


def get_csv(fname):
	'''
	Ließt den Inhalt einer gegebenen CSV-Datei.
	:param fname: Der Datei-Pfad der CSV-Datei.
	:return: Gibt den Inhalt der CSV zurück.
	'''

	with open(fname, "r") as csv_file:
		reader = csv.reader(csv_file, delimiter=DELIM, quotechar=QUOTE)
		return reader


def export_cache(fname, data, total_wc, total_outlist):
	'''
	Speichert eine cache ab.

	:param fname: Der Dateiname der originalen Datei.
	:param data: Der Inhalt welcher gespeichert werden soll.
	:param total_wc: Die gesamte Anzahl aller Wörter.
	:param total_outlist: Die Anzahl der Wörter für jede Kategorie.
	'''

	if not _cache: return

	with open(cache_fname(fname), "wb") as stream:
		pickle.dump((data, total_wc, total_outlist), stream)

	return


def get_filenames():
	global filenames
	dir = _dir_export
	return map(lambda x: "".join((dir, x)),
			   filter(lambda x: x.startswith("export") and x.endswith(".json"), os.listdir(dir)))


if __name__ == '__main__':
	counter = WordCounter()
	res = counter.start()

	w = WeeklyCounter(total_data)
	w.start()
	w.export()

	for r in res: print(r)
