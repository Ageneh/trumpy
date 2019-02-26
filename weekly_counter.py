from trumpytrump import *
from trumpytrump import _file_csv


class WeeklyCounter:

	def __init__(self, data):
		from word_counter import WordCounter
		if type(data) == WordCounter:
			data = data.total_data

		self.data = data
		self.filename = _file_csv.format("weekly")

	def start(self):
		self.years = {}

		for id, article in self.data.items():
			date = parser.parse(article["publishDate"]).replace(tzinfo=utc)
			week = date.isocalendar()[1]
			year = date.year

			if date.year not in self.years.keys():
				self.years[year] = {}
				for w in range(1, 53):
					self.years[year][w] = 0

			if week not in self.years[year]:
				self.years[year][week] = 0

			self.years[year][week] += 1

		self.export()
		return

	def export(self):

		with open(self.filename, "w") as file:
			writer = csv.writer(file, delimiter=DELIM, quotechar=QUOTE)
			writer.writerow(["year", "week", "articles"])

			for year, weeks in sorted(self.years.items(), key=lambda x: x[0]):
				for week, count in sorted(weeks.items(), key=lambda x: x[0]):
					writer.writerow([year, week, count])

		self.csv_to_excel()

		return

	def csv_to_excel(self, filename=None, cols=None):
		from xlsxwriter.workbook import Workbook

		if not filename:
			filename = self.filename

		xlsx_fname = "{}.xlsx".format(filename.split(".")[0])
		print("CSV:", filename, ", XLSX:", xlsx_fname)

		workbook = Workbook(xlsx_fname, {'strings_to_numbers': True, 'constant_memory': True})

		for year, weeks in sorted(self.years.items(), key=lambda x: x[0]):
			worksheet = workbook.add_worksheet(name=str(year))

			worksheet.write(0, 0, "week")
			worksheet.write(0, 1, "count")

			for week, count in sorted(weeks.items(), key=lambda x: x[0]):
				worksheet.write(week, 0, week)
				worksheet.write(week, 1, count)

			if not cols:
				worksheet.set_column(1, 1, 10)
				worksheet.set_column(2, 2, 10)
			else:
				for col in cols:
					worksheet.set_column(col[0], col[1], col[2])

		workbook.close()
		print("-------------------------------------------")
		print("--- .CSV to .XLSX Conversion Successful ---")
		print("-------------------------------------------\n")

		return
