import threading
import datetime

import csv
from xlsxwriter.workbook import Workbook
import json

from trumpytrump import _dir_export, _file_export

filename1 = _file_export.format("export_harnisch_valid_vor.json")
filename2 = _file_export.format("export_harnisch_valid_nach.json")

def write(filename):
	xlsx_fname = "{}.xlsx".format(filename.split(".")[0])
	print("CSV:", filename, ", XLSX:", xlsx_fname)

	workbook = Workbook(xlsx_fname, {'strings_to_numbers': True, 'constant_memory': True})
	worksheet = workbook.add_worksheet()

	with open(filename, mode='r', encoding="utf-8") as f:
		data = json.load(f)

		keys = [x for x in data[0].keys()]
		for col, key in enumerate(keys):
			worksheet.write(0, col, key)

		for row, ele in enumerate(data, start=1):
			for col, key in enumerate(keys):
				worksheet.write(row, col, ele[key])


	workbook.close()


def write1(filename, *args):
	write(filename)
	print("1 fertig")


def write2(filename, *args):
	write(filename)
	print("2 fertig")

t1 = threading.Thread(target=write1, args=[filename1])
t2 = threading.Thread(target=write2, args=[filename2])

t1.daemon = True
t2.daemon = True

t1.start()
t2.start()

t1.join()
t2.join()