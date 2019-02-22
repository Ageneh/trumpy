import trumpytrump

with open("../export/export_harnisch_valid_deutsch_output.csv", "r") as file:
	per_cent = 0.0

	for line in file:
		txt, p_cent = line.replace("%\n", "").split(",")
		if txt == "wordcount":
			break
		per_cent += float(p_cent)

	print(per_cent)