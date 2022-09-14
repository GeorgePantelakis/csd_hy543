from glob import glob
from os import stat

parsed_data = dict()

for profitableAreasDir in glob(f'./plotData/profitableAreas*.data'):
	for part in glob(f'{profitableAreasDir}/part-*'):
		if stat(part).st_size != 0:
			with open(part, 'r') as partfp:
				for line in partfp:
					if line != '\n':
						fields = line.split('\t')
						if int(fields[0]) not in parsed_data:
							parsed_data[int(fields[0])] = 0.0
						parsed_data[int(fields[0])] += float(fields[1])

parsed_data = dict(sorted(parsed_data.items(), key=lambda item: item[1], reverse=True))
with open(f'./plotDataNew/profitableAreas.dat', 'w') as fp:
	fp.write('\n'.join(f'{locationID} {profit}' for locationID, profit in parsed_data.items()))