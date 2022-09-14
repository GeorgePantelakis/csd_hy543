from glob import glob
from os import stat

parsed_data = dict()

for payingTypeDir in glob(f'./plotData/payingType*.data'):
		for part in glob(f'{payingTypeDir}/part-*'):
			if stat(part).st_size != 0:
				with open(part, 'r') as partfp:
					for line in partfp:
						if line != '\n':
							fields = line.split('\t')
							if int(fields[0]) < 2014 or int(fields[0]) > 2020:
								continue
							if int(fields[0]) not in parsed_data:
								parsed_data[int(fields[0])] = dict()
							if fields[1] not in parsed_data[int(fields[0])]:
								parsed_data[int(fields[0])][fields[1]] = [0, 0]
							parsed_data[int(fields[0])][fields[1]][0] += int(fields[2])
							parsed_data[int(fields[0])][fields[1]][1] += int(fields[3])

for year, month_data in parsed_data.items():
	with open(f'./plotDataNew/payingTypeYear{year}.dat', 'w') as fp:
		for month in ('JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER'):
			if month not in parsed_data[year]:
				fp.write(f'{month} 0 0\n')
			else:
				fp.write(f'{month} {parsed_data[year][month][0]} {parsed_data[year][month][1]}\n')