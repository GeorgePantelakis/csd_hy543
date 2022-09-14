from glob import glob
from os import stat

parsed_data = {'MONDAY': dict.fromkeys(range(0, 24), 0),
		'TUESDAY': dict.fromkeys(range(0, 24), 0),
		'WEDNESDAY': dict.fromkeys(range(0, 24), 0),
		'THURSDAY': dict.fromkeys(range(0, 24), 0),
		'FRIDAY': dict.fromkeys(range(0, 24), 0),
		'SATURDAY': dict.fromkeys(range(0, 24), 0),
		'SUNDAY': dict.fromkeys(range(0, 24), 0)}

for busyHoursDir in glob(f'./plotData/busyHours*.data'):
	for part in glob(f'{busyHoursDir}/part-*'):
		if stat(part).st_size != 0:
			with open(part, 'r') as partfp:
				for line in partfp:
					if line != '\n':
						fields = line.split('\t')
						nums = fields[1].split(' ')
						parsed_data[fields[0]][int(nums[0])] += int(nums[1].strip())

for day, hour_data in parsed_data.items():
	with open(f'./plotDataNew/busyHoursOn{day}.dat', 'w') as fp:
		fp.write('\n'.join(f'{hour} {cnt}' for hour, cnt in hour_data.items()))