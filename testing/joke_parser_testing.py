import csv
import re
import json
import mysql.connector

conn = mysql.connector.connect(
	host="localhost",
	user="root",
	password="",
	database="RipDB"
)

# Executes a stored procedure. Assumes the last parameter is an output parameter and returns it.
def run_sql_proc(proc_name, params):
	proc = conn.cursor()
	result = proc.callproc(proc_name, params)

	return result[len(result) - 1]

def write_missing_joke(joke_data):
	file = open('missing_jokes.txt', 'a')
	file.write(joke_data)
	file.close()

# Looks for the given search in the list of meta jokes
# Search is not case insensitive so that names of songs that are basic words aren't always caught as false matches. (e.g. "Alone" won't match with "all alone")
# This isn't perfect, but will reduce a few false matches.
def check_joke_data(jokes, search):
	actual_joke = None
	actual_meta_joke = None
	actual_meta = None
	tags = []
	search = search
	for joke in jokes:
		search_items = joke['joke_search'].split('&|')
		matches = 0
		for criteria in search_items:
			if (criteria in search):
				matches += 1
		if matches == len(search_items):
			actual_joke = joke['joke'].strip()
			actual_meta_joke = joke['meta_joke'].strip()
			actual_meta = joke['meta'].strip()
			tags = joke['tags']
				

	if actual_meta == '':
		actual_meta = None
	if actual_meta_joke == '':
		actual_meta_joke = None
	if (len(tags) == 0):
		tags = None

	return actual_joke, actual_meta_joke, actual_meta, tags

# Prepares the joke-related data
def prepare_joke_struct(joke, existing_jokes, data):
	new_joke_name, meta_joke, meta, tags = check_joke_data(existing_jokes, data)
	if joke is None:
		joke = {'name': None, 'timestamps': [], 'meta_joke': None, 'tags': None, 'primary_tag': None}

	if new_joke_name is not None:
		joke['name'] = new_joke_name
		joke['meta_joke_name'] = meta_joke
		joke['meta_name'] = meta
		meta_id = run_sql_proc('usp_InsertMeta', (meta, 0))
		meta_joke = run_sql_proc('usp_InsertMetaJoke', (meta_joke, None, meta_id, 0))
		joke['meta_joke'] = meta_joke
		joke['meta_id'] = meta_id
		joke['primary_tag'] = None
		joke['tags'] = None
		if (tags is not None):
			if (tags[0] != ''):
				tag_id = run_sql_proc('usp_InsertTag', (tags[0], 0))
				joke['primary_tag'] = tag_id
			if (len(tags) > 1):
				joke['tags'] = []
				for tag in tags[1:]:
					tag_id = run_sql_proc('usp_InsertTag', (tag, 0))
					joke['tags'].append(tag_id)

				if len(joke['tags']) == 0:
					joke['tags'] = None

	return joke

# Inserts the given joke into the database, and returns its ID.
def insert_joke(joke):
	# print(json.dumps(joke))
	meta_jokes = None
	if joke['meta_joke'] is not None:
		meta_jokes = json.dumps([joke['meta_joke']])
	tags = None
	if joke['tags'] is not None:
		tags = json.dumps(joke['tags'])

	return run_sql_proc('usp_InsertJoke', (joke['name'], None, joke['primary_tag'], tags, meta_jokes, 0))

def parse_jokes(text_box, meta_file, existing_jokes):
	# Get jokes segment
	joke_ids = {}

	# First check if a jokes table exists. If it does, use it instead.
	matches = re.search(r'==.*Joke', text_box, re.IGNORECASE)
	# if matches is None:
	# 	matches = re.search(r'==.*setlist', text_box, re.IGNORECASE)
	if not matches is None:
		jokes_start = matches.start()
		table_start = text_box.find('{|', jokes_start)

		# If no table:
		if table_start == -1:
			heading_end = text_box.find('==', jokes_start + 3)
			jokes_end = text_box.find('==', heading_end + 3)
			joke_data = text_box[heading_end+3:jokes_end-1].strip()

			joke = prepare_joke_struct(None, existing_jokes, joke_data)
			if joke['name'] is not None:
				joke_id = insert_joke(joke)
				joke_ids[joke_id] = {'timestamps': joke['timestamps'], 'comment': None}
			else:
				print('no joke match!')
				write_missing_joke(joke_data)

		# Else, check the contents of the jokes table
		else:
			table_start = text_box.find('{|');
			table_start = text_box.find('|-', table_start)
			jokes_end = text_box.find('|}\n', table_start)
			joke_data = text_box[table_start+3:jokes_end+2]

			# parse jokes table. Read 4 lines at a time.
			last_start = None
			last_end = None
			skip_timestamp_rows = 0
			invalid_joke = False
			joke = {'name': None, 'timestamps': [], 'meta_joke': None, 'tags': None, 'primary_tag': None}
			debug_prev_line = None

			lineCounter = 0
			for line in joke_data.splitlines():
				# print (line, lineCounter, skip_timestamp_rows)

				# If the line is a header, ignore it.
				if (line[0] != '!'):

					# If end of joke, Add data to list
					if (line == '|-' or line == '|}'):
						if skip_timestamp_rows > 0:
							skip_timestamp_rows -= 1

						# If the joke is not invalid, add it. Disable this if statement to add invalid jokes.
						if not joke['name'] is None and not invalid_joke:
							joke_id = insert_joke(joke)
							joke_ids[joke_id] = {'timestamps': joke['timestamps'], 'comment': None}

						# reset joke
						joke = {'name': None, 'timestamps': [], 'meta_joke': None, 'tags': None, 'primary_tag': None}
						invalid_joke = False
						if (skip_timestamp_rows > 0):
							lineCounter = 1
						else:
							lineCounter = 0
						continue

					# If the first line of a joke, but the timestamp is omitted, set the line counter to the next line to get the joke name
					if lineCounter == 0:
						# if 'rowspan' in line:
						# 	skip_timestamp_rows = int(re.search(r'rowspan?.=?.([0-9])', line).groups()[0])

						if skip_timestamp_rows == 0:
							# print('parsing timestamp:', line, skip_timestamp_rows)
							# If the timestamp has a rowspan, skip a row

							# If the timestamp has a start and end, get it
							if ('-' in line):
								timestamps = re.search(r'(?:[0-9]{0,2}:?[0-9]{0,2}:[0-9]{0,2}).-.(?:[0-9]{0,2}:?[0-9]{0,2}:[0-9]{0,2})', line)
								if (timestamps.group() is not None):
									time = timestamps.group()
									split = time.split('-')
									start = split[0].replace(':', '').strip()
									end = split[1].replace(':', '').strip()
									joke['timestamps'].append({'start': start, 'end': end})
									last_start = start
									last_end = end
							else:
								timestamps = re.search(r'(?:[0-9]{0,2}:?[0-9]{0,2}:[0-9]{0,2})', line)
								last_end = None
								if len(timestamps.groups()) > 0:
									for time in timestamps.groups():
										joke['timestamps'].append({'start': time})
										last_start = time
								else:
									joke['timestamps'].append({'start': timestamps.group()})
									last_start = timestamps.group()
						else:
							if last_start is not None and last_end is None:
								joke['timestamps'].append({'start': last_start})
							elif last_end is not None:
								joke['timestamps'].append({'start': last_start, 'end': last_end})

						if 'rowspan' in line:
							skip_timestamp_rows = int(re.search(r'rowspan?.=?.([0-9])', line).groups()[0])

					# joke name
					elif lineCounter == 1:
						if ('???' in line):
							invalid_joke = True
						line = line[1:].replace("''", '').strip()
						
						# If joke name starts with quotation marks, remove them from start and end.
						if (line[0] == '"'):
							line = line[1:len(line) -1]

						joke_name = None
						matches = re.search(r'\[\[(.*)\]\]', line, re.IGNORECASE)
						if (matches is None):
							joke_name = line.strip()
						else:
							joke_name = matches.groups()[0]

						joke = prepare_joke_struct(joke, existing_jokes, joke_name)

					# Meta joke
					# elif lineCounter == 2:
					# 	line = line[1:].replace("''", '')
					# 	matches = re.search(r'\{\{category\|(.*)\}\}', line, re.IGNORECASE)
					# 	meta_joke = None
					# 	if (matches is None):
					# 		matches = re.search(r'\[\[(.*)\]\]', line, re.IGNORECASE)
					# 		if (matches is None):
					# 			meta_joke = line.strip()
					# 		else:
					# 			meta_joke = matches.groups()[0]
					# 	else:
					# 		meta_joke = matches.groups()[0]

					# 	if (not meta_joke in metas.keys()):
					# 		meta_file.write(joke['name'] + ',' + meta_joke + ",\n")

					# 	if meta_joke is not None and meta_joke != '???':
					# 		meta_joke, meta = check_joke_data(existing_jokes, meta_joke)
							
					# 		print(meta_joke, meta)
					# 		if meta is not None:
					# 			joke['meta_joke_name'] = meta_joke
					# 			joke['meta_name'] = meta
					# 			meta_id = run_sql_proc('usp_InsertMeta', (meta, 0))
					# 			meta_joke = run_sql_proc('usp_InsertMetaJoke', (meta_joke, None, meta_id, 0))
					# 			joke['meta_joke'] = meta_joke
					# 			joke['meta_id'] = meta_id
						
					# else:
					# 	print('bogus: ', line, lineCounter)

				lineCounter += 1
				debug_prev_line = line

	return joke_ids
	
# Returns a dictionary of jokes, meta jokes, their metas and tags of the joke.
def read_joke_metas(meta_file):
	jokes = []
	with open(meta_file, 'r') as csv_file:
		reader = csv.reader(csv_file)
		next(reader, None)

		for row in reader:
			jokes.append({
			'joke': row[0],
			'joke_search': row[1],
			'meta_joke': row[2],
			'meta': row[3],
			'tags': row[4:]
			})

	return jokes

# Read joke data
file = open('joke_sample3.txt', 'r')
jokes = read_joke_metas('meta_jokes.csv')
meta_file = open('meta_jokes.csv', 'a')

text_box = file.read()
file.close()
parsed_jokes = parse_jokes(text_box, meta_file, jokes)

print(json.dumps(parsed_jokes))
