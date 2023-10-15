# Importing the libraries used in the code
import requests
import json
import datetime
import time
import yaml
from configparser import ConfigParser
import logging
import logging.config
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# Initializes a connection to the database based on the values in the config
def init_db():
	global connection
	connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)

# Gets a cursor for the currently connected DB
def get_cursor():
	global connection
	try:
		connection.ping(reconnect=True, attempts=1, delay=0)
		connection.commit()
	except mysql.connector.Error as err:
		logger.error("No connection to db " + str(err))
		connection = init_db()
		connection.commit()
	return connection.cursor()

# Check if asteroid exists in db
def mysql_check_if_ast_exists_in_db(request_day, ast_id):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		logger.error('Problem checking if asteroid exists: ' + str(e))
		pass
	return records[0][0]

# Asteroid value insert
def mysql_insert_ast_into_db(create_date, hazardous, name, url, diam_min, diam_max, ts, dt_utc, dt_local, speed, distance, ast_id):
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		connection.commit()
	except Error as e :
		logger.error( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "')")
		logger.error('Problem inserting asteroid values into DB: ' + str(e))
		pass

# Inserts multiple asteroids into the DB from a given array if the asteroid is not already in the DB
def push_asteroids_arrays_to_db(request_day, ast_array, hazardous):
	for asteroid in ast_array:
		if mysql_check_if_ast_exists_in_db(request_day, asteroid[9]) == 0:
			logger.debug("Asteroid NOT in db")
			mysql_insert_ast_into_db(request_day, hazardous, asteroid[0], asteroid[1], asteroid[2], asteroid[3], asteroid[4], asteroid[5], asteroid[6], asteroid[7], asteroid[8], asteroid[9])
		else:
			logger.debug("Asteroid already IN DB")

def sort_ast_by_pass_dist(ast_arr):
	if len(ast_arr) > 0:
		min_len = 1000000
		max_len = -1
		for val in ast_arr:
			if len(val) > max_len:
				max_len = len(val)
			if len(val) < min_len:
				min_len = len(val)
		if min_len == max_len and min_len >= 10:
			ast_arr.sort(key = lambda x: x[8], reverse=False)
			return ast_arr
		else:
			return []
	else:
		return []

def sort_ast_by_time(ast_arr):
	ast_hazardous.sort(key = lambda x: x[4], reverse=False)
	return ast_hazardous

# Loading logging configuration
with open('./log_worker.yaml', 'r') as stream:
	log_config = yaml.safe_load(stream)
logging.config.dictConfig(log_config)

# Creating logger
logger = logging.getLogger('root')

logger.info('Asteroid processing service')

# Initiating and reading config values
logger.info('Loading configuration from file')

# Definining the API key and URL for the data request
nasa_api_key = ""
nasa_api_url = "https://api.nasa.gov/neo/"

# Attempting to read the API key and URL from the config.ini file
try:
	config = ConfigParser()
	config.read('config.ini')

	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')

	mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
	mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
	mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
	mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
except:
	logger.exception("Unable to read data from config file")

logger.info('DONE')

connection = None
connected = False

init_db()

# Opening connection to mysql DB
logger.info('Connecting to MySQL DB')
try:
	# connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
	cursor = get_cursor()
	if connection.is_connected():
		db_Info = connection.get_server_info()
		logger.info('Connected to MySQL database. MySQL Server version on ' + str(db_Info))
		cursor = connection.cursor()
		cursor.execute("select database();")
		record = cursor.fetchone()
		logger.debug('You are connected to - ' + str(record))
		connection.commit()
except Error as e :
	logger.error('Error while connecting to MySQL' + str(e))

# Getting today's date
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
logger.debug("Generated today's date: " + str(request_date))

# Making the actual request to NASA's API
logger.debug("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

logger.debug("Response status code: " + str(r.status_code))
logger.debug("Response headers: " + str(r.headers))
logger.debug("Response content: " + str(r.text))

# If the request was successful, start processing the data
if r.status_code == 200:
	# Preparing a JSON object via the json library for easier data processing
	json_data = json.loads(r.text)
	# Initializing arrays to save the processed data
	ast_safe = []
	ast_hazardous = []
	
	# Check if there is the field for asteroid counts in today's data
	if 'element_count' in json_data:
		# The count is present, save it and process everything else
		ast_count = int(json_data['element_count'])
		logger.info("Asteroid count today: " + str(ast_count))
		
		# If there are any asteroids in the data, start processing those
		if ast_count > 0:
			# For every NEO in today's data,
			for val in json_data['near_earth_objects'][request_date]:
				# If all the fields about the asteroid's name, NASA URL, diameter, whether it is potentially hazardous, possible close approaches are present
				if 'name' and 'nasa_jpl_url' and 'id' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# Get and save the asteroid's name, URL, ID
					tmp_ast_name = val['name']
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
					tmp_ast_id = val['id']
					# If there is diameter data given in kilometers in the asteroid's data
					if 'kilometers' in val['estimated_diameter']:
						# If diameter estimations are available, round each of the diameters to the 3rd decimal place and save the value
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						# If for some reason the estimations aren't provided, save them as -2 anyway
						# This can be used later to see if NASA simply didnt provide the values or something was changed in the field names, or if something else broke
						else:
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					# If no diameter in kilometers is provided at all, save it for the same reasons as listed in the prev comment
					else:
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1
					# Save the data on whether the asteroid is potentially hazardous
					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']
					
					# If there is data on close approaches
					if len(val['close_approach_data']) > 0:
						# If data on the date, relative velocity, miss distance is available in the first item of the close approach data list
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							# Convert the provided unix timestamp to a more human readable date
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							
							# If the km/h velocity and miss distance in kilometers is available in the data
							# Process it similarly to the estimated diameter
							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								tmp_ast_speed = -1

							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								tmp_ast_miss_dist = -1
						# If for some reason any of the close approach date, relative velocity and miss distance fields is not in the data
						else:
							# Set the date values to easy-to-spot wrong/default ones
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					else:
						logger.warning("No close approach data in message")
						# Set the close approach data to some default values
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1
					
					# Print the now processed asteroid info
					logger.debug("------------------------------------------------------- >>")
					logger.debug("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					logger.debug("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					logger.debug("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id])
		# If the asteroid count is 0
		else:
			logger.info("No asteroids are going to hit earth today")
	
	# Print hazardous and safe asteroid counts
	logger.info("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

	if len(ast_hazardous) > 0:
		# Sort the hazardous asteroid array by comparing the 5th element in each of the data lists (should be the close approach epoch timestamp)
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		logger.info("Today's possible apocalypse (asteroid impact on earth) times:")
		# For each asteroid in the hazardous asteroid data, print the possible impact date, the name of the asteroid and the NASA page URL
		for asteroid in ast_hazardous:
			logger.info(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))
		
		# Sort the array by the 9th element (asteroid miss distance)
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		# Print the closest passing asteroid and provide its name, miss distance and NASA page URL
		logger.info("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		logger.info("No asteroids close passing earth today")

	# Add new asteroid data to the connected DB
	push_asteroids_arrays_to_db(request_date, ast_hazardous, 1)
	push_asteroids_arrays_to_db(request_date, ast_safe, 0)

# If the status code was anything other than 200 OK, inform the user about it
else:
	logger.error("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
