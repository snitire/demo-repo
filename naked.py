# Importing the libraries used in the code
import requests
import json
import datetime
import time
import yaml

from datetime import datetime
print('Asteroid processing service')

# Initiating and reading config values
print('Loading configuration from file')

# Definining the API key and URL for the data request
nasa_api_key = "uwZhNf0wYgs7waqOTaZWiBwJwnpXLgRGzhWZzufr"
nasa_api_url = "https://api.nasa.gov/neo/"

# Getting todays date
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
print("Generated today's date: " + str(request_date))

# Making the actual request to NASA's API
print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

print("Response status code: " + str(r.status_code))
print("Response headers: " + str(r.headers))
print("Response content: " + str(r.text))

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
		print("Asteroid count today: " + str(ast_count))
		
		# If there are any asteroids in the data, start processing those
		if ast_count > 0:
			# For every NEO in today's data,
			for val in json_data['near_earth_objects'][request_date]:
				# If all the fields about the asteroid's name, NASA URL, diameter, whether it is potentially hazardous, possible close approaches are present
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# Get and save the asteroid's name, URL
					tmp_ast_name = val['name']
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
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
						print("No close approach data in message")
						# Set the close approach data to some default values
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1
					
					# Print the now processed asteroid info
					print("------------------------------------------------------- >>")
					print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
		# If the asteroid count is 0
		else:
			print("No asteroids are going to hit earth today")
	
	# Print hazardous and safe asteroid counts
	print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

	if len(ast_hazardous) > 0:
		# Sort the hazardous asteroid array by comparing the 5th element in each of the data lists (should be the close approach epoch timestamp)
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		print("Today's possible apocalypse (asteroid impact on earth) times:")
		# For each asteroid in the hazardous asteroid data, print the possible impact date, the name of the asteroid and the NASA page URL
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))
		
		# Sort the array by the 9th element (asteroid miss distance)
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		# Print the closest passing asteroid and provide its name, miss distance and NASA page URL
		print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		print("No asteroids close passing earth today")

# If the status code was anything other than 200 OK, inform the user about it
else:
	print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
