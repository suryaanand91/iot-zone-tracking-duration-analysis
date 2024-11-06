#!/usr/bin/env python3
from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from fastkml import kml
from shapely.geometry import Point, Polygon
from datetime import datetime, timedelta, timezone
import configparser
import os
import csv
import logging
import paramiko
import io

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("location_zone1.log"),  
                        logging.StreamHandler()                  
                    ])

def connect_to_mongodb_via_ssh(ssh_host, ssh_port, ssh_username, ssh_private_key,ssh_password, mongo_host, mongo_port, db_name, collection_name,mongo_tunnel_port):
    server = SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_username,
    ssh_pkey=ssh_private_key,
    ssh_private_key_password=ssh_password,
    remote_bind_address=(mongo_host, mongo_port),
    local_bind_address=(mongo_host, mongo_tunnel_port) 
    )

    server.start()
    client = MongoClient(mongo_host, mongo_tunnel_port)
    db = client[db_name]
    collection = db[collection_name]
    
    logging.info(f"Mongo SSH connection is established successfully")
    return server, collection

def perform_aggregation(collection, start_time, end_time):
    documents = collection.aggregate([
    {
        '$match': {
            'attrName': {   '$in': [    'longitude', 'latitude'    ]    }
           ,'recvTime': { '$gte': start_time} #, '$lte': end_time }
        }
    }, {
        '$group': { '_id': '$recvTime',
                    'count': {   '$sum': 1   },
                    'docs': {   '$push': '$$ROOT'    }   }
    },{
    '$sort': {   '_id': 1    }
    }
])
    device_entry =[]
    
    for doc in documents:
        latitude = 0.0
        longitude = 0.0
           
        if(doc.get('docs')[0]['attrName'] == 'latitude'):
            latitude = doc.get('docs')[0]['attrValue']
            longitude = doc.get('docs')[1]['attrValue']
        else:
            latitude = doc.docs[1].get('latitude')
            longitude = doc.docs[0].get('longitude')
            
        device = DeviceLocation( doc.get('_id'), doc.get('_id'), latitude, longitude, None)
        device_entry.append(device)
    logging.info(f"Executed Mongo Aggregatio query successfully")
    return device_entry



def read_kml_file(kml_file_path):
    if os.path.exists(kml_file_path):
        with open(kml_file_path, 'rb') as f:
            kml_data = f.read()
            k = kml.KML()
            k.from_string(kml_data)
        geometries = []
        zones_list = []
        zone_identifier = 1 # temperary _id value for zone. can be replaced later
        for feature in k.features():
            for placemark in feature.features():
                name = placemark.name
                geometry = placemark.geometry
                if hasattr(geometry, 'exterior'):
                # Convert to Shapely Polygon (ignore 3rd Z-coordinate)
                    polygon_2d = Polygon([(x, y) for x, y, z in geometry.exterior.coords])
                    geometries.append(polygon_2d)
                    zone = Zone(zone_identifier, name, polygon_2d , 0)
                    zones_list.append(zone);
                    zone_identifier = zone_identifier+ 1 ;
        return zones_list   
    else:
        logging.error(f"FileNotFoundError : KML file or path  not found: {kml_file_path}")
        return


# Function to check if a point is inside the given polygon
def check_is_point_inside_zone(point, zone):
    if zone.contains(point):
        #print(f"Point {point} is inside the polygon.")
        return True
    #print(f"Point {point} is not inside the polygon.")
    return False
    
    
def perform_zone_device_track_duration(locations,zones,max_idle_time):
    zone_cache = None # variable to save the zone where the device is located currently in the loop. Acts as a cache to avoid unceccessary iteration in the loop.
    entry_time = None
    exit_time = None
    current_location = False # variable to save the status of a location in current zone
    previous_timestamp = None # variable to save the timestamp of last location. Used tocheck the duration between the timestamps and whether its in the limit of max idle time
    for location in locations:
        if(zone_cache == None):
            for zone in zones:
                current_location = check_is_point_inside_zone(Point(location.longitude,location.latitude),zone.boundaries)
                if(current_location):
                            zone_cache=process_duration_for_point_inside_zone(zone,location,zone_cache)
                            previous_timestamp = location.recvTime
                            break
                else:
                    continue
        else:
            current_location = check_is_point_inside_zone(Point(location.longitude,location.latitude),zone_cache.boundaries)
            timestamp_difference = location.recvTime-previous_timestamp
            if(current_location):
                if (location == locations[-1] ):
                    zone_cache.exit_time.append(location.recvTime)
                    location.zone=zone_cache
                    zone_cache = None
                if timestamp_difference.total_seconds() > max_idle_time:
                    zone_cache.exit_time.append(previous_timestamp)
                    zone_cache.entry_time.append(location.recvTime)
                    location.zone=zone_cache                    
                previous_timestamp = location.recvTime
            else:
                previous_timestamp = location.recvTime
                zone_cache.exit_time.append(location.recvTime)
                location.zone=zone_cache
                previous_zone = zone_cache
                zone_cache = None
                previous_timestamp = location.recvTime        
                for zone in zones:
                    if(zone !=  previous_zone):
                        current_location = check_is_point_inside_zone(Point(location.longitude,location.latitude),zone.boundaries)
                        if(current_location):
                            zone_cache=zone;
                            zone_cache=process_duration_for_point_inside_zone(zone,location,zone_cache)
                            break
                    else:
                        continue
    logging.info(f"perform_zone_device_track_duration completed")

def process_duration_for_point_inside_zone(zone,location,zone_cache):
    if(zone_cache == zone):
        return zone_cache
    elif (zone_cache == None):
        zone.entry_time.append(location.recvTime)
        zone_cache=zone
        location.zone=zone
        return zone_cache
        
def calculate_time_duration(zones):
    for zone in zones:
        if(len(zone.exit_time) > 0 and len(zone.entry_time) > 0 and (len(zone.entry_time) == len(zone.exit_time))):
            total_duration = 0
            for i in range(len(zone.exit_time)):
                duration = zone.exit_time[i]  - zone.entry_time[i]
                total_duration += duration.total_seconds()
            zone.duration = total_duration
            minutes = total_duration/60


def generate_csv_file_local(zones,device_name,folder_path):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(folder_path, exist_ok=True)
    csv_file = os.path.join(folder_path,f"{device_name}{timestamp}.csv")
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Zone Id","Zone Name","Duration ","Total time in minutes"])
        
        for zone in zones:
            writer.writerow(zone.to_list())

    logging.info(f"Data written to {csv_file}")

def generate_csv_file(zones, device_name, sftp_host, sftp_port,sftp_username,sftp_password,remote_folder):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"{device_name}{timestamp}.csv"
    remote_file_path = f"{remote_folder}/{csv_filename}"
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Zone Id", "Zone Name", "Duration", "Total time in minutes"])
    for zone in zones:
        writer.writerow(zone.to_list())
    output.seek(0)
    print(f"SFTP Host: {sftp_host}")

    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)
        sftp_client = ssh_client.open_sftp()
        with sftp_client.file(remote_file_path, "w") as sftp_file:
            sftp_file.write(output.getvalue())
        logging.info(f"Data successfully written to {remote_file_path} on the SFTP server.")
        sftp_client.close()
        ssh_client.close()
    except Exception as e:
        logging.error(f"Failed to upload CSV to SFTP server: {e}")


def read_properties_file(file_path):
    config = configparser.ConfigParser()
    if not os.path.exists(file_path):
        logging.error(f"FileNotFoundError : Properties file not found: {file_path}")
        raise FileNotFoundError(f"Properties file not found: {file_path}")
    config.read(file_path)
    properties = {}
    for key in config['DEFAULT']:
        properties[key] = config['DEFAULT'][key]
    return properties

def convert_seconds_to_hm_string(seconds):
    total_minutes = seconds // 60
    hours = int(total_minutes // 60)
    remaining_minutes = int(total_minutes % 60)
    time_string = f"{hours}h:{remaining_minutes}m"
    return time_string


def main():
    end_time = datetime.now(tz=timezone.utc)
    logging.info(f"-------------------------------------Execution Started---------------------------------------")
    properties_file_path = '//Users/suryaanand/Documents/python/diy/readKML/config.properties'
    properties = read_properties_file(properties_file_path)
    time_limit_in_days = int(properties.get('records.max.limit.time')) 
    start_time = (datetime.now(tz=timezone.utc) - timedelta(days=time_limit_in_days))
    ssh_host = properties.get('ssh.host') 
    ssh_port = int(properties.get('ssh.port')) 
    ssh_username = properties.get('ssh.username') 
    ssh_private_key = properties.get('ssh.private.key')
    ssh_password = properties.get('ssh.password') 
    mongo_host = properties.get('mongo.host')
    mongo_port = int(properties.get('mongo.port'))
    mongo_tunnel_port = int(properties.get('mongo.tunnel.port'))
    db_name = properties.get('mongo.db.name')
    collection_name = properties.get('mongo.collection.name')
    device_name = properties.get('file.name.csv')
    csv_file_path = properties.get('folder.path.csv')
    folder_path = properties.get('folder.path.kml')
    file_name = properties.get('file.name.kml')
    kml_file = os.path.join(folder_path, file_name)
    max_idle_time = int(properties.get('device.max.idle.time'))
    sftp_host = properties.get('sftp.host.url')#"0.0.0.0"
    sftp_port = int(properties.get('sftp.port'))#22
    sftp_username = properties.get('sftp.username')#"admin"
    sftp_password = properties.get('sftp.password')#"admin"
    remote_folder = properties.get('sftp.remote.folder')#"/upload/GeoFence/output"
    # Read the KML file and extract polygons
    zones = read_kml_file(kml_file)       
    try:
        # Establish SSH tunnel and connect to MongoDB
        server, collection = connect_to_mongodb_via_ssh(
            ssh_host, ssh_port, ssh_username, ssh_private_key,ssh_password, mongo_host, mongo_port, db_name, collection_name,mongo_tunnel_port
        )
        # Execute the aggregation query and fetch the location details
        locations = perform_aggregation(collection, start_time, end_time)
        #Process the entry and exit times  in each location
        perform_zone_device_track_duration(locations,zones,max_idle_time)
        #Calculate the total duration in each location
        calculate_time_duration(zones)
        #Generate CSV file and save to output folder
        #generate_csv_file_local(zones,device_name,csv_file_path)
        generate_csv_file(zones, device_name, sftp_host, sftp_port,sftp_username,sftp_password,remote_folder)
        
        
    finally:
        # Close the SSH tunnel
        server.stop()
        logging.info(f"-------------------------------------Execution ended---------------------------------------")
        
        

 ## Zone have details regarding the different areas in a map. Zone is generatred using a KML file
 ## Id is a userdefined value currently its like an index (starts from 1)    
 ## name is the name of the polygon as defined in the kml file
 ## boundaries holds the cordinates of the polygon
 ## duration is the total duration that a device had spend at the zone
 ## entry and exit times are array of datetime when the device entried and exited a zone
class Zone:
  def __init__(self, zone_id, zone_name, boundaries, duration):
    self.zone_id = zone_id
    self.zone_name = zone_name
    self.boundaries = boundaries
    self.duration= duration
    self.entry_time = []
    self.exit_time = []
    
  def update_duration(self, duration):
        self.duration = duration
        
  def __repr__(self):
        return f"Zone(id='{self.zone_id}', boundaries='{self.boundaries}', duration='{self.duration}')"
        
  def to_list(self):
        duration_in_hr_format = convert_seconds_to_hm_string(self.duration)
        return [self.zone_id, self.zone_name,duration_in_hr_format,round(self.duration/60,2)]



 ## DeviceLocation have details regarding the device's location over the specified time.
 ## DeviceLocation is generatred using a data from mongo db
 ## Id is mongo time stamp when a record is generated  
 ## recvTime is same as _id 
 ## latitude holds the latitude of the device cordinate during recvTime
 ## longitude holds the longitude of the device cordinate during recvTime
 ## zone is the zone object to which currently the device is located. it can be none or valid
 ## zone is updated in the later stage      
class DeviceLocation:
    def __init__(self, _id, recvTime, latitude, longitude, zone):
        self._id = _id
        self.recvTime = recvTime
        self.latitude = latitude
        self.longitude= longitude
        self.zone= zone
        
    def update_zone(self, zone):
        self.zone = zone
        
    def __repr__(self):
        return f"DeviceLocation(recvTime='{self.recvTime}', latitude='{self.latitude}', longitude='{self.longitude}', zone='{self.zone}')"


        
if __name__ == "__main__":
    main()
