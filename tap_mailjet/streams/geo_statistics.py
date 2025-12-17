from tap_mailjet.streams.abstracts import FullTableStream

class GeoStatistics(FullTableStream):
    tap_stream_id = "geo_statistics"
    key_properties = []
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "geostatistics"

