from tap_mailjet.streams.abstracts import FullTableStream

class CampaignOverview(FullTableStream):
    tap_stream_id = "campaign_overview"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "campaignoverview"

