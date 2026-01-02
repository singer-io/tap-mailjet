from tap_mailjet.streams.abstracts import IncrementalStream

class MessageInformation(IncrementalStream):
    tap_stream_id = "message_information"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["CreatedAt"]
    data_key = "Data"
    path = "messageinformation"
    
    def set_incremental_params(self, bookmark_date: str) -> None:
        """Set FromTS parameter for MessageInformation stream"""
        self.update_params(FromTS=bookmark_date)

