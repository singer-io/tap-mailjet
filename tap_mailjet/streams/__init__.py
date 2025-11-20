from tap_mailjet.streams.message import Message
from tap_mailjet.streams.messageinformation import Messageinformation
from tap_mailjet.streams.contact import Contact
from tap_mailjet.streams.contactslist import Contactslist
from tap_mailjet.streams.contactdata import Contactdata
from tap_mailjet.streams.contactmetadata import Contactmetadata
from tap_mailjet.streams.listrecipient import Listrecipient
from tap_mailjet.streams.campaign import Campaign
from tap_mailjet.streams.contactfilter import Contactfilter
from tap_mailjet.streams.campaignoverview import Campaignoverview

STREAMS = {
    "message": Message,
    "messageinformation": Messageinformation,
    "contact": Contact,
    "contactslist": Contactslist,
    "contactdata": Contactdata,
    "contactmetadata": Contactmetadata,
    "listrecipient": Listrecipient,
    "campaign": Campaign,
    "contactfilter": Contactfilter,
    "campaignoverview": Campaignoverview,
}

