from tap_mailjet.streams.message import Message
from tap_mailjet.streams.contact import Contact
from tap_mailjet.streams.contactslist import Contactslist
from tap_mailjet.streams.listrecipient import Listrecipient
from tap_mailjet.streams.campaign import Campaign
from tap_mailjet.streams.template import Template
from tap_mailjet.streams.messageinformation import Messageinformation
from tap_mailjet.streams.clickstatistics import Clickstatistics
from tap_mailjet.streams.geostatistics import Geostatistics
from tap_mailjet.streams.toplinkclicked import Toplinkclicked
from tap_mailjet.streams.campaignoverview import Campaignoverview

STREAMS = {
    "message": Message,
    "contact": Contact,
    "contactslist": Contactslist,
    "listrecipient": Listrecipient,
    "campaign": Campaign,
    "template": Template,
    "messageinformation": Messageinformation,
    "clickstatistics": Clickstatistics,
    "geostatistics": Geostatistics,
    "toplinkclicked": Toplinkclicked,
    "campaignoverview": Campaignoverview,
}

