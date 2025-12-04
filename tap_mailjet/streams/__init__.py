from tap_mailjet.streams.message import Message
from tap_mailjet.streams.contact import Contact
from tap_mailjet.streams.contactslist import Contactslist
from tap_mailjet.streams.listrecipient import Listrecipient
from tap_mailjet.streams.campaign import Campaign

STREAMS = {
    "message": Message,
    "contact": Contact,
    "contactslist": Contactslist,
    "listrecipient": Listrecipient,
    "campaign": Campaign,
}

