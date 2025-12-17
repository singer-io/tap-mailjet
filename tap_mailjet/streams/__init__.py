from tap_mailjet.streams.messages import Messages
from tap_mailjet.streams.contacts import Contacts
from tap_mailjet.streams.contacts_list import ContactsList
from tap_mailjet.streams.list_recipient import ListRecipient
from tap_mailjet.streams.campaigns import Campaigns
from tap_mailjet.streams.template import Template
from tap_mailjet.streams.message_information import MessageInformation
from tap_mailjet.streams.geo_statistics import GeoStatistics
from tap_mailjet.streams.click_statistics import ClickStatistics
from tap_mailjet.streams.top_link_clicked import TopLinkClicked
from tap_mailjet.streams.campaign_overview import CampaignOverview

STREAMS = {
    "messages": Messages,
    "contacts": Contacts,
    "contacts_list": ContactsList,
    "list_recipient": ListRecipient,
    "campaigns": Campaigns,
    "template": Template,
    "message_information": MessageInformation,
    "geo_statistics": GeoStatistics,
    "click_statistics": ClickStatistics,
    "top_link_clicked": TopLinkClicked,
    "campaign_overview": CampaignOverview,
}

