import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import csv
import hashlib
import time
import re
import sys
import xml.etree.ElementTree as ET
import concurrent.futures
import random

# ------------------------------------------------------------------------------
# CONFIGURATIE
# ------------------------------------------------------------------------------
SITEMAP_URL = "https://werkenbij.ggzingeest.nl/job-sitemap.xml"
OUTPUT_FILE = "jobs_feed.csv"
DEFAULT_IMAGE = "https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg"
MAX_WORKERS = 5 

# Google Ads "Jobs" Feed Specificaties
# Toegevoegd: "Similar Job IDs"
CSV_HEADERS = [
    "Job ID", "Location ID", "Title", "Final URL", "Image URL", 
    "Subtitle", "Description", "Salary", "Category", 
    "Contextual keywords", "Address", "Similar Job IDs"
]

# Woorden die we NIET aan het einde van een titel willen zien
BAD_ENDINGS = [
    "en", "of", "tot", "bij", "voor", "de", "het", "een", "in", "met", "&",
    "ambulant", "klinisch", "coordinerend", "verpleegkundig", "specialist",
    "psychotherapeut", "begeleider", "high", "intensive", "care", "senior",
    "junior", "tijdelijk", "vaste", "waarnemend", "bipolaire", "sociale",
    "inkoop", "coördinator", "coordinator", "medewerker", "functionaris"
]

# Slimme vervangingen
TITLE_REPLACEMENTS = {
    "Verpleegkundig Specialist": "VS",
    "Verpleegkundige": "Vpl.",
    "Gezondheidszorgpsycholoog": "GZ-psycholoog",
    "Klinisch Psycholoog": "KP",
    "Psychiater": "Psych.",
    "Begeleider": "Begl.",
    "Ambulant": "Amb.",
    "Spoedeisende Psychiatrie": "SEH Psychiatrie",
    "Opleiding tot": "Opleiding",
    "High Intensive Care": "HIC",
    "Intensive Care": "IC",
    "Ouderen": "Oud.",
    "Kinderen": "Kind.",
    "Coördinator": "Coord.",
    "Coördinerend": "Coord.",
    "Medewerker": "Medw.",
    " & ": " en ",
    "Inkoop": "Ink.",
    "Contractbeheer": "Contract."
}

KNOWN_LOCATIONS = [
    "Amsterdam", "Haarlem", "Amstelveen", "Hoofddorp", "Bennebroek", 
    "Badhoevedorp", "Zuid-Kennemerland", "Amstelland"
]

LOCATION_MAPPING = {
    "Regio Amsterdam-Amstelland en Zuid-Kennemerland": "Amsterdam",
    "Zuid-Kennemerland": "Haarlem",
    "Amstelland": "Amstelveen",
    "Kennemerland": "Haarlem",
    "Amsterdam-Amstelland": "Amsterdam"
}

KEYWORD_MAPPING = {
    'Verpleegkundige': ['Ambulant verpleegkundige', 'GGZ verpleegkundige', 'HBO verpleegkundige', 'Leerling verpleegkundige', 'Verpleging', 'BIG', 'Ziekenhuis', 'Zorg'],
    'Psychiater': ['Psychiater vacature', 'Psychiatrie', 'Medisch specialist', 'Arts', 'BIG', 'Zorg', 'GGZ Amsterdam'],
    'Begeleider': ['Persoonlijk begeleider', 'Ambulant begeleider', 'Begeleider niveau 4', 'Ambulante begeleiding', 'Maatschappelijke zorg', 'Activiteitenbegeleider', 'Welzijn', 'MBO', 'HBO', 'Begeleider Amsterdam'],
    'Psycholoog': ['GZ-psycholoog', 'Klinisch psycholoog', 'Klinisch neuropsycholoog', 'Basispsycholoog', 'Psychologie', 'Behandelaar', 'GGZ'],
    'Arts': ['ANIOS psychiatrie', 'Basisarts', 'Geneeskunde', 'Medisch', 'Specialist', 'Zorg', 'Opleidingsplaats'],
    'Casemanager': ['Casemanager GGZ', 'Casemanager Amsterdam', 'Ambulante zorg', 'Regiebehandelaar', 'Zorgcoördinatie'],
    'Ervaringsdeskundige': ['Ervaringsdeskundigheid', 'Herstel', 'Ondersteuning', 'GGZ ervaringsdeskundige'],
    'Ondersteunend': ['Administratie', 'Bedrijfsvoering', 'Kantoor', 'Management', 'Secretariaat'],
    'Specialist': ['Specialisme', 'Expertise', 'Zorgprofessional', 'Behandelaar'],
    'Zorg': ['Gezondheidszorg', 'Welzijn', 'Hulpverlening', 'Jeugdpsychiatrie', 'Jeugd GGZ']
}

BASE_KEYWORDS = [
    "Werken bij GGZ inGeest", "GGZ inGeest vacatures", "Vacatures GGZ inGeest",
    "Werken bij GGZ", "GGZ vacatures", "Vacatures inGeest", "Werken in de GGZ"
]

# ------------------------------------------------------------------------------
# SESSION SETUP
# ------------------------------------------------------------------------------
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    })
    return session

http = create_session()

# ------------------------------------------------------------------------------
# HULP FUNCTIES
# ------------------------------------------------------------------------------

def get_content(url):
    try:
        response = http.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"⚠️ Fout bij laden {url}: {e}")
        return None

def extract_links_from_sitemap():
    print(f"🕵️‍♂️  Ophalen sitemap: {SITEMAP_URL}")
    vacancy_urls = set()
    content = get_content(SITEMAP_URL)
    if not content: return []

    try:
        root = ET.fromstring(content)
        for child in root.iter():
            if child.tag.endswith('loc'):
                url = child.text
                if url and url.startswith('http'):
                    vacancy_urls.add(url)
        print(f"   -> {len(vacancy_urls)} links gevonden in sitemap.")
    except:
        print("❌ Kon XML niet lezen.")
        return []
    return list(vacancy_urls)

def clean_forbidden_chars(text):
    if not text: return ""
    text = re.sub(r'[,/?"“”]', ' ', text)
    return re.sub(' +', ' ', text).strip()

def format_google_text(text, max_len=25, is_title=False):
    if not text: return ""
    text = re.sub('<[^<]+?>', '', text)
    text = text.replace('&nbsp;', ' ').replace('\n', ' ').strip()
    
    if is_title:
        for long_term, short_term in TITLE_REPLACEMENTS.items():
            if long_term in text:
                text = text.replace(long_term, short_term)

    text = clean_forbidden_chars(text)
    
    if len(text) <= max_len:
        return text
    
    if len(text) > max_len:
        truncated = text[:max_len]
        if " " in truncated:
            truncated = truncated.rsplit(' ', 1)[0]
        text = truncated
    
    if is_title:
        words = text.split()
        while words and (words[-1].lower() in BAD_ENDINGS or not words[-1].isalnum()):
            words.pop()
        text = " ".join(words)
        
    return text

def clean_salary(text):
    if not text: return ""
    text = clean_forbidden_chars(text)
    matches = re.findall(r'€\s?(\d{1,3}\.?\d{0,3})', text)
    if len(matches) >= 2:
        low = matches[0].replace('.', '')
        high = matches[1].replace('.', '')
        return f"€{low}-€{high}"
    elif len(matches) == 1:
        val = matches[0].replace('.', '')
        return f"€{val}"
    return format_google_text(text, 25)

def generate_keywords(title, category, location):
    keywords = list(BASE_KEYWORDS)
    if category in KEYWORD_MAPPING:
        keywords.extend(KEYWORD_MAPPING[category])
    
    if location:
        clean_loc = clean_forbidden_chars(location)
        keywords.append(clean_loc)
        keywords.append(f"Vacatures {clean_loc}")
        keywords.append(f"GGZ vacatures {clean_loc}")
        keywords.append(f"GGZ inGeest {clean_loc} vacatures")
        keywords.append(f"Werken bij GGZ {clean_loc}")
        
    stop_words = ['bij', 'de', 'het', 'een', 'en', 'voor', 'van', 'vacature']
    title_words = [clean_forbidden_chars(w) for w in title.split() if w.lower() not in stop_words and len(w) > 3]
    keywords.extend(title_words)
    
    unique_keywords = list(set(keywords))
    cleaned_keywords = [k.replace(';', '') for k in unique_
