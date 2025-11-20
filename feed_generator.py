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
DSA_OUTPUT_FILE = "dsa_feed.csv"
MAX_WORKERS = 5 

# ------------------------------------------------------------------------------
# HUISSTIJL VLAKKEN (OFFICIAL BRAND COLORS)
# ------------------------------------------------------------------------------
# We gebruiken placehold.co om 'live' gekleurde vlakken te genereren.
# Kleuren aangeleverd door gebruiker (hashes verwijderd voor URL):
COLOR_MAGENTA = "c8007f"  # Hoofdkleur (Krachtig)
COLOR_LILAC   = "deceff"  # Steunkleur (Zacht)
# COLOR_WHITE = "f9f6f8"  # Te licht voor een advertentievlak, dus die slaan we over

# Fallback image
DEFAULT_IMAGE = f"https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png"

IMAGE_MAPPING = {
    # Kernbehandelaars & Medisch -> Magenta
    'Psychiater': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png',
    'Arts': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png',
    'Specialist': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png',
    'Verpleegkundige': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png',
    'Psycholoog': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png',
    
    # Begeleiding & Ondersteuning -> Lila (Fris & Toegankelijk)
    'Casemanager': f'https://placehold.co/1200x628/{COLOR_LILAC}/{COLOR_LILAC}.png',
    'Begeleider': f'https://placehold.co/1200x628/{COLOR_LILAC}/{COLOR_LILAC}.png',
    'Agogisch': f'https://placehold.co/1200x628/{COLOR_LILAC}/{COLOR_LILAC}.png',
    'Ervaringsdeskundige': f'https://placehold.co/1200x628/{COLOR_LILAC}/{COLOR_LILAC}.png',
    'Ondersteunend': f'https://placehold.co/1200x628/{COLOR_LILAC}/{COLOR_LILAC}.png',
    
    # Fallback
    'Zorg': f'https://placehold.co/1200x628/{COLOR_MAGENTA}/{COLOR_MAGENTA}.png'
}

# ------------------------------------------------------------------------------
# SPECIFICATIES
# ------------------------------------------------------------------------------
CSV_HEADERS = [
    "Job ID", "Location ID", "Title", "Final URL", "Image URL", 
    "Subtitle", "Description", "Salary", "Category", 
    "Contextual keywords", "Address", "Similar Job IDs"
]
DSA_HEADERS = ["Page URL", "Custom Label"]

BAD_ENDINGS = [
    "en", "of", "tot", "bij", "voor", "de", "het", "een", "in", "met", "&",
    "ambulant", "klinisch", "coordinerend", "verpleegkundig", "specialist",
    "psychotherapeut", "begeleider", "high", "intensive", "care", "senior",
    "junior", "tijdelijk", "vaste", "waarnemend", "bipolaire", "sociale",
    "inkoop", "coördinator", "coordinator", "medewerker", "functionaris"
]

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
    if len(text) <= max_len: return text
    if len(text) > max_len:
        truncated = text[:max_len]
        if " " in truncated: truncated = truncated.rsplit(' ', 1)[0]
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
    cleaned_keywords = [k.replace(';', '') for k in unique_keywords if k]
    return ";".join(cleaned_keywords[:25])

def calculate_similar_jobs(jobs):
    print("🔄 Berekenen van vergelijkbare vacatures...")
    for job in jobs:
        similar_ids = []
        my_id = job['Job ID']
        my_loc = job['Location ID']
        my_cat = job['Category']
        my_title = job['Title']
        for other in jobs:
            other_id = other['Job ID']
            if my_id == other_id: continue
            other_loc = other['Location ID']
            other_cat = other['Category']
            other_title = other['Title']
            if my_loc == other_loc and my_cat == other_cat:
                similar_ids.append(other_id)
            elif my_loc != other_loc and my_title.lower() == other_title.lower():
                similar_ids.append(other_id)
        job['Similar Job IDs'] = ";".join(similar_ids[:10])
    return jobs

# ------------------------------------------------------------------------------
# PARSE FUNCTIE (Met Brand Color Mapping)
# ------------------------------------------------------------------------------
def parse_job_page(url):
    time.sleep(random.uniform(0.01, 0.1))
    if "vacatures/?view" in url or url.endswith("/vacatures/"): return None
    content = get_content(url)
    if not content: return None
    soup = BeautifulSoup(content, 'html.parser')

    final_url = url
    utm_params = "utm_source=google&utm_medium=cpc&utm_campaign=job_feed"
    if '?' in url: final_url += f"&{utm_params}"
    else: final_url += f"?{utm_params}"

    job = {k: "" for k in CSV_HEADERS}
    job["Final URL"] = final_url
    job["Job ID"] = hashlib.md5(url.encode()).hexdigest()[:10]
    
    full_title = ""
    h1 = soup.find('h1')
    if h1: full_title = h1.get_text(strip=True)
    elif soup.title: full_title = soup.title.get_text().split('-')[0].strip()
    if not full_title or full_title.lower() == "vacatures": return None
    
    categories = ['Verpleegkundige', 'Psychiater', 'Begeleider', 'Psycholoog', 'Arts', 'ANIOS', 'Casemanager', 'Ervaringsdeskundige', 'Ondersteunend', 'Specialist', 'Agogisch']
    found_cat = "Zorg"
    for cat in categories:
        if cat.lower() in full_title.lower():
            if cat == 'ANIOS': found_cat = 'Arts'
            else: found_cat = cat
            break
    job["Category"] = found_cat
    job["Subtitle"] = format_google_text(found_cat, 25)

    # AFBEELDING KIEZEN: HUISSTIJL VLAKKEN
    if found_cat in IMAGE_MAPPING:
        job["Image URL"] = IMAGE_MAPPING[found_cat]
    else:
        job["Image URL"] = DEFAULT_IMAGE

    raw_location = "" 
    raw_salary = ""
    try:
        main = soup.find('main')
        if main:
            article = main.find('article')
            if article:
                section = article.find('section')
                if section:
                    container = section.find('div').find('div').find_all('div', recursive=False)[0]
                    if container:
                        items = container.find_all('div', recursive=False)
                        for item in items:
                            text = item.get_text(strip=True)
                            if '€' in text:
                                raw_salary = text
                                continue
                            if 'uur' in text.lower() or '/wk' in text.lower():
                                continue
                            if any(loc in text for loc in KNOWN_LOCATIONS) or "Regio" in text:
                                raw_location = text
                                continue
    except Exception: pass

    job["Title"] = format_google_text(full_title, 25, is_title=True)
    if len(job["Title"]) < 3: job["Title"] = format_google_text(full_title.split()[0], 25)

    final_city = "Amsterdam"
    if raw_location:
        mapped = False
        for regio, stad in LOCATION_MAPPING.items():
            if regio in raw_location:
                final_city = stad
                mapped = True
                break
        if not mapped:
            clean_raw = clean_forbidden_chars(raw_location)
            if len(clean_raw) < 20: final_city = clean_raw

    job["Location ID"] = final_city
    job["Address"] = f"{final_city}, NL"
    job["Salary"] = clean_salary(raw_salary)

    desc_div = soup.find('div', class_='vacancy-content') or soup.find('div', class_='content')
    if desc_div:
        for script in desc_div(["script", "style", "iframe"]): script.extract()
        raw_desc = desc_div.get_text(" ")
        job["Description"] = format_google_text(raw_desc, 25)
    else: job["Description"] = "Bekijk deze vacature"

    job["Contextual keywords"] = generate_keywords(full_title, found_cat, final_city)

    return job

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    start_time = time.time()
    print(f"🚀 Start Scraper v20.0 (Official Brand Colors)")
    links = extract_links_from_sitemap()
    if not links: sys.exit(1)
    print(f"✅ {len(links)} links gevonden. Start parallelle verwerking...")
    valid_jobs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(parse_job_page, url): url for url in links}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_url):
            completed += 1
            if completed % 20 == 0: print(f"   Voortgang: {completed}/{len(links)}...")
            try:
                data = future.result()
                if data: valid_jobs.append(data)
            except Exception as exc: print(f"   Fout in thread: {exc}")

    if valid_jobs: valid_jobs = calculate_similar_jobs(valid_jobs)

    print(f"💾 Opslaan {OUTPUT_FILE} (Vacature Feed)...")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(valid_jobs)

    print(f"💾 Opslaan {DSA_OUTPUT_FILE} (Page Feed)...")
    try:
        with open(DSA_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=DSA_HEADERS)
            writer.writeheader()
            for job in valid_jobs:
                writer.writerow({
                    "Page URL": job["Final URL"],
                    "Custom Label": job["Category"]
                })
    except Exception as e: print(f"Fout bij schrijven DSA feed: {e}")
    
    duration = time.time() - start_time
    print(f"🎉 Klaar in {duration:.2f} seconden! Twee feeds gegenereerd.")

if __name__ == "__main__":
    main()
