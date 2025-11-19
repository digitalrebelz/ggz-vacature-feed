import requests
from bs4 import BeautifulSoup
import csv
import hashlib
import time
import re
import sys
import xml.etree.ElementTree as ET

# ------------------------------------------------------------------------------
# CONFIGURATIE
# ------------------------------------------------------------------------------
SITEMAP_URL = "https://werkenbij.ggzingeest.nl/job-sitemap.xml"
OUTPUT_FILE = "jobs_feed.csv"
DEFAULT_IMAGE = "https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg"

# Google Ads "Jobs" Feed Specificaties
CSV_HEADERS = [
    "Job ID",           
    "Location ID",      
    "Title",            
    "Final URL",        
    "Image URL",        
    "Subtitle",         
    "Description",      
    "Salary",           
    "Category",         
    "Contextual keywords", 
    "Address"           
]

# Woorden die we NIET aan het einde van een titel willen zien als hij wordt afgekapt
BAD_ENDINGS = [
    "en", "of", "tot", "bij", "voor", "de", "het", "een", "in", "met",
    "ambulant", "klinisch", "coordinerend", "verpleegkundig", "specialist",
    "psychotherapeut", "begeleider", "high", "intensive", "care", "senior",
    "junior", "tijdelijk", "vaste", "waarnemend"
]

# Slimme vervangingen om titels korter te maken zodat ze wel passen (< 25 chars)
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
    "Kinderen": "Kind."
}

# Geoptimaliseerde mapping op basis van jouw best converterende zoekwoorden
KEYWORD_MAPPING = {
    'Verpleegkundige': [
        'Ambulant verpleegkundige', 'GGZ verpleegkundige', 'HBO verpleegkundige', 
        'Leerling verpleegkundige', 'Verpleging', 'BIG', 'Ziekenhuis', 'Zorg'
    ],
    'Psychiater': [
        'Psychiater vacature', 'Psychiatrie', 'Medisch specialist', 
        'Arts', 'BIG', 'Zorg', 'GGZ Amsterdam'
    ],
    'Begeleider': [
        'Persoonlijk begeleider', 'Ambulant begeleider', 'Begeleider niveau 4', 
        'Ambulante begeleiding', 'Maatschappelijke zorg', 'Activiteitenbegeleider', 
        'Welzijn', 'MBO', 'HBO', 'Begeleider Amsterdam'
    ],
    'Psycholoog': [
        'GZ-psycholoog', 'Klinisch psycholoog', 'Klinisch neuropsycholoog', 
        'Basispsycholoog', 'Psychologie', 'Behandelaar', 'GGZ'
    ],
    'Arts': [
        'ANIOS psychiatrie', 'Basisarts', 'Geneeskunde', 'Medisch', 
        'Specialist', 'Zorg', 'Opleidingsplaats'
    ],
    'Casemanager': [
        'Casemanager GGZ', 'Casemanager Amsterdam', 'Ambulante zorg', 
        'Regiebehandelaar', 'Zorgcoördinatie'
    ],
    'Ervaringsdeskundige': [
        'Ervaringsdeskundigheid', 'Herstel', 'Ondersteuning', 'GGZ ervaringsdeskundige'
    ],
    'Ondersteunend': [
        'Administratie', 'Bedrijfsvoering', 'Kantoor', 'Management', 'Secretariaat'
    ],
    'Specialist': [
        'Specialisme', 'Expertise', 'Zorgprofessional', 'Behandelaar'
    ],
    'Zorg': [
        'Gezondheidszorg', 'Welzijn', 'Hulpverlening', 'Jeugdpsychiatrie', 'Jeugd GGZ'
    ]
}

BASE_KEYWORDS = [
    "Werken bij GGZ inGeest", "GGZ inGeest vacatures", "Vacatures GGZ inGeest",
    "Werken bij GGZ", "GGZ vacatures", 
    "Vacatures inGeest", "Werken in de GGZ"
]

# ------------------------------------------------------------------------------
# HULP FUNCTIES
# ------------------------------------------------------------------------------

def get_content(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    }
    try:
        response = requests.get(url, headers=headers, timeout=20)
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
    """Verwijdert onnodige tekens zoals , / ? en " uit de tekst."""
    if not text: return ""
    # Vervang specifieke tekens door spaties of niets
    text = re.sub(r'[,/?"]', ' ', text)
    # Dubbele spaties weghalen die hierdoor ontstaan
    return re.sub(' +', ' ', text).strip()

def format_google_text(text, max_len=25, is_title=False):
    if not text: return ""
    
    # Stap 1: Basis opschoning (HTML weg)
    text = re.sub('<[^<]+?>', '', text)
    text = text.replace('&nbsp;', ' ').replace('\n', ' ').strip()
    
    # Stap 2: Verboden tekens weghalen
    text = clean_forbidden_chars(text)
    
    if len(text) <= max_len:
        return text
    
    # Stap 3: Als het te lang is, probeer eerst slimme vervangingen (alleen voor titels)
    if is_title:
        for long_term, short_term in TITLE_REPLACEMENTS.items():
            if long_term in text:
                text = text.replace(long_term, short_term)
    
    # Nog steeds te lang? Dan hard afkappen
    if len(text) > max_len:
        truncated = text[:max_len]
        # Zorg dat we niet midden in een woord knippen
        if " " in truncated:
            truncated = truncated.rsplit(' ', 1)[0]
        text = truncated
        
    # Stap 4: Check op "rare" eindwoorden (alleen als we hebben ingekort)
    if is_title:
        words = text.split()
        # Zolang het laatste woord in de BAD_ENDINGS lijst staat, haal het weg
        while words and words[-1].lower() in BAD_ENDINGS:
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
    """Genereert keywords, max 25 items, geen verboden tekens."""
    keywords = list(BASE_KEYWORDS)
    
    if category in KEYWORD_MAPPING:
        keywords.extend(KEYWORD_MAPPING[category])
    
    if location:
        # Schoon de locatie ook op
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
    
    # Google limiet contextual keywords: We houden het op max 25
    # En we zorgen dat de puntkomma scheiding goed gaat (geen puntkomma IN de woorden)
    cleaned_keywords = [k.replace(';', '') for k in unique_keywords if k]
    
    return ";".join(cleaned_keywords[:25])

def parse_job_page(url):
    if "vacatures/?view" in url or url.endswith("/vacatures/"):
        return None

    content = get_content(url)
    if not content: return None
    soup = BeautifulSoup(content, 'html.parser')

    job = {k: "" for k in CSV_HEADERS}
    job["Final URL"] = url
    job["Job ID"] = hashlib.md5(url.encode()).hexdigest()[:10]
    job["Image URL"] = DEFAULT_IMAGE
    
    full_title = ""
    raw_location = "Amsterdam" 
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
                        if len(items) >= 2:
                            raw_location = items[1].get_text(strip=True)
                        if len(items) >= 3:
                            raw_salary = items[2].get_text(strip=True)

        h1 = soup.find('h1')
        if h1:
            full_title = h1.get_text(strip=True)
        elif soup.title:
            full_title = soup.title.get_text().split('-')[0].strip()

    except Exception:
        pass

    if not full_title or full_title.lower() == "vacatures":
        return None

    # DATA VULLEN
    # is_title=True activeert de slimme logica om "tot", "en", "ambulant" aan het eind te verwijderen
    job["Title"] = format_google_text(full_title, 25, is_title=True)
    
    # Als de titel na opschonen leeg of te kort is (bijv. omdat alles is weggeknipt),
    # val terug op de categorie of eerste woord van originele titel
    if len(job["Title"]) < 3:
         job["Title"] = format_google_text(full_title.split()[0], 25)
    
    categories = [
        'Verpleegkundige', 'Psychiater', 'Begeleider', 'Psycholoog', 
        'Arts', 'ANIOS', 'Casemanager', 'Ervaringsdeskundige', 
        'Ondersteunend', 'Specialist', 'Agogisch'
    ]
    
    found_cat = "Zorg"
    for cat in categories:
        if cat.lower() in full_title.lower():
            if cat == 'ANIOS':
                found_cat = 'Arts'
            else:
                found_cat = cat
            break
            
    job["Category"] = found_cat
    job["Subtitle"] = format_google_text(found_cat, 25)

    if raw_location:
        clean_loc = clean_forbidden_chars(raw_location)
        job["Location ID"] = clean_loc
        job["Address"] = f"{clean_loc} NL" # Komma weggehaald want dat was verboden teken
    
    job["Salary"] = clean_salary(raw_salary)

    desc_div = soup.find('div', class_='vacancy-content') or soup.find('div', class_='content')
    if desc_div:
        for script in desc_div(["script", "style", "iframe"]):
            script.extract()
        raw_desc = desc_div.get_text(" ")
        job["Description"] = format_google_text(raw_desc, 25)
    else:
        job["Description"] = "Bekijk deze vacature"

    job["Contextual keywords"] = generate_keywords(full_title, found_cat, raw_location)

    img = soup.find('meta', property='og:image')
    if img:
        job["Image URL"] = img['content']

    return job

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    print("🚀 Start Scraper v9.0 (Clean & Smart Titles)")
    links = extract_links_from_sitemap()
    if not links: sys.exit(1)

    print(f"✅ {len(links)} links gevonden. Start verwerking...")
    valid_jobs = []
    
    for i, link in enumerate(links):
        if i >= 300: break
        if i % 10 == 0: print(f"   Bezig met {i+1}/{len(links)}...")
        
        try:
            job = parse_job_page(link)
            if job: valid_jobs.append(job)
            time.sleep(0.1)
        except Exception as e:
            print(f"   Fout bij {link}: {e}")

    print(f"💾 Opslaan van {len(valid_jobs)} vacatures naar {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(valid_jobs)
    print("🎉 Klaar!")

if __name__ == "__main__":
    main()
