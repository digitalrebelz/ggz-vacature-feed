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
# De sleutel tot succes: de specifieke job sitemap
SITEMAP_URL = "https://werkenbij.ggzingeest.nl/job-sitemap.xml"

OUTPUT_FILE = "jobs_feed.csv"
DEFAULT_IMAGE = "https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg"

CSV_HEADERS = ["Job ID", "Location ID", "Title", "Final URL", "Image URL", "Category", "Description", "Salary", "Address"]

# ------------------------------------------------------------------------------
# HULP FUNCTIES
# ------------------------------------------------------------------------------

def get_content(url):
    """Haalt URL op met een echte browser User-Agent."""
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
    """Haalt vacature URL's direct uit de job-sitemap."""
    print(f"🕵️‍♂️  Ophalen sitemap: {SITEMAP_URL}")
    vacancy_urls = set()

    content = get_content(SITEMAP_URL)
    if not content:
        print("❌ Kon sitemap niet laden.")
        return []

    try:
        root = ET.fromstring(content)
        # XML parsen en zoeken naar <loc> tags
        for child in root.iter():
            if child.tag.endswith('loc'):
                url = child.text
                # Filter: moet een link zijn
                if url and url.startswith('http'):
                    vacancy_urls.add(url)
        
        print(f"   -> {len(vacancy_urls)} links gevonden in sitemap.")

    except ET.ParseError:
        print("❌ Kon XML niet lezen. Is de sitemap wel XML?")
        return []

    return list(vacancy_urls)

def clean_text(text):
    if not text: return ""
    # Verwijder HTML tags
    text = re.sub('<[^<]+?>', '', text)
    # Verwijder iframe codes en vreemde tekens
    text = text.replace('&nbsp;', ' ').replace('iframe', '')
    # Netjes maken witruimtes
    text = text.replace('\n', ' ').replace('\r', '').strip()
    text = re.sub(' +', ' ', text)
    return text

def parse_job_page(url):
    # Sla overzichtspagina's over die per ongeluk in de sitemap staan
    if "vacatures/?view" in url or url.endswith("/vacatures/"):
        return None

    content = get_content(url)
    if not content: return None
    soup = BeautifulSoup(content, 'html.parser')

    job = {k: "" for k in CSV_HEADERS}
    job["Final URL"] = url
    job["Job ID"] = hashlib.md5(url.encode()).hexdigest()[:10]
    job["Image URL"] = DEFAULT_IMAGE
    job["Address"] = "Amsterdam/Omgeving"

    # 1. TITEL
    h1 = soup.find('h1')
    if h1:
        job["Title"] = h1.get_text(strip=True)
    elif soup.title:
        job["Title"] = soup.title.get_text().split('-')[0].strip()
    
    # BELANGRIJK: Als de titel "Vacatures" is, zitten we toch op een overzichtspagina -> NEGEREN
    if not job["Title"] or job["Title"].lower() == "vacatures" or "werken bij" in job["Title"].lower():
        return None

    # 2. BESCHRIJVING
    # We zoeken naar de specifieke div die vaak bij vacatures wordt gebruikt
    # Eerst proberen we specifieke classes
    desc_div = soup.find('div', class_='vacancy-content') 
    if not desc_div:
        desc_div = soup.find('div', class_='content')
    if not desc_div:
        desc_div = soup.find('div', id='content')

    if desc_div:
        # Pak tekst, maar negeer scripts en styles
        for script in desc_div(["script", "style", "iframe"]):
            script.extract()
        raw_desc = desc_div.get_text(" ")
        job["Description"] = clean_text(raw_desc)[:250] + "..."
    else:
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta:
            job["Description"] = meta.get('content', '')[:250]

    # 3. SALARIS (Regex voor bedragen)
    salary_match = re.search(r'€\s?(\d{1,3}\.?\d{0,3}).*?€\s?(\d{1,3}\.?\d{0,3})', soup.get_text())
    if salary_match:
        job["Salary"] = salary_match.group(0).replace('.', '') # Google wil vaak geen punten in duizendtallen, check dit evt
    
    # 4. CATEGORIE
    categories = ['verpleegkundige', 'psychiater', 'begeleider', 'psycholoog', 'arts', 'ondersteunend', 'specialist']
    title_lower = job["Title"].lower()
    for cat in categories:
        if cat in title_lower:
            job["Category"] = cat.capitalize()
            break
    if not job["Category"]: job["Category"] = "Zorg"

    # 5. LOCATIE
    # Soms staat locatie in een lijstje of specifieke tag. 
    # We scannen de tekst op bekende locaties van GGZ inGeest
    locaties = ["Amsterdam", "Haarlem", "Hoofddorp", "Amstelveen", "Bennebroek"]
    full_text = soup.get_text()
    for loc in locaties:
        if loc in full_text:
            job["Address"] = f"{loc}, Nederland"
            job["Location ID"] = loc
            break

    # 6. BEELD
    img = soup.find('meta', property='og:image')
    if img:
        job["Image URL"] = img['content']

    return job

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    print("🚀 Start Scraper v3.0 (Specific Job Sitemap)")
    
    links = extract_links_from_sitemap()
    
    if not links:
        print("❌ Geen links gevonden. Stop.")
        sys.exit(1)

    print(f"✅ {len(links)} links gevonden. Start verwerking...")

    valid_jobs = []
    # We verwerken er maximaal 300 om timeouts te voorkomen
    for i, link in enumerate(links):
        if i >= 300: break
        
        # Voortgangsindicator (print elke 10 jobs)
        if i % 10 == 0:
            print(f"   Bezig met {i+1}/{len(links)}...")
            
        try:
            job = parse_job_page(link)
            if job:
                valid_jobs.append(job)
            time.sleep(0.1) # Korte pauze
        except Exception as e:
            print(f"   Fout bij {link}: {e}")

    print(f"💾 Opslaan van {len(valid_jobs)} ECHTE vacatures naar {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(valid_jobs)
    
    print("🎉 Klaar!")

if __name__ == "__main__":
    main()
