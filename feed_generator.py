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

CSV_HEADERS = ["Job ID", "Location ID", "Title", "Final URL", "Image URL", "Category", "Description", "Salary", "Address"]

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

def clean_text(text):
    if not text: return ""
    text = re.sub('<[^<]+?>', '', text)
    text = text.replace('&nbsp;', ' ').replace('iframe', '')
    text = text.replace('\n', ' ').replace('\r', '').strip()
    return re.sub(' +', ' ', text)

def parse_job_page(url):
    # Filter overzichtspagina's
    if "vacatures/?view" in url or url.endswith("/vacatures/"):
        return None

    content = get_content(url)
    if not content: return None
    soup = BeautifulSoup(content, 'html.parser')

    job = {k: "" for k in CSV_HEADERS}
    job["Final URL"] = url
    job["Job ID"] = hashlib.md5(url.encode()).hexdigest()[:10]
    job["Image URL"] = DEFAULT_IMAGE
    
    # 1. TITEL
    h1 = soup.find('h1')
    if h1:
        job["Title"] = h1.get_text(strip=True)
    elif soup.title:
        job["Title"] = soup.title.get_text().split('-')[0].strip()
    
    if not job["Title"] or job["Title"].lower() == "vacatures":
        return None

    # 2. INTELLIGENTE SCRAPER (Gebaseerd op jouw XPath)
    # XPath: /html/body/main/article/section[1]/div/div/div[1]/div[x]
    try:
        # Navigeer door de structuur zoals in de XPath
        main = soup.find('main')
        if main:
            article = main.find('article')
            if article:
                # Eerste section
                section = article.find('section')
                if section:
                    # div > div > div (container van de metadata blokjes)
                    container = section.find('div').find('div').find_all('div', recursive=False)[0]
                    if container:
                        items = container.find_all('div', recursive=False)
                        
                        # Item 2 is Locatie (index 1 in Python)
                        if len(items) >= 2:
                            loc_text = items[1].get_text(strip=True)
                            if loc_text:
                                job["Address"] = f"{loc_text}, Nederland"
                                job["Location ID"] = loc_text

                        # Item 3 is Salaris (index 2 in Python)
                        if len(items) >= 3:
                            sal_text = items[2].get_text(strip=True)
                            # Soms staat er tekst voor, we pakken de hele string want die is netjes
                            if "€" in sal_text or "schaal" in sal_text.lower():
                                job["Salary"] = sal_text
    except Exception as e:
        pass # Faalt stilzwijgend, valt terug op fallback hieronder

    # 3. FALLBACKS (Als bovenstaande faalt)
    
    # Fallback Locatie
    if not job["Address"]:
        locaties = ["Amsterdam", "Haarlem", "Hoofddorp", "Amstelveen", "Bennebroek", "Badhoevedorp"]
        full_text = soup.get_text()
        for loc in locaties:
            if loc in full_text:
                job["Address"] = f"{loc}, Nederland"
                job["Location ID"] = loc
                break
        if not job["Address"]:
            job["Address"] = "Regio Noord-Holland, Nederland"

    # Fallback Salaris (Regex)
    if not job["Salary"]:
        salary_match = re.search(r'€\s?(\d{1,3}\.?\d{0,3}).*?€\s?(\d{1,3}\.?\d{0,3})', soup.get_text())
        if salary_match:
            job["Salary"] = salary_match.group(0).replace('.', '')

    # 4. BESCHRIJVING
    desc_div = soup.find('div', class_='vacancy-content') or soup.find('div', class_='content') or soup.find('div', id='content')
    if desc_div:
        for script in desc_div(["script", "style", "iframe"]):
            script.extract()
        job["Description"] = clean_text(desc_div.get_text(" "))[:250] + "..."
    else:
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta:
            job["Description"] = meta.get('content', '')[:250]

    # 5. CATEGORIE
    categories = ['Verpleegkundige', 'Psychiater', 'Begeleider', 'Psycholoog', 'Arts', 'Ondersteunend', 'Specialist']
    for cat in categories:
        if cat.lower() in job["Title"].lower():
            job["Category"] = cat
            break
    if not job["Category"]: job["Category"] = "Zorg"

    # 6. BEELD
    img = soup.find('meta', property='og:image')
    if img:
        job["Image URL"] = img['content']

    return job

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    print("🚀 Start Scraper v4.0 (XPath Logic)")
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
