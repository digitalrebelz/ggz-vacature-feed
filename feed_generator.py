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
BASE_URL = "[https://werkenbij.ggzingeest.nl](https://werkenbij.ggzingeest.nl)"
SITEMAP_URLS = [
    "[https://werkenbij.ggzingeest.nl/vacature-sitemap.xml](https://werkenbij.ggzingeest.nl/vacature-sitemap.xml)",
    "[https://werkenbij.ggzingeest.nl/sitemap_index.xml](https://werkenbij.ggzingeest.nl/sitemap_index.xml)"
]

OUTPUT_FILE = "jobs_feed.csv"
DEFAULT_IMAGE = "[https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg](https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg)"

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
    """Probeert vacature URL's direct uit de XML sitemap te halen."""
    print("🕵️‍♂️  Start zoektocht via Sitemaps...")
    vacancy_urls = set()

    for sitemap_url in SITEMAP_URLS:
        print(f"   Proberen: {sitemap_url}")
        content = get_content(sitemap_url)
        if not content:
            continue

        try:
            root = ET.fromstring(content)
            for child in root.iter():
                if child.tag.endswith('loc'):
                    url = child.text
                    if url and '/vacatures/' in url and url != "[https://werkenbij.ggzingeest.nl/vacatures/](https://werkenbij.ggzingeest.nl/vacatures/)":
                        vacancy_urls.add(url)
            
            if len(vacancy_urls) == 0 and 'index' in sitemap_url:
                print("   Dit lijkt een index sitemap. We zoeken naar sub-sitemaps...")
                for child in root.iter():
                    if child.tag.endswith('loc'):
                        sub_url = child.text
                        if 'vacature' in sub_url or 'post' in sub_url:
                            print(f"   >>> Sub-sitemap gevonden: {sub_url}")
                            sub_content = get_content(sub_url)
                            if sub_content:
                                sub_root = ET.fromstring(sub_content)
                                for sub_child in sub_root.iter():
                                    if sub_child.tag.endswith('loc'):
                                        v_url = sub_child.text
                                        if v_url and '/vacatures/' in v_url:
                                            vacancy_urls.add(v_url)

        except ET.ParseError:
            print("   Kon XML niet lezen")
            continue

    return list(vacancy_urls)

def clean_text(text):
    if not text: return ""
    text = re.sub('<[^<]+?>', '', text)
    text = text.replace('\n', ' ').replace('\r', '').strip()
    return re.sub(' +', ' ', text)

def parse_job_page(url):
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
    else:
        if soup.title:
            job["Title"] = soup.title.get_text().split('-')[0].strip()

    # 2. BESCHRIJVING
    desc_div = soup.find('div', class_=re.compile(r'(content|entry|vacature-tekst|job-description)'))
    if desc_div:
        raw_desc = str(desc_div)[:800]
        job["Description"] = clean_text(raw_desc)[:200] + "..."
    else:
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta:
            job["Description"] = meta.get('content', '')[:200]

    # 3. CATEGORIE & SALARIS
    salary_match = re.search(r'€\s?(\d{1,3}\.?\d{0,3}).*?€\s?(\d{1,3}\.?\d{0,3})', soup.get_text())
    if salary_match:
        job["Salary"] = salary_match.group(0)
    
    categories = ['verpleegkundige', 'psychiater', 'begeleider', 'psycholoog', 'arts', 'ondersteunend']
    for cat in categories:
        if cat in job["Title"].lower():
            job["Category"] = cat.capitalize()
            break
    if not job["Category"]: job["Category"] = "Zorg"

    # 4. BEELD
    img = soup.find('meta', property='og:image')
    if img:
        job["Image URL"] = img['content']

    return job

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------
def main():
    print("🚀 Start Scraper v2.0 (Sitemap Mode)")
    
    links = extract_links_from_sitemap()
    
    if not links:
        print("⚠️ Geen links in sitemap. Fallback naar hoofdpagina...")
        main_soup = BeautifulSoup(get_content("[https://werkenbij.ggzingeest.nl/vacatures/](https://werkenbij.ggzingeest.nl/vacatures/)"), 'html.parser')
        for a in main_soup.find_all('a', href=True):
            if '/vacatures/' in a['href']:
                links.append(a['href'])
        links = list(set(links))

    print(f"✅ Totaal {len(links)} vacatures gevonden.")

    if len(links) == 0:
        print("❌ FATALE FOUT: Geen vacatures gevonden.")
        sys.exit(1)

    valid_jobs = []
    for i, link in enumerate(links):
        if i > 150: break
        print(f"   Verwerken {i+1}/{len(links)}: {link}")
        
        try:
            job = parse_job_page(link)
            if job and job["Title"] and "Overzicht" not in job["Title"]:
                valid_jobs.append(job)
            time.sleep(0.2)
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
