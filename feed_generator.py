import requests
from bs4 import BeautifulSoup
import csv
import json
import hashlib
import time
import re
import sys

# ------------------------------------------------------------------------------
# CONFIGURATIE
# ------------------------------------------------------------------------------
BASE_URL = "https://werkenbij.ggzingeest.nl"
VACANCY_OVERVIEW_URL = "https://werkenbij.ggzingeest.nl/vacatures/"
OUTPUT_FILE = "jobs_feed.csv"

# Google Ads "Jobs" Feed Header Specificaties
# Zie: https://support.google.com/google-ads/answer/6053288?hl=nl
CSV_HEADERS = [
    "Job ID",           # Unieke ID (verplicht)
    "Location ID",      # Locatie code (optioneel, wij gebruiken stad)
    "Title",            # Functietitel (verplicht)
    "Final URL",        # Link naar vacature (verplicht)
    "Image URL",        # Plaatje (sterk aanbevolen)
    "Category",         # Categorie (bijv. Verpleegkunde)
    "Description",      # Korte beschrijving
    "Salary",           # Salaris indicatie
    "Address"           # Stad/Regio
]

# Een standaard afbeelding voor als er geen specifiek plaatje gevonden wordt.
# Dit is het logo van GGZ inGeest (vervang dit evt. door een URL die jij host)
DEFAULT_IMAGE = "https://werkenbij.ggzingeest.nl/wp-content/themes/ggz-ingeest/assets/img/logo.svg"

# ------------------------------------------------------------------------------
# FUNCTIES
# ------------------------------------------------------------------------------

def get_soup(url):
    """Haalt een URL op en returnt een BeautifulSoup object."""
    try:
        # Headers zijn belangrijk om niet geblokkeerd te worden (lijkt op een echte browser)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"Fout bij ophalen {url}: {e}")
        return None

def extract_job_links(soup):
    """Zoekt naar vacature links op de overzichtspagina."""
    links = set()
    
    # We zoeken naar links die '/vacatures/' bevatten maar niet de index zelf zijn
    # Dit is een generieke methode die vaak werkt, zelfs als de CSS classes veranderen.
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        # Filter logica: moet vacatures in URL hebben, mag geen filter/mail/tel link zijn
        if '/vacatures/' in href and href != '/vacatures/' and 'javascript' not in href:
            full_url = href if href.startswith('http') else BASE_URL + href
            # Filter dubbele slashes of ankers weg
            full_url = full_url.split('#')[0]
            links.add(full_url)
            
    return list(links)

def parse_job_page(url):
    """Bezoekt een specifieke vacaturepagina en schraapt data."""
    soup = get_soup(url)
    if not soup:
        return None

    job_data = {
        "Job ID": "",
        "Location ID": "",
        "Title": "",
        "Final URL": url,
        "Image URL": DEFAULT_IMAGE,
        "Category": "",
        "Description": "",
        "Salary": "",
        "Address": ""
    }

    # STRATEGIE 1: Probeer JSON-LD (Schema.org) data te vinden.
    # Dit is de 'Gouden Standaard' voor vacaturesites.
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            data = json.loads(script.string)
            # Soms is het een lijst met schemas, soms 1 object
            if isinstance(data, list):
                items = data
            else:
                items = [data]
            
            for item in items:
                if item.get('@type') == 'JobPosting':
                    job_data["Title"] = item.get('title', '')
                    job_data["Description"] = clean_html(item.get('description', ''))
                    job_data["Category"] = item.get('occupationalCategory', '')
                    
                    # Locatie
                    if 'jobLocation' in item and 'address' in item['jobLocation']:
                        addr = item['jobLocation']['address']
                        if isinstance(addr, dict):
                            job_data["Address"] = addr.get('addressLocality', '')
                            job_data["Location ID"] = addr.get('addressLocality', '')
                    
                    # Plaatje
                    if 'image' in item:
                        if isinstance(item['image'], str):
                             job_data["Image URL"] = item['image']
                        elif isinstance(item['image'], list):
                             job_data["Image URL"] = item['image'][0]
                    
                    # Salaris
                    if 'baseSalary' in item:
                        val = item['baseSalary'].get('value', {})
                        if isinstance(val, dict):
                            min_sal = val.get('minValue')
                            max_sal = val.get('maxValue')
                            if min_sal and max_sal:
                                job_data["Salary"] = f"{min_sal} - {max_sal}"
                            elif val.get('value'):
                                job_data["Salary"] = str(val.get('value'))

                    break # We hebben de job gevonden, stop loop
        except:
            continue

    # STRATEGIE 2: Fallback naar HTML scraping als JSON-LD ontbreekt of incompleet is
    if not job_data["Title"]:
        # Zoek H1 titel
        h1 = soup.find('h1')
        if h1:
            job_data["Title"] = h1.get_text(strip=True)
    
    if not job_data["Description"]:
        # Zoek meta description
        meta = soup.find('meta', attrs={'name': 'description'})
        if meta:
            job_data["Description"] = meta.get('content', '')

    # CLEANUP & ID GENERATIE
    
    # Genereer een unieke ID op basis van de URL (blijft altijd hetzelfde voor dezelfde vacature)
    job_data["Job ID"] = hashlib.md5(url.encode('utf-8')).hexdigest()[:10]

    # Zorg dat image URL valide is
    if not job_data["Image URL"].startswith("http"):
        job_data["Image URL"] = DEFAULT_IMAGE
        
    # Fallback voor locatie als leeg
    if not job_data["Address"]:
        job_data["Address"] = "Regio Amsterdam" # Veilige aanname voor GGZ inGeest
        
    # Titel opschonen
    job_data["Title"] = job_data["Title"].replace("Vacature ", "").strip()

    return job_data

def clean_html(raw_html):
    """Verwijdert HTML tags voor een schone beschrijving."""
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    # Google Ads max description is vaak kort, we pakken de eerste 200 chars
    return cleantext[:200].replace('\n', ' ').strip() + "..."

# ------------------------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------------------------

def main():
    print("--- Start Feed Generator ---")
    print(f"Bezoek overzichtspagina: {VACANCY_OVERVIEW_URL}")
    
    overview_soup = get_soup(VACANCY_OVERVIEW_URL)
    if not overview_soup:
        print("Kon overzichtspagina niet laden. Stop.")
        sys.exit(1)
        
    links = extract_job_links(overview_soup)
    print(f"{len(links)} vacature links gevonden.")
    
    jobs = []
    
    for i, link in enumerate(links):
        print(f"Scrapen ({i+1}/{len(links)}): {link}")
        job_details = parse_job_page(link)
        
        if job_details and job_details["Title"]:
            jobs.append(job_details)
            # Korte pauze om de server niet te overbelasten (netjes)
            time.sleep(0.5)
    
    # CSV Schrijven
    print(f"Wegschrijven naar {OUTPUT_FILE}...")
    try:
        with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
            writer.writeheader()
            for job in jobs:
                writer.writerow(job)
        print("Klaar! Bestand succesvol gegenereerd.")
        
    except Exception as e:
        print(f"Fout bij schrijven bestand: {e}")

if __name__ == "__main__":
    main()