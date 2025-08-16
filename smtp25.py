import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment
import dns.resolver
import smtplib
import ssl
import re
import time
import os
import random
from typing import List, Tuple, Optional, Set, Dict
import socket
from functools import lru_cache
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import socks
from concurrent.futures import ThreadPoolExecutor, as_completed
import unicodedata
import argparse

# ====================== КОНФИГУРАЦИЯ ======================
DOMAINS_FILE = "domains.txt"
BLACKLIST_FILE = "blacklist.txt"
VERIFY_PROXY_FILE = "verify_proxy.txt"
SEND_PROXY_FILE = "send_proxy.txt"
TEMPLATES_FILE = "templates.txt"
SUBJECTS_FILE = "subjects.txt"
ACCOUNTS_FILE = "accounts.txt"
LOG_FILE = "errors.log"
SLEEP_TIME = 0
SMTP_TIMEOUT = 5
MAX_RETRIES = 2
MIN_SEND_DELAY = 3
MAX_SEND_DELAY = 6
THREADS = 300
SMTP_PORTS = [25]
MIN_NICK_LENGTH_WITH_SEPARATOR = 7
MIN_NICK_LENGTH_NO_SEPARATOR = 9
STOPWORDS_LAST = {
    "seller", "verkauf", "privat", "privatverkauf",
    "sale", "shop", "store", "katharina", "dominique", "elisabeth", "gmbh", "uni"
}
# ==========================================================

# Глобальные переменные
BLACKLIST_CACHE = set()
PROCESSED_NICKS_CACHE = set()
VERIFY_PROXY_LIST = []
SEND_PROXY_LIST = []
CURRENT_VERIFY_PROXY = None
CURRENT_SEND_PROXY = None
EMAIL_ACCOUNTS = []
TEMPLATES = []
SUBJECTS = []

TRANSLIT_TABLE = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
    'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
    'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
    'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
    'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
    'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
    'Ф': 'F', 'Х': 'H', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch',
    'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya'
}

def log_error(error_msg: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {error_msg}\n")

def transliterate(text: str) -> str:
    return ''.join(TRANSLIT_TABLE.get(c, c) for c in text)

def normalize_nick(nick: str) -> str:
    normalized = unicodedata.normalize('NFKD', nick)
    ascii_nick = normalized.encode('ascii', 'ignore').decode('ascii')
    return ascii_nick.lower()

def load_blacklist() -> Set[str]:
    blacklist = set()
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            blacklist.update(line.strip().lower() for line in f if line.strip())
    print(f"📋 Загружено {len(blacklist)} ников в черном списке")
    return blacklist

def load_domains(filename: str) -> List[str]:
    with open(filename, "r", encoding="utf-8") as f:
        domains = [d.strip().lower() for d in f.readlines() if d.strip()]
    print(f"🌐 Загружено {len(domains)} доменов")
    return domains

def is_smtp_port_open(ip: str, port: int, timeout: int = 5) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception as e:
        log_error(f"Порт {port} на {ip} закрыт: {str(e)}")
        return False

@lru_cache(maxsize=1000)
def check_mx_with_ports(domain: str) -> Tuple[bool, List[Tuple[str, int]]]:
    try:
        answers = dns.resolver.resolve(domain, 'MX', lifetime=5)
        mx_servers = []
        for r in answers:
            mx_host = str(r.exchange).rstrip('.')
            if is_smtp_port_open(mx_host, 25):
                mx_servers.append((mx_host, 25))
        return bool(mx_servers), mx_servers
    except Exception as e:
        log_error(f"Ошибка проверки MX для {domain}: {str(e)}")
        return False, []

def clean_email(email: str) -> str:
    return email

def verify_email_with_proxy(email: str, mx_servers: List[Tuple[str, int]], proxy: dict) -> bool:
    cleaned_email = clean_email(email)
    for mx_host, port in mx_servers:
        for attempt in range(MAX_RETRIES):
            try:
                socks.setdefaultproxy(
                    socks.SOCKS5,
                    proxy["host"],
                    proxy["port"],
                    True,
                    proxy["user"],
                    proxy["password"]
                )
                socket.socket = socks.socksocket

                custom_hostname = f"mx{random.randint(1,100)}.randomdomain.com"

                with smtplib.SMTP(mx_host, port, timeout=SMTP_TIMEOUT) as smtp:
                    smtp.ehlo(custom_hostname)
                    smtp.mail('noreply@randomdomain.com')
                    code, _ = smtp.rcpt(cleaned_email)

                if code in [250, 251]:
                    return True
                elif code == 554:
                    raise Exception("PTR block detected")
                
            except Exception as e:
                log_error(f"Ошибка проверки {email} через прокси {proxy['host']}:{proxy['port']} (попытка {attempt+1}): {str(e)}")
                continue
    
    return False

def verify_email(email: str, mx_servers: List[Tuple[str, int]]) -> bool:
    cleaned_email = clean_email(email)
    for mx_host, port in mx_servers:
        for attempt in range(MAX_RETRIES):
            try:
                with smtplib.SMTP(mx_host, port, timeout=SMTP_TIMEOUT) as smtp:
                    smtp.ehlo('example.com')
                    smtp.mail('noreply@example.com')
                    code, _ = smtp.rcpt(cleaned_email)
                
                if code == 250:
                    return True
            except Exception as e:
                log_error(f"Ошибка проверки {email} (попытка {attempt+1}): {str(e)}")
                continue
    return False

def load_proxies(proxy_type: str) -> List[dict]:
    proxy_file = VERIFY_PROXY_FILE if proxy_type == "verify" else SEND_PROXY_FILE
    
    if not os.path.exists(proxy_file):
        return []

    proxies = []
    with open(proxy_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.count(":") != 3:
                continue
            ip, port, user, password = line.split(":")
            proxies.append({
                "host": ip,
                "port": int(port),
                "user": user,
                "password": password
            })

    print(f"🎯 Загружено {len(proxies)} прокси ({proxy_type})")
    return proxies

_proxy_state = {
    "send": {"i": -1, "last_id": None},
    "verify": {"i": -1, "last_id": None},
}

def reset_proxy_rotation(kind: str | None = None) -> None:
    """
    Опционально можно вызвать после массовых изменений прокси.
    Если kind не указан, сбрасывает оба.
    """
    kinds = [kind] if kind in ("send", "verify") else ["send", "verify"]
    for k in kinds:
        _proxy_state[k] = {"i": -1, "last_id": None}

def get_next_proxy(kind: str) -> dict | None:
    """
    Возвращает следующий прокси для kind in {"send", "verify"}.
    Устойчиво к перезагрузке списка прокси: не использует list.index(dict).
    Ставит курсор по id, если удаётся, иначе начинает с начала.
    """
    proxy_list = SEND_PROXY_LIST if kind == "send" else VERIFY_PROXY_LIST if kind == "verify" else None
    if not proxy_list:
        return None

    st = _proxy_state.setdefault(kind, {"i": -1, "last_id": None})

    # Если ранее что-то выбирали — попробуем восстановиться по last_id в новом списке
    if st["last_id"] is not None:
        try:
            pos = next((idx for idx, p in enumerate(proxy_list) if p.get("id") == st["last_id"]), None)
        except Exception:
            pos = None
        if pos is None:
            # список изменился — начинаем сначала
            st["i"] = -1

    # Продвинуться на 1
    st["i"] = (st["i"] + 1) % len(proxy_list)
    pr = proxy_list[st["i"]]
    st["last_id"] = pr.get("id")
    return pr

def load_templates() -> List[str]:
    templates = []
    if not os.path.exists(TEMPLATES_FILE):
        return templates
    
    with open(TEMPLATES_FILE, "r", encoding="utf-8") as f:
        current_template = []
        for line in f:
            line = line.strip()
            if line == "=":
                if current_template:
                    templates.append("\n".join(current_template))
                    current_template = []
            else:
                current_template.append(line)
        
        if current_template:
            templates.append("\n".join(current_template))
    
    print(f"📝 Загружено {len(templates)} шаблонов писем")
    return templates

def load_subjects() -> List[str]:
    subjects = []
    if not os.path.exists(SUBJECTS_FILE):
        return subjects
    
    with open(SUBJECTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                subjects.append(line)
    
    print(f"📌 Загружено {len(subjects)} тем писем")
    return subjects

def load_email_accounts() -> List[Dict[str, str]]:
    accounts = []
    if not os.path.exists(ACCOUNTS_FILE):
        return accounts
    
    with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                name, credentials = line.split("|")
                login, password = credentials.split(":")
                email = login.split(":")[0] if ":" in login else login
                accounts.append({
                    "name": name.strip(),
                    "email": email.strip(),
                    "password": password.strip()
                })
            except ValueError as e:
                log_error(f"Ошибка разбора строки аккаунта: {line} - {str(e)}")
                continue
    
    print(f"📧 Загружено {len(accounts)} email аккаунтов")
    return accounts

def get_random_template() -> str:
    if not TEMPLATES:
        raise ValueError("Нет доступных шаблонов писем")
    return random.choice(TEMPLATES)

def get_random_subject() -> str:
    if not SUBJECTS:
        return "Вопрос по товару"
    return random.choice(SUBJECTS)

def get_random_account() -> Dict[str, str]:
    if not EMAIL_ACCOUNTS:
        raise ValueError("Нет доступных email аккаунтов")
    return random.choice(EMAIL_ACCOUNTS)

def initialize_smtp(account: dict, proxy: dict) -> Optional[smtplib.SMTP]:
    try:
        socks.setdefaultproxy(
            socks.SOCKS5,
            proxy["host"],
            proxy["port"],
            True,
            proxy["user"],
            proxy["password"]
        )
        socket.socket = socks.socksocket

        smtp = smtplib.SMTP("smtp.gmail.com", 587, timeout=SMTP_TIMEOUT)
        smtp.starttls()
        smtp.login(account["email"], account["password"])
        return smtp
    except Exception as e:
        log_error(f"Ошибка инициализации SMTP для {account['email']}: {str(e)}")
        return None

def extract_seller_name(nick: str) -> Optional[str]:
    if not isinstance(nick, str) or not nick.strip():
        return None
    
    original_nick = nick.strip()
    
    # Пытаемся извлечь ник из скобок
    nick_in_brackets = None
    if '(' in original_nick and ')' in original_nick:
        match = re.search(r'\((.*?)\)', original_nick)
        if match:
            nick_in_brackets = match.group(1).strip()
    
    # Сначала пробуем ник из скобок
    if nick_in_brackets:
        parts = nick_in_brackets.split()
        if len(parts) > 1 and len(parts[-1]) == 1:
            parts = parts[:-1]
        if parts:
            return parts[0][0].upper() + parts[0][1:].lower()
    
    # Если из скобок ничего не получилось, пробуем основной ник
    main_nick = re.sub(r'\(.*?\)', '', original_nick).strip()
    parts = main_nick.split()
    if len(parts) > 1 and len(parts[-1]) == 1:
        parts = parts[:-1]
    
    if not parts:
        return None
        
    return parts[0][0].upper() + parts[0][1:].lower()

def extract_name_parts(nick: str) -> Optional[Tuple[str, str]]:
    original_nick = str(nick).strip()
    
    # 1. Пытаемся извлечь ник из скобок (приоритет)
    nick_in_brackets = None
    if '(' in original_nick and ')' in original_nick:
        match = re.search(r'\((.*?)\)', original_nick)
        if match:
            nick_in_brackets = match.group(1).strip()
    
    # 2. Если в скобках есть валидный ник — используем его
    if nick_in_brackets:
        parts = re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s._]', '', nick_in_brackets).split()
        if parts:
            if len(parts) == 1 and len(parts[0]) >= MIN_NICK_LENGTH_NO_SEPARATOR:
                return parts[0].capitalize(), ""
            elif len(parts) >= 2 and len(' '.join(parts)) >= MIN_NICK_LENGTH_WITH_SEPARATOR:
                return parts[0].capitalize(), ' '.join(parts[1:]).capitalize()
    
    # 3. Если скобок нет или их содержимое невалидно — обрабатываем основной ник
    main_nick = re.sub(r'\(.*?\)', '', original_nick).strip()
    parts = re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s.]', '', main_nick).split()
    if len(parts) > 1 and len(parts[-1]) == 1:
        parts = parts[:-1]
    
    if not parts:
        return None
        
    if len(parts) == 1:
        if len(parts[0]) >= MIN_NICK_LENGTH_NO_SEPARATOR:
            return parts[0].capitalize(), ""
        return None
    
    if len(parts) >= 2:
        if len(' '.join(parts)) < MIN_NICK_LENGTH_WITH_SEPARATOR:
            return None
        return parts[0].capitalize(), ' '.join(parts[1:]).capitalize()
    
    return None

def generate_email(first_name: str, last_name: str) -> str:
    first = transliterate(first_name.lower())
    last = transliterate(last_name.lower()) if last_name else ""
    
    if last:
        return f"{first}.{last}"
    return first

def process_row(row: pd.Series, domains: List[str]) -> Optional[Tuple[str, str]]:
    nick = str(row["seller_nick"]).strip()
    normalized_nick = normalize_nick(nick)
    
    # Первичная проверка черного списка
    if normalized_nick in BLACKLIST_CACHE:
        return None
        
    if normalized_nick in PROCESSED_NICKS_CACHE:
        return None
    
    name_parts = extract_name_parts(nick)
    if not name_parts:
        PROCESSED_NICKS_CACHE.add(normalized_nick)
        return None
        
    first_name, last_name = name_parts
    
    if len(first_name) < 3 or (last_name and len(last_name) < 3):
        PROCESSED_NICKS_CACHE.add(normalized_nick)
        return None
    
    email_base = generate_email(first_name, last_name)
    seller_name = extract_seller_name(nick) if ' ' in nick else None
    
    if not email_base:
        PROCESSED_NICKS_CACHE.add(normalized_nick)
        return None

    for domain in domains:
        email = f"{email_base}@{domain}"
        
        if email.lower() in BLACKLIST_CACHE:
            continue
            
        cleaned_email = clean_email(email)
        has_mx, mx_servers = check_mx_with_ports(domain)
        
        if not has_mx:
            continue
            
        try:
            if VERIFY_PROXY_LIST:
                proxy = get_next_proxy("verify")
                if proxy and verify_email_with_proxy(cleaned_email, mx_servers, proxy):
                    print(f"✅ Найден валидный email (через прокси): {email}")
                    save_to_blacklist(normalized_nick)
                    PROCESSED_NICKS_CACHE.add(normalized_nick)
                    return email, seller_name
            
            if verify_email(cleaned_email, mx_servers):
                print(f"✅ Найден валидный email: {email}")
                save_to_blacklist(normalized_nick)
                PROCESSED_NICKS_CACHE.add(normalized_nick)
                return email, seller_name
                
        except Exception as e:
            log_error(f"Ошибка обработки email {email}: {str(e)}")
            continue

    PROCESSED_NICKS_CACHE.add(normalized_nick)
    return None

def save_to_blacklist(nick: str):
    with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
        f.write(f"{nick.lower()}\n")
    BLACKLIST_CACHE.add(nick.lower())

def send_email(to_email: str, seller_name: str, item_name: str) -> bool:
    for attempt in range(3):  # 3 попытки отправки
        try:
            account = get_random_account()
            proxy = get_next_proxy("send")
            
            if not proxy:
                error_msg = "Нет доступных прокси для отправки"
                print(f"⚠️ {error_msg}")
                log_error(error_msg)
                continue
                
            # Для повторных попыток делаем небольшую паузу (1 сек)
            if attempt > 0:
                time.sleep(1)
            
            smtp = initialize_smtp(account, proxy)
            if not smtp:
                continue
                
            template = get_random_template()
            subject_template = get_random_subject()
            
            if seller_name is None:
                subject = subject_template.replace("{ITEM}", item_name).replace("{SELLER}", "").strip()
                email_body = template.replace("{SELLER}", "").replace("{ITEM}", item_name)
            else:
                subject = subject_template.replace("{ITEM}", item_name).replace("{SELLER}", seller_name)
                email_body = template.replace("{SELLER}", seller_name).replace("{ITEM}", item_name)
            
            msg = MIMEMultipart()
            msg['From'] = f"{account['name']} <{account['email']}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(email_body, 'plain'))
            
            smtp.sendmail(account["email"], to_email, msg.as_string())
            smtp.quit()
            
            print(f"✉️ Отправлено письмо на {to_email}")
            return True
            
        except Exception as e:
            error_msg = f"Ошибка отправки на {to_email} (попытка {attempt+1}): {str(e)}"
            print(f"⚠️ {error_msg}")
            log_error(error_msg)
    
    return False

def detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    col_mapping = {
        'seller_nick': None,
        'title': None,
        'price': None,
        'link': None
    }
    
    for col in df.columns:
        col_lower = col.strip().lower()
        if 'ник' in col_lower and 'продавца' in col_lower:
            col_mapping['seller_nick'] = col
        elif 'название' in col_lower and 'товара' in col_lower:
            col_mapping['title'] = col
        elif 'цена' in col_lower:
            col_mapping['price'] = col
        elif 'ссылка' in col_lower and 'товар' in col_lower:
            col_mapping['link'] = col
    
    if not col_mapping['seller_nick'] and 'Имя продавца' in df.columns:
        col_mapping['seller_nick'] = 'Имя продавца'
    if not col_mapping['title'] and 'Название' in df.columns:
        col_mapping['title'] = 'Название'
    if not col_mapping['link'] and 'Ссылка на объявление' in df.columns:
        col_mapping['link'] = 'Ссылка на объявление'
    
    return col_mapping

def load_resources() -> List[str]:
    global BLACKLIST_CACHE, EMAIL_ACCOUNTS, TEMPLATES, SUBJECTS, VERIFY_PROXY_LIST, SEND_PROXY_LIST
    
    BLACKLIST_CACHE = load_blacklist()
    print(f"📋 Загружено {len(BLACKLIST_CACHE)} ников в черном списке")
    
    domains = load_domains(DOMAINS_FILE)
    
    EMAIL_ACCOUNTS = load_email_accounts()
    TEMPLATES = load_templates()
    SUBJECTS = load_subjects()
    VERIFY_PROXY_LIST = load_proxies("verify")
    SEND_PROXY_LIST = load_proxies("send")
    
    return domains

def main(input_file: str):
    print("🎯 Email Verifier & Sender v3.5 🎯")
    print("🔍 Проверка только порта 25 | ✉️ Отправка через прокси")
    
    try:
        domains = load_resources()
        df = pd.read_excel(input_file)
        
        col_mapping = detect_columns(df)
        
        missing = [k for k, v in col_mapping.items() if v is None]
        if missing:
            error_msg = f"Не найдены колонки: {', '.join(missing)}"
            print(f"\n❌ {error_msg}")
            log_error(error_msg)
            return
        
        df = df.rename(columns={v: k for k, v in col_mapping.items() if v})
        df = df[list(col_mapping.keys())]
        
        print("\n🔍 Начинаем проверку email (многопоточная, порт 25)...")
        df["email"] = None
        df["seller_name"] = None
        
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {
                executor.submit(process_row, row, domains): idx
                for idx, row in df.iterrows()
            }
            for future in as_completed(futures):
                idx = futures[future]
                result = future.result()
                if result:
                    df.at[idx, "email"], df.at[idx, "seller_name"] = result
        
        result_df = df[df["email"].notna()].copy()
        
        if not result_df.empty:
            print("\n✉️ Начинаем отправку писем через прокси...")
            sent_count = 0
            last_message_length = 0
            
            for _, row in result_df.iterrows():
                success = send_email(
                    to_email=row["email"],
                    seller_name=row["seller_name"],
                    item_name=row["title"]
                )
                
                if success:
                    sent_count += 1
                    if sent_count < len(result_df):  # Не показывать задержку после последнего письма
                        delay = random.uniform(MIN_SEND_DELAY, MAX_SEND_DELAY)
                        delay_msg = f"⏳ Следующее письмо через {delay:.1f} сек..."
                        print(delay_msg, end='\r', flush=True)
                        time.sleep(delay)
                        # Очищаем строку с сообщением о задержке
                        print(' ' * len(delay_msg), end='\r', flush=True)
            
            print(f"\n✉️ Отправлено {sent_count} писем")
            
            domain_stats = result_df["email"].str.split("@").str[1].value_counts()
            print("\n📊 Статистика по доменам:")
            print(domain_stats)
        else:
            print("\n😢 Валидные email не найдены")
            
    except Exception as e:
        error_msg = f"Критическая ошибка: {str(e)}"
        print(f"\n❌ {error_msg}")
        log_error(error_msg)
    finally:
        input("\nНажмите Enter для выхода...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Email Verifier & Sender')
    parser.add_argument('input_file', type=str, help='Path to input Excel file')
    args = parser.parse_args()
    
    main(args.input_file)