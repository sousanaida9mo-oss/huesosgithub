import smtplib
import socks
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from db import (
    SessionLocal, Account, HtmlTemplate, Preset, Subject, Proxy,
    get_setting
)
import config

def _apply_proxy(proxy: Optional[Proxy]):
    if not proxy:
        # Сброс socks, если ранее был установлен
        socket.socket = socket._socketobject if hasattr(socket, "_socketobject") else socket.socket
        return
    socks.setdefaultproxy(
        socks.SOCKS5,
        proxy.host,
        proxy.port,
        True if proxy.user else False,
        proxy.user,
        proxy.password
    )
    socket.socket = socks.socksocket

def render_subject(default_subject: str) -> str:
    inject = get_setting("inject_subject", "1") == "1"
    html_mode = get_setting("use_html_mailer", "1") == "1"
    html_subject = get_setting("html_subject", "") or default_subject
    if inject and html_mode and html_subject:
        return html_subject
    return default_subject

def render_body(text_body: str, html_template_id: Optional[int]) -> tuple[str, Optional[str]]:
    use_html = get_setting("use_html_mailer", "1") == "1"
    if use_html and html_template_id:
        with SessionLocal() as s:
            t = s.query(HtmlTemplate).filter_by(id=html_template_id).first()
            if t:
                return text_body, t.html
    return text_body, None

def send_email(
    account_id: int,
    to_email: str,
    subject: str,
    text_body: str,
    html_template_id: Optional[int] = None,
    proxy_id: Optional[int] = None,
) -> bool:
    with SessionLocal() as s:
        acc = s.query(Account).filter_by(id=account_id, active=True).first()
        if not acc:
            return False
        proxy = s.query(Proxy).filter_by(id=proxy_id).first() if proxy_id else None

    try:
        _apply_proxy(proxy)
        server = smtplib.SMTP(acc.smtp_server or config.DEFAULT_SMTP, acc.smtp_port or config.DEFAULT_SMTP_PORT, timeout=20)
        server.starttls()
        server.login(acc.email, acc.password)

        spoofing = get_setting("spoofing", "0") == "1"
        spoof_name = get_setting("spoofing_name", "") or acc.name
        from_header = f"{spoof_name} <{acc.email}>" if spoofing else f"{acc.name} <{acc.email}>"

        subj = render_subject(subject)
        text_body, html_body = render_body(text_body, html_template_id)

        msg = MIMEMultipart("alternative") if html_body else MIMEMultipart()
        msg["From"] = from_header
        msg["To"] = to_email
        msg["Subject"] = subj
        msg.attach(MIMEText(text_body or "", "plain", "utf-8"))
        if html_body:
            msg.attach(MIMEText(html_body, "html", "utf-8"))

        server.sendmail(acc.email, [to_email], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print("SMTP error:", e)
        return False