from __future__ import annotations
import os
import datetime as dt
from typing import Optional, Iterable

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, ForeignKey,
    Text, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

DB_URL = os.getenv("DATABASE_URL", "sqlite:///bot.db")

engine = create_engine(DB_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    tg_id = Column(Integer, unique=True, index=True, nullable=False)
    username = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=dt.datetime.utcnow)

    accounts = relationship("Account", back_populates="user", cascade="all, delete-orphan")
    proxies = relationship("Proxy", back_populates="user", cascade="all, delete-orphan")
    domains = relationship("Domain", back_populates="user", cascade="all, delete-orphan")
    templates = relationship("HtmlTemplate", back_populates="user", cascade="all, delete-orphan")
    subjects = relationship("Subject", back_populates="user", cascade="all, delete-orphan")
    presets = relationship("Preset", back_populates="user", cascade="all, delete-orphan")
    smart_presets = relationship("SmartPreset", back_populates="user", cascade="all, delete-orphan")
    blacklist = relationship("BlacklistBase", back_populates="user", cascade="all, delete-orphan")
    settings = relationship("Setting", back_populates="user", cascade="all, delete-orphan")
    incoming = relationship("IncomingMessage", back_populates="user", cascade="all, delete-orphan")

class Setting(Base):
    __tablename__ = "settings"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    key = Column(String(128), index=True)
    value = Column(Text)
    user = relationship("User", back_populates="settings")
    __table_args__ = (UniqueConstraint("user_id", "key", name="uq_user_setting"),)

class Domain(Base):
    __tablename__ = "domains"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name = Column(String(255), index=True)
    order_index = Column(Integer, default=0, index=True)
    user = relationship("User", back_populates="domains")
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_user_domain"),)

class Proxy(Base):
    __tablename__ = "proxies"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    host = Column(String(255))
    port = Column(Integer)
    user_login = Column(String(255), nullable=True)
    password = Column(String(255), nullable=True)
    type = Column(String(20), default="send")  # send | verify
    active = Column(Boolean, default=True)
    user = relationship("User", back_populates="proxies")

class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    display_name = Column(String(255))
    email = Column(String(255))
    password = Column(String(255))
    proxy_id = Column(Integer, ForeignKey("proxies.id", ondelete="SET NULL"), nullable=True)
    active = Column(Boolean, default=True)
    user = relationship("User", back_populates="accounts")
    proxy = relationship("Proxy")
    incoming = relationship("IncomingMessage", back_populates="account", cascade="all, delete-orphan")

class HtmlTemplate(Base):
    __tablename__ = "html_templates"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    title = Column(String(255))
    html = Column(Text)
    user = relationship("User", back_populates="templates")

class Preset(Base):
    __tablename__ = "presets"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    title = Column(String(255))
    body = Column(Text)
    user = relationship("User", back_populates="presets")

class SmartPreset(Base):
    __tablename__ = "smart_presets"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    title = Column(String(255))
    body = Column(Text)
    user = relationship("User", back_populates="smart_presets")

class Subject(Base):
    __tablename__ = "subjects"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    title = Column(Text)
    user = relationship("User", back_populates="subjects")

class BlacklistBase(Base):
    __tablename__ = "blacklist_bases"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    base = Column(String(255), index=True)
    user = relationship("User", back_populates="blacklist")
    __table_args__ = (UniqueConstraint("user_id", "base", name="uq_user_blacklist_base"),)

class IncomingMessage(Base):
    __tablename__ = "incoming_messages"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), index=True)
    account_id = Column(Integer, ForeignKey("accounts.id", ondelete="CASCADE"), index=True)
    uid = Column(String(255))  # IMAP UID
    from_name = Column(String(255))
    from_email = Column(String(255))
    subject = Column(Text)
    body = Column(Text)
    answered = Column(Boolean, default=False)
    tg_message_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=dt.datetime.utcnow)

    user = relationship("User", back_populates="incoming")
    account = relationship("Account", back_populates="incoming")
    __table_args__ = (UniqueConstraint("account_id", "uid", name="uq_acc_uid"),)

def init_db():
    Base.metadata.create_all(engine)

# --------- HELPERS (короткая версия) ---------

def get_or_create_user(s: Session, tg_id: int, username: Optional[str], first_name: Optional[str], last_name: Optional[str]) -> User:
    u = s.query(User).filter_by(tg_id=tg_id).first()
    if u:
        u.username = username or u.username
        u.first_name = first_name or u.first_name
        u.last_name = last_name or u.last_name
        s.commit()
        return u
    u = User(tg_id=tg_id, username=username, first_name=first_name, last_name=last_name, status="pending")
    s.add(u); s.commit(); s.refresh(u)
    return u

def approve_user(s: Session, user_id: int, approved: bool):
    u = s.query(User).filter_by(id=user_id).first()
    if u:
        u.status = "approved" if approved else "denied"
        s.commit()

def get_setting(user_id: int, key: str, default: Optional[str] = None) -> Optional[str]:
    with SessionLocal() as s:
        st = s.query(Setting).filter_by(user_id=user_id, key=key).first()
        return st.value if st else default

def set_setting(user_id: int, key: str, value: str):
    with SessionLocal() as s:
        st = s.query(Setting).filter_by(user_id=user_id, key=key).first()
        if not st:
            st = Setting(user_id=user_id, key=key, value=value); s.add(st)
        else:
            st.value = value
        s.commit()

def list_domains(s: Session, user_id: int) -> list[str]:
    doms = s.query(Domain).filter_by(user_id=user_id).order_by(Domain.order_index.asc(), Domain.id.asc()).all()
    return [d.name for d in doms]

def set_domains_order(s: Session, user_id: int, names_in_order: Iterable[str]):
    s.query(Domain).filter_by(user_id=user_id).delete()
    for idx, name in enumerate(names_in_order):
        s.add(Domain(user_id=user_id, name=name.strip().lower(), order_index=idx))
    s.commit()

def add_domain(s: Session, user_id: int, name: str, position: Optional[int] = None):
    names = list_domains(s, user_id); name = name.strip().lower()
    if position is None or position < 1 or position > len(names) + 1:
        names.append(name)
    else:
        names.insert(position - 1, name)
    set_domains_order(s, user_id, names)

def delete_domains_by_indices(s: Session, user_id: int, indices: list[int]):
    names = list_domains(s, user_id)
    for i in sorted(set(indices), reverse=True):
        if 1 <= i <= len(names): names.pop(i - 1)
    set_domains_order(s, user_id, names)

def clear_domains(s: Session, user_id: int):
    s.query(Domain).filter_by(user_id=user_id).delete(); s.commit()

def get_proxies(s: Session, user_id: int, ptype: str) -> list["Proxy"]:
    return s.query(Proxy).filter_by(user_id=user_id, type=ptype, active=True).all()

def get_random_send_proxy(s: Session, user_id: int) -> Optional["Proxy"]:
    import random
    items = get_proxies(s, user_id, "send")
    return random.choice(items) if items else None

def add_account(s: Session, user_id: int, display_name: str, email: str, password: str, auto_bind_proxy: bool = True) -> "Account":
    acc = Account(user_id=user_id, display_name=display_name.strip(), email=email.strip(), password=password.strip())
    if auto_bind_proxy:
        prx = get_random_send_proxy(s, user_id)
        if prx: acc.proxy_id = prx.id
    s.add(acc); s.commit(); s.refresh(acc)
    return acc

def update_account(s: Session, user_id: int, account_id: int, display_name: Optional[str] = None,
                   email: Optional[str] = None, password: Optional[str] = None, proxy_id: Optional[int] = None) -> Optional["Account"]:
    acc = s.query(Account).filter_by(user_id=user_id, id=account_id).first()
    if not acc: return None
    if display_name is not None: acc.display_name = display_name
    if email is not None: acc.email = email
    if password is not None: acc.password = password
    if proxy_id is not None: acc.proxy_id = proxy_id
    s.commit(); return acc

def delete_account(s: Session, user_id: int, account_id: int):
    s.query(Account).filter_by(user_id=user_id, id=account_id).delete(); s.commit()

def clear_accounts(s: Session, user_id: int):
    s.query(Account).filter_by(user_id=user_id).delete(); s.commit()

def get_blacklist_set(s: Session, user_id: int) -> set[str]:
    return set(x.base for x in s.query(BlacklistBase).filter_by(user_id=user_id).all())

def add_blacklist_base(s: Session, user_id: int, base: str):
    base = base.strip().lower()
    if base and not s.query(BlacklistBase).filter_by(user_id=user_id, base=base).first():
        s.add(BlacklistBase(user_id=user_id, base=base)); s.commit()

init_db()