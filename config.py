import os
from dotenv import load_dotenv
from sqlalchemy.pool import NullPool

load_dotenv()


class Config:
    # PostgreSQL
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "poolclass": NullPool,
    }

    # Flask
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-in-prod")

    # Salesforce
    SF_USERNAME = os.getenv("SF_USERNAME")
    SF_PASSWORD = os.getenv("SF_PASSWORD")
    SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
    SF_DOMAIN = os.getenv("SF_DOMAIN", "login")
    SF_TRADE_OBJECT = os.getenv("SF_TRADE_OBJECT", "")  # e.g. Trade__c

    # Cotton feature flags
    # Gate cotton → Salesforce push-back until BOOK_TO_SF mapping is confirmed.
    # Set COTTON_SF_PUSH_ENABLED=true in .env to enable.
    COTTON_SF_PUSH_ENABLED = os.getenv("COTTON_SF_PUSH_ENABLED", "false").lower() == "true"
