"""
API Flask pour extraction des taux de change EUR de la BCE
+ extraction EUR -> TND depuis ExchangeRate-API (free latest endpoint)
+ EMAIL ALERTS en cas d'échec
"""
import os
import sys
import asyncio
import logging
import time
from datetime import datetime, date
import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request
from flask_mail import Mail, Message
from flasgger import Swagger
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import json
# ==============================
# CONFIG asyncio (Windows)
# ==============================
if sys.platform.startswith("win") and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ==============================
# LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("currency_api")
logger.info("=" * 80)
logger.info(" DÉMARRAGE DE L'API FX (ECB + EXCHANGERATE-API)")
logger.info("=" * 80)

# ==============================
# CONFIGURATION BASE DE DONNÉES
# ==============================
DB_CONFIG = {
    "user": os.getenv("ECB_DB_USER", "administrationSTS"),
    "password": os.getenv("ECB_DB_PASSWORD", "St$@0987"),
    "host": os.getenv("ECB_DB_HOST", "avo-adb-002.postgres.database.azure.com"),
    "port": os.getenv("ECB_DB_PORT", "5432"),
    "database": os.getenv("ECB_DB_NAME", "LME_DB"),
    "sslmode": "require",
}

def get_db_connection():
    """Créer une connexion à la base de données PostgreSQL."""
    if not DB_CONFIG["password"]:
        raise RuntimeError("ECB_DB_PASSWORD est vide. Définis la variable d'environnement ECB_DB_PASSWORD.")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Connexion à la base de données établie")
        return conn
    except Exception as e:
        logger.error(f"✗ Erreur de connexion à la base de données: {e}")
        raise

# ==============================
# CONFIGURATION ECB
# ==============================
ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
BASE_CURRENCY = "EUR"

TARGET_CURRENCIES = ["CNY", "EUR", "INR", "KRW", "MXN", "USD"]
logger.info(f"Devises ECB ciblées: {', '.join(TARGET_CURRENCIES)}")

# ==============================
# CONFIGURATION EXCHANGERATE-API (Free)
# ==============================
EXR_API_KEY = os.getenv("EXR_API_KEY", "bfcac5f634ebfed7dbd26ab9")
EXR_BASE_CURRENCY = "EUR"
EXR_TARGET_CURRENCY = "TND"
EXR_LATEST_URL = f"https://v6.exchangerate-api.com/v6/{EXR_API_KEY}/latest/{EXR_BASE_CURRENCY}"
logger.info(f"Devise ExchangeRate-API ciblée: {EXR_TARGET_CURRENCY}")

# ==============================
# FLASK APP INITIALIZATION
# ==============================
app = Flask(__name__)

# ==============================
# CONFIGURATION EMAIL
# ==============================
app.config['MAIL_SERVER'] = 'avocarbon-com.mail.protection.outlook.com'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = None
app.config['MAIL_PASSWORD'] = None
app.config['MAIL_DEFAULT_SENDER'] = 'administration.STS@avocarbon.com'

mail = Mail(app)

# Email de notification
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "hadil.sakouhi@avocarbon.com")

logger.info("📧 Configuration email initialisée")
logger.info(f"📧 Alertes envoyées à: {ALERT_EMAIL}")

# ==============================
# FONCTIONS EMAIL
# ==============================
def send_alert_email(subject, body, error_details=None):
    """
    Envoyer un email d'alerte en cas d'échec de scraping.
    """
    try:
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #d32f2f; color: white; padding: 15px; }}
                .content {{ padding: 20px; }}
                .error-box {{ background-color: #ffebee; border-left: 4px solid #d32f2f; padding: 15px; margin: 15px 0; }}
                .footer {{ color: #666; font-size: 12px; margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>⚠️ Alerte - Échec de Synchronisation des Taux de Change</h2>
            </div>
            <div class="content">
                <p><strong>Date/Heure:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>{body}</p>
                
                {f'<div class="error-box"><strong>Détails de l\'erreur:</strong><br><pre>{error_details}</pre></div>' if error_details else ''}
                
                <p>Veuillez vérifier les logs de l'application et corriger le problème dès que possible.</p>
                
                <p><strong>Actions recommandées:</strong></p>
                <ul>
                    <li>Vérifier la connectivité réseau</li>
                    <li>Vérifier la disponibilité des sources de données (ECB, ExchangeRate-API)</li>
                    <li>Consulter les logs système pour plus de détails</li>
                    <li>Tester manuellement via les endpoints /ecb/extract ou /exr/extract</li>
                </ul>
            </div>
            <div class="footer">
                <p>Cet email a été généré automatiquement par le système Currency API.</p>
                <p>Pour consulter les logs: <a href="/sync/logs">/sync/logs</a></p>
            </div>
        </body>
        </html>
        """
        
        msg = Message(
            subject=subject,
            recipients=[ALERT_EMAIL],
            html=html_body
        )
        
        mail.send(msg)
        logger.info(f"📧 Email d'alerte envoyé à {ALERT_EMAIL}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Échec d'envoi d'email d'alerte: {e}")
        return False


def send_success_summary_email(ecb_result, exr_result):
    """
    Envoyer un email de résumé quotidien (optionnel).
    """
    try:
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .header {{ background-color: #2e7d32; color: white; padding: 15px; }}
                .content {{ padding: 20px; }}
                .success-box {{ background-color: #e8f5e9; border-left: 4px solid #2e7d32; padding: 15px; margin: 15px 0; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4caf50; color: white; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>✅ Synchronisation des Taux de Change Réussie</h2>
            </div>
            <div class="content">
                <p><strong>Date/Heure:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="success-box">
                    <h3>📊 Résumé ECB</h3>
                    <p><strong>Status:</strong> {ecb_result.get('status', 'N/A')}</p>
                    <p><strong>Date de référence:</strong> {ecb_result.get('ref_date', 'N/A')}</p>
                    <p><strong>Taux enregistrés:</strong> {ecb_result.get('rates_saved', 0)}</p>
                </div>
                
                <div class="success-box">
                    <h3>📊 Résumé ExchangeRate-API (TND)</h3>
                    <p><strong>Status:</strong> {exr_result.get('status', 'N/A')}</p>
                    <p><strong>Date de référence:</strong> {exr_result.get('ref_date', 'N/A')}</p>
                    <p><strong>Taux EUR→TND:</strong> {exr_result.get('tnd_rate', 'N/A')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        msg = Message(
            subject="✅ Synchronisation quotidienne des taux - Succès",
            recipients=[ALERT_EMAIL],
            html=html_body
        )
        
        mail.send(msg)
        logger.info(f"📧 Email de résumé envoyé à {ALERT_EMAIL}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Échec d'envoi d'email de résumé: {e}")
        return False

# ==============================
# FONCTIONS DB
# ==============================
def save_ecb_rates_to_db(ref_date: date, rates: dict, source_url: str, metadata: dict = None) -> int:
    """
    Enregistrer les taux dans la base (ECB ou ExchangeRate-API).
    """
    saved_count = 0
    metadata = metadata or {}

    insert_query = """
        INSERT INTO ecb_exchange_rates (
            ref_date, base_currency, quote_currency, rate, source_url, created_at, metadata
        )
        VALUES (%s, %s, %s, %s, %s, NOW(), %s::jsonb)
        ON CONFLICT (ref_date, base_currency, quote_currency)
        DO UPDATE SET
            rate = EXCLUDED.rate,
            source_url = EXCLUDED.source_url,
            created_at = NOW(),
            metadata = EXCLUDED.metadata;
    """

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        for currency, rate in rates.items():
            cursor.execute(
                insert_query,
                (
                    ref_date,
                    BASE_CURRENCY,
                    currency,
                    rate,
                    source_url,
                    json.dumps(metadata),
                )
            )
            saved_count += 1
            logger.info(f"    Enregistré: {ref_date} | 1 {BASE_CURRENCY} = {rate} {currency}")

        conn.commit()
        logger.info(f"✓ {saved_count} taux enregistrés/mis à jour dans ecb_exchange_rates")
        return saved_count

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"✗ Erreur lors de l'enregistrement des taux: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("✓ Connexion DB fermée")


def log_sync_operation(sync_type, status, records, error_message=None, duration=None):
    """
    Enregistrer une opération de synchronisation dans sync_logs.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO sync_logs
            (sync_type, status, metals_updated, error_message, duration_seconds, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW());
        """

        cursor.execute(insert_query, (sync_type, status, records, error_message, duration))
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"✗ Erreur lors de l'enregistrement du log: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ==============================
# EXTRACTION ECB
# ==============================
def fetch_ecb_daily_rates():
    logger.info(f"🌐 Requête ECB vers: {ECB_DAILY_URL}")
    resp = requests.get(ECB_DAILY_URL, timeout=30)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)

    rates = {}
    ref_date = None

    for cube_time in root.iter():
        if cube_time.tag.endswith("Cube") and "time" in cube_time.attrib:
            ref_date_str = cube_time.attrib["time"]
            ref_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()

            for cube in cube_time:
                if "currency" in cube.attrib and "rate" in cube.attrib:
                    currency = cube.attrib["currency"]
                    if currency not in TARGET_CURRENCIES:
                        continue
                    rates[currency] = float(cube.attrib["rate"])
            break

    if "EUR" in TARGET_CURRENCIES:
        rates["EUR"] = 1.0

    if ref_date is None or not rates:
        raise ValueError("Impossible d'extraire les taux ECB")

    return ref_date, rates, ECB_DAILY_URL


def scrape_ecb_and_save(sync_type="api", send_email_on_error=True):
    start_time = time.time()
    try:
        ref_date, rates, url = fetch_ecb_daily_rates()
        metadata = {"source": "ecb_xml", "fetched_at": datetime.now().isoformat()}
        saved_count = save_ecb_rates_to_db(ref_date, rates, url, metadata)
        duration = time.time() - start_time
        log_sync_operation(sync_type, "success", saved_count, None, duration)
        logger.info(f"✅ ECB sync réussi: {saved_count} taux enregistrés")
        return {"status": "success", "ref_date": ref_date.isoformat(), "rates_saved": saved_count}
    except Exception as e:
        duration = time.time() - start_time
        error_msg = str(e)
        log_sync_operation(sync_type, "failed", 0, error_msg, duration)
        logger.error(f"❌ Échec ECB sync: {error_msg}")
        
        # Envoyer email d'alerte
        if send_email_on_error:
            send_alert_email(
                subject="🚨 ALERTE - Échec synchronisation ECB",
                body=f"La synchronisation des taux de change ECB a échoué lors de l'exécution {sync_type}.",
                error_details=error_msg
            )
        
        return {"status": "error", "message": error_msg, "sync_type": sync_type}

# ==============================
# EXTRACTION EXCHANGERATE-API (TND)
# ==============================
def fetch_exr_latest_tnd_rate():
    logger.info(f"🌐 Requête ExchangeRate-API vers: {EXR_LATEST_URL}")
    resp = requests.get(EXR_LATEST_URL, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if data.get("result") != "success":
        raise ValueError(f"ExchangeRate-API error: {data.get('error-type')}")

    tnd_rate = float(data["conversion_rates"]["TND"])
    last_update_utc = data.get("time_last_update_utc")

    if last_update_utc:
        ref_date = datetime.strptime(last_update_utc, "%a, %d %b %Y %H:%M:%S %z").date()
    else:
        ref_date = datetime.utcnow().date()

    metadata = {"source": "exchangerate_api", "time_last_update_utc": last_update_utc}
    return ref_date, {"TND": tnd_rate}, EXR_LATEST_URL, metadata


def scrape_exr_and_save(sync_type="api", send_email_on_error=True):
    start_time = time.time()
    try:
        ref_date, rates, url, metadata = fetch_exr_latest_tnd_rate()
        saved_count = save_ecb_rates_to_db(ref_date, rates, url, metadata)
        duration = time.time() - start_time
        log_sync_operation(sync_type, "success", saved_count, None, duration)
        logger.info(f"✅ ExchangeRate-API sync réussi: TND rate = {rates['TND']}")
        return {"status": "success", "ref_date": ref_date.isoformat(), "rates_saved": saved_count, "tnd_rate": rates["TND"]}
    except Exception as e:
        duration = time.time() - start_time
        error_msg = str(e)
        log_sync_operation(sync_type, "failed", 0, error_msg, duration)
        logger.error(f"❌ Échec ExchangeRate-API sync: {error_msg}")
        
        # Envoyer email d'alerte
        if send_email_on_error:
            send_alert_email(
                subject="🚨 ALERTE - Échec synchronisation EUR→TND",
                body=f"La synchronisation du taux EUR→TND (ExchangeRate-API) a échoué lors de l'exécution {sync_type}.",
                error_details=error_msg
            )
        
        return {"status": "error", "message": error_msg, "sync_type": sync_type}

# ==============================
# SCHEDULER JOBS
# ==============================
def scheduled_ecb_job():
    """Job planifié pour ECB avec email en cas d'échec"""
    logger.info("🕐 Démarrage du job planifié ECB")
    scrape_ecb_and_save(sync_type="scheduled", send_email_on_error=True)

def scheduled_exr_job():
    """Job planifié pour ExchangeRate-API avec email en cas d'échec"""
    logger.info("🕐 Démarrage du job planifié ExchangeRate-API")
    scrape_exr_and_save(sync_type="scheduled", send_email_on_error=True)

# ==============================
# SWAGGER CONFIG
# ==============================
swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "Currency API (ECB + ExchangeRate-API)",
        "version": "1.3",
        "description": "API d'extraction des taux ECB + EUR→TND (ExchangeRate-API) avec alertes email.",
    },
    "basePath": "/",
    "schemes": ["https"],
}

swagger_config = {
    "headers": [],
    "specs": [{
        "endpoint": "apispec",
        "route": "/apispec.json",
        "rule_filter": lambda rule: True,
        "model_filter": lambda tag: True,
    }],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs",
    "validate": True,
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)

# ==============================
# SCHEDULER (Production)
# ==============================
scheduler = BackgroundScheduler(timezone="Africa/Tunis")

# 1. Synchronisation BCE : Une fois par jour à 11:30
scheduler.add_job(
    scheduled_ecb_job,
    trigger=CronTrigger(hour=11, minute=30),
    id="prod_ecb_sync",
    replace_existing=True
)

# 2. Synchronisation TND : Trois fois par jour (09:00, 12:00, 17:00)
scheduler.add_job(
    scheduled_exr_job,
    trigger=CronTrigger(hour="9,12,17", minute=0),
    id="prod_exr_tnd_sync",
    replace_existing=True
)

scheduler.start()
logger.info("✅ Production Scheduler démarré :")
logger.info("   - ECB : Quotidien à 11:30 (Africa/Tunis)")
logger.info("   - EXR (TND) : Quotidien à 09:00, 12:00 et 17:00 (Africa/Tunis)")

# ==============================
# ROUTES
# ==============================
@app.route("/", methods=["GET"])
def home():
    """
    Page d'accueil Currency API
    ---
    tags:
      - System
    responses:
      200:
        description: Infos API
    """
    return jsonify({
        "service": "Currency API",
        "version": "1.3",
        "features": ["ECB rates", "EUR→TND rates", "Email alerts"],
        "swagger": "/docs"
    })

@app.route("/health", methods=["GET"])
def health_check():
    """
    Health check API + DB
    ---
    tags:
      - System
    responses:
      200:
        description: API healthy
    """
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route("/ecb/extract", methods=["POST"])
def extract_ecb_rates():
    """
    Extraction immédiate des taux ECB
    ---
    tags:
      - ECB
    parameters:
      - name: send_email
        in: query
        type: boolean
        default: false
        description: Envoyer un email en cas d'échec
    responses:
      200:
        description: Extraction réussie
      500:
        description: Erreur extraction
    """
    send_email = request.args.get("send_email", "false").lower() == "true"
    result = scrape_ecb_and_save(sync_type="manual", send_email_on_error=send_email)
    return jsonify(result), 200 if result["status"] == "success" else 500

@app.route("/exr/extract", methods=["POST"])
def extract_exr_rates():
    """
    Extraction immédiate EUR→TND (ExchangeRate-API)
    ---
    tags:
      - ExchangeRate-API
    parameters:
      - name: send_email
        in: query
        type: boolean
        default: false
        description: Envoyer un email en cas d'échec
    responses:
      200:
        description: Extraction réussie
      500:
        description: Erreur extraction
    """
    send_email = request.args.get("send_email", "false").lower() == "true"
    result = scrape_exr_and_save(sync_type="manual", send_email_on_error=send_email)
    return jsonify(result), 200 if result["status"] == "success" else 500

@app.route("/exr/rates/latest", methods=["GET"])
def get_latest_exr_tnd_rate():
    """
    Dernier taux EUR→TND stocké
    ---
    tags:
      - ExchangeRate-API
    responses:
      200:
        description: Dernier taux stocké
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT *
            FROM ecb_exchange_rates
            WHERE quote_currency = 'TND'
            ORDER BY ref_date DESC
            LIMIT 1;
        """)
        row = cursor.fetchone()
        return jsonify({"status": "success", "rate": row}), 200
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

@app.route("/sync/logs", methods=["GET"])
def get_sync_logs():
    """
    Logs de synchronisation
    ---
    tags:
      - Logs
    parameters:
      - name: limit
        in: query
        required: false
        type: integer
        default: 50
    responses:
      200:
        description: Logs list
    """
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(limit, 1000))
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT *
            FROM sync_logs
            ORDER BY created_at DESC
            LIMIT %s;
        """, (limit,))
        results = cursor.fetchall()
        return jsonify({"status": "success", "count": len(results), "logs": results}), 200
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

@app.route("/test/email", methods=["POST"])
def test_email():
    """
    Tester l'envoi d'email
    ---
    tags:
      - System
    responses:
      200:
        description: Email de test envoyé
    """
    try:
        send_alert_email(
            subject="🧪 Test Email - Currency API",
            body="Ceci est un email de test pour vérifier la configuration SMTP.",
            error_details="Aucune erreur - Test de fonctionnalité"
        )
        return jsonify({"status": "success", "message": f"Email de test envoyé à {ALERT_EMAIL}"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==============================
# ENTRYPOINT
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False, threaded=True)
