"""
API Flask pour extraction des taux de change EUR de la BCE
----------------------------------------------------------
pip install flask flasgger requests psycopg2-binary apscheduler python-dotenv
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
logger = logging.getLogger("ecb_fx_api")
logger.info("=" * 80)
logger.info(" DÉMARRAGE DE L'API ECB FX")
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
        raise RuntimeError(
            "ECB_DB_PASSWORD est vide. Définis la variable d'environnement ECB_DB_PASSWORD."
        )
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(" Connexion à la base de données établie (ECB)")
        return conn
    except Exception as e:
        logger.error(f" Erreur de connexion à la base de données: {e}")
        raise
# ==============================
# CONFIGURATION ECB
# ==============================
ECB_DAILY_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
BASE_CURRENCY = "EUR"
#  DEVISES CIBLÉES
TARGET_CURRENCIES = ["CNY", "EUR", "INR", "KRW", "MXN", "USD"]
logger.info(f"Devises ciblées: {', '.join(TARGET_CURRENCIES)}")
# ==============================
# FONCTIONS DB
# ==============================
def save_ecb_rates_to_db(ref_date: date, rates: dict, source_url: str, metadata: dict = None) -> int:
    """
    Enregistrer les taux ECB dans la base.

    Args:
        ref_date (date): Date de référence fournie par la BCE
        rates (dict): {quote_currency: rate_float}
        source_url (str): URL source
        metadata (dict): Infos additionnelles
    Returns:
        int: Nombre d'enregistrements insérés/mis à jour
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
            if rate is None:
                logger.warning(f"Taux manquant pour {currency}, ignoré")
                continue

            # Filtrer uniquement les devises ciblées
            if currency not in TARGET_CURRENCIES:
                logger.debug(f"⏭  Devise {currency} hors cible, ignorée")
                continue

            cursor.execute(
                insert_query,
                (
                    ref_date,
                    BASE_CURRENCY,
                    currency,
                    rate,
                    source_url,
                    json.dumps(metadata) if metadata else "{}",
                )
            )
            saved_count += 1
            logger.info(f"    Enregistré: {ref_date} | 1 {BASE_CURRENCY} = {rate} {currency}")

        conn.commit()
        logger.info(f" {saved_count} taux enregistrés/mis à jour dans ecb_exchange_rates")
        return saved_count

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f" Erreur lors de l'enregistrement des taux ECB: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info(" Connexion DB fermée (ECB)")


def log_sync_operation(sync_type, status, records, error_message=None, duration=None):
    """
    Enregistrer une opération de synchronisation ECB dans sync_logs.
    sync_type ∈ ('scheduled','manual','api') avec ta contrainte actuelle.
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

        cursor.execute(
            insert_query,
            (sync_type, status, records, error_message, duration),
        )
        conn.commit()
        logger.info(f" Log ECB sync enregistré: {status} - {records} taux")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f" Erreur lors de l'enregistrement du log ECB: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ==============================
# FONCTIONS D'EXTRACTION ECB
# ==============================
def fetch_ecb_daily_rates():
    """
    Récupère les taux de change du jour depuis l'XML de la BCE.
    Retourne:
        ref_date (date), rates (dict{currency: rate}), source_url (str)
    """
    logger.info(f" Requête ECB vers: {ECB_DAILY_URL}")
    resp = requests.get(ECB_DAILY_URL, timeout=30)
    resp.raise_for_status()

    xml_text = resp.text
    logger.info(f" Réponse ECB reçue ({len(xml_text)} chars)")

    root = ET.fromstring(xml_text)

    rates = {}
    ref_date = None

    for cube_time in root.iter():
        if cube_time.tag.endswith("Cube") and "time" in cube_time.attrib:
            ref_date_str = cube_time.attrib["time"]
            ref_date = datetime.strptime(ref_date_str, "%Y-%m-%d").date()

            for cube in cube_time:
                if "currency" in cube.attrib and "rate" in cube.attrib:
                    currency = cube.attrib["currency"]

                    #  Filtrer dès l'extraction
                    if currency not in TARGET_CURRENCIES:
                        continue

                    rate = float(cube.attrib["rate"])
                    rates[currency] = rate

            break
    # Ajouter EUR=1.0 si ciblé
    if "EUR" in TARGET_CURRENCIES:
        rates["EUR"] = 1.0
        logger.info("   EUR = 1.0 ajouté (base currency)")

    if ref_date is None or not rates:
        raise ValueError("Impossible d'extraire les taux ou la date depuis l'XML ECB")

    logger.info(f"Taux ECB extraits pour la date {ref_date}: {len(rates)} devises ciblées")
    logger.info(f"   Devises extraites: {', '.join(sorted(rates.keys()))}")

    return ref_date, rates, ECB_DAILY_URL

# ==============================
# PIPELINE PRINCIPAL
# ==============================
def scrape_ecb_and_save(sync_type="api"):
    """
    Exécute le fetch ECB + enregistrement DB + log dans sync_logs.
    sync_type: 'api' (par défaut), 'scheduled' ou 'manual'
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info(f"🚀 DÉBUT EXTRACTION ECB ({sync_type})")
    logger.info("=" * 80)

    try:
        ref_date, rates, url = fetch_ecb_daily_rates()

        metadata = {
            "source": "ecb_xml",
            "job": "daily_ecb_scrape",
            "fetched_at": datetime.now().isoformat(),
            "target_currencies": TARGET_CURRENCIES,
        }

        saved_count = save_ecb_rates_to_db(ref_date, rates, url, metadata)
        duration = time.time() - start_time

        status = "success" if saved_count > 0 else "failed"

        log_sync_operation(sync_type=sync_type, status=status, records=saved_count,
                           error_message=None, duration=duration)

        logger.info("=" * 80)
        logger.info(f"EXTRACTION ECB TERMINÉE ({duration:.2f}s)")
        logger.info(f"   Taux enregistrés: {saved_count}/{len(TARGET_CURRENCIES)}")
        logger.info("=" * 80)

        return {
            "status": status,
            "ref_date": ref_date.isoformat(),
            "rates_saved": saved_count,
            "total_rates": len(rates),
            "target_currencies": TARGET_CURRENCIES,
            "duration": duration,
            "sync_type": sync_type,
        }

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f" Erreur globale ECB: {e}", exc_info=True)

        log_sync_operation(sync_type=sync_type, status="failed", records=0,
                           error_message=str(e), duration=duration)

        return {
            "status": "error",
            "message": str(e),
            "sync_type": sync_type,
        }
# ==============================
# TÂCHE PLANIFIÉE
# ==============================
def scheduled_ecb_job():
    """Tâche planifiée quotidienne pour les taux ECB (18h Tunis)."""
    logger.info("Exécution tâche planifiée ECB (18h00)")
    scrape_ecb_and_save(sync_type="scheduled")
# ==============================
# FLASK + SWAGGER
# ==============================
app = Flask(__name__)

# ✅ CORRECTION: Configuration Swagger 2.0 (compatible Flasgger)
swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "ECB FX API",
        "version": "1.1",
        "description": "API d'extraction des taux de change EUR depuis la BCE (eurofxref-daily.xml).",
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
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)


# Scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    func=scheduled_ecb_job,
    trigger=CronTrigger(hour=18, minute=0),
    id="daily_ecb_scraping",
    name="Extraction quotidienne des taux ECB à 18h00",
    replace_existing=True,
)
scheduler.start()
logger.info(" Scheduler ECB initialisé: extraction quotidienne à 18h00")


# ==============================
# ROUTES API
# ==============================
@app.route("/", methods=["GET"])
def home():
    """
    Page d'accueil ECB FX API
    ---
    responses:
      200:
        description: Infos API
    """
    return jsonify({
        "service": "API d'extraction des taux ECB (EUR FX)",
        "version": "1.1",
        "target_currencies": TARGET_CURRENCIES,
        "features": {
            "scraping": "Extraction depuis eurofxref-daily.xml",
            "database": "Table ecb_exchange_rates",
            "scheduler": "Exécution quotidienne à 18h00",
            "filtering": "Seulement 7 devises ciblées",
        },
        "endpoints": {
            "/ecb/extract": "POST - Extraire et enregistrer les taux du jour",
            "/ecb/rates/latest": "GET - Derniers taux (dernière date)",
            "/ecb/rates/history": "GET - Historique des taux pour une devise",
            "/sync/logs": "GET - Logs de synchronisation (communs)",
            "/health": "GET - État de l'API",
            "/docs": "GET - Documentation Swagger",
        },
    })


@app.route("/health", methods=["GET"])
def health_check():
    """
    Santé de l'API et connexion DB
    ---
    responses:
      200:
        description: API opérationnelle
    """
    db_status = "unknown"
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "connected"
    except Exception:
        db_status = "disconnected"

    return jsonify({
        "status": "healthy",
        "database": db_status,
        "scheduler": "running" if scheduler.running else "stopped",
        "target_currencies": TARGET_CURRENCIES,
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/ecb/extract", methods=["POST"])
def extract_ecb_rates():
    """
    Extraction immédiate des taux ECB et enregistrement
    ---
    responses:
      200:
        description: Extraction réussie
      500:
        description: Erreur
    """
    logger.info("Requête /ecb/extract reçue (manuelle)")
    result = scrape_ecb_and_save(sync_type="manual")

    if result["status"] in ("success", "partial"):
        return jsonify(result), 200
    return jsonify(result), 500


@app.route("/ecb/rates/latest", methods=["GET"])
def get_latest_ecb_rates():
    """
    Obtenir les derniers taux ECB (dernière date disponible)
    ---
    parameters:
      - name: quote_currency
        in: query
        required: false
        type: string
        description: "Filtrer par devise (ex: USD)"
    responses:
      200:
        description: Derniers taux
    """
    quote_currency = request.args.get("quote_currency")

    if quote_currency:
        quote_currency = quote_currency.strip().upper()

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT MAX(ref_date) AS max_date FROM ecb_exchange_rates;")
        row = cursor.fetchone()
        max_date = row["max_date"]

        if not max_date:
            return jsonify({"status": "success", "count": 0, "rates": []}), 200

        if quote_currency:
            query = """
                SELECT *
                FROM ecb_exchange_rates
                WHERE ref_date = %s AND quote_currency = %s
                ORDER BY quote_currency;
            """
            cursor.execute(query, (max_date, quote_currency))
        else:
            query = """
                SELECT *
                FROM ecb_exchange_rates
                WHERE ref_date = %s
                ORDER BY quote_currency;
            """
            cursor.execute(query, (max_date,))

        results = cursor.fetchall()
        return jsonify({
            "status": "success",
            "ref_date": max_date.isoformat(),
            "count": len(results),
            "rates": results,
        }), 200

    except Exception as e:
        logger.error(f"Erreur get_latest_ecb_rates: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


@app.route("/ecb/rates/history", methods=["GET"])
def get_ecb_rate_history():
    """
    Historique des taux ECB pour une devise
    ---
    parameters:
      - name: quote_currency
        in: query
        required: true
        type: string
        description: "Devise (ex: USD)"
      - name: days
        in: query
        required: false
        type: integer
        default: 30
      - name: limit
        in: query
        required: false
        type: integer
        default: 100
    responses:
      200:
        description: Historique
    """
    quote_currency = request.args.get("quote_currency")
    days = request.args.get("days", default=30, type=int)
    limit = request.args.get("limit", default=100, type=int)

    if not quote_currency:
        return jsonify({"status": "error", "message": "quote_currency est obligatoire (ex: USD)"}), 400

    quote_currency = quote_currency.strip().upper()

    if quote_currency not in TARGET_CURRENCIES:
        return jsonify({
            "status": "error",
            "message": f"quote_currency invalide. Devises acceptées: {', '.join(TARGET_CURRENCIES)}"
        }), 400

    days = max(1, min(days, 3650))
    limit = max(1, min(limit, 5000))

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT *
            FROM ecb_exchange_rates
            WHERE quote_currency = %s
              AND ref_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            ORDER BY ref_date DESC
            LIMIT %s;
        """
        cursor.execute(query, (quote_currency, days, limit))
        results = cursor.fetchall()

        return jsonify({
            "status": "success",
            "quote_currency": quote_currency,
            "days": days,
            "count": len(results),
            "history": results,
        }), 200

    except Exception as e:
        logger.error(f"Erreur get_ecb_rate_history: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


@app.route("/sync/logs", methods=["GET"])
def get_sync_logs():
    """
    Obtenir les logs de synchronisation (communs Shmet + ECB)
    ---
    parameters:
      - name: limit
        in: query
        required: false
        type: integer
        default: 50
    responses:
      200:
        description: Logs de synchronisation
    """
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(limit, 1000))

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT *
            FROM sync_logs
            ORDER BY created_at DESC
            LIMIT %s;
        """
        cursor.execute(query, (limit,))
        results = cursor.fetchall()

        return jsonify({"status": "success", "count": len(results), "logs": results}), 200

    except Exception as e:
        logger.error(f"Erreur get_sync_logs: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


# ==============================
# POINT D'ENTRÉE
# ==============================
if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info(" DÉMARRAGE DU SERVEUR ECB FX")
    logger.info(f" Devises ciblées: {', '.join(TARGET_CURRENCIES)}")
    logger.info(" Documentation: http://localhost:5001/docs")
    logger.info("  Base de données: PostgreSQL (Azure, LME_DB)")
    logger.info(" Extraction quotidienne ECB: 18h00")
    logger.info("=" * 80)
    try:
        app.run(
            host="0.0.0.0",
            port=5001,
            debug=False,
            threaded=True
        )
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info(" Arrêt du scheduler ECB")
    except Exception as e:
        logger.error(f"Erreur fatale ECB: {e}", exc_info=True)
        raise
