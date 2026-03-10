"""
meta_service.py
Lógica para llamar a la API de Anuncios de Meta, paginar y agrupar los anuncios por cuerpo creativo.
"""

import httpx
import json
from database import get_db_connection


def get_backend_db_connection():
    """Conexión a la BD 'backend' donde viven los accessTokens."""
    import pyodbc, os
    SERVER = os.environ.get('DB_SERVER', 'nichebreakerdb.database.windows.net')
    USERNAME = os.environ.get('DB_USER', 'backendTest')
    PASSWORD = os.environ.get('DB_PASSWORD', 'Xk9#mP2$vL7@nQ4!')
    DRIVER = os.environ.get('DB_DRIVER', '{ODBC Driver 18 for SQL Server}')
    if os.name == 'nt':
        DRIVER = '{SQL Server}'
    conn_str = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE=backend;"
        f"UID={USERNAME};PWD={PASSWORD};Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_available_access_token():
    """Obtiene un token de acceso disponible de la BD 'backend'."""
    conn = get_backend_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 1 accessToken 
            FROM accessTokens 
            WHERE status = 'READY'
            ORDER BY Id ASC
        """)
        row = cursor.fetchone()
        if row:
            return row[0]
        print("[meta_service] No tokens found with status='READY' in 'backend' DB")
        return None
    except Exception as e:
        print(f"[meta_service] Error querying accessTokens: {e}")
        return None
    finally:
        conn.close()


def set_analyzing_marker(page_id: str):
    """Escribe el marcador __ANALYZING__ en la BD para la page dada."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE pages SET AdGroupsJson = '__ANALYZING__' WHERE Page_id = ?", page_id)
        conn.commit()
        print(f"[meta_service] Set ANALYZING marker for page {page_id}")
    except Exception as e:
        print(f"[meta_service] Could not set ANALYZING marker: {e}")
    finally:
        conn.close()


def clear_analyzing_marker(page_id: str):
    """Limpia el marcador __ANALYZING__ si el proceso falla."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE pages SET AdGroupsJson = NULL WHERE Page_id = ? AND AdGroupsJson = '__ANALYZING__'", page_id)
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


async def fetch_all_page_ads(page_id: str, access_token: str) -> list:
    """
    Llama a la Meta Ads Library API paginando hasta obtener todos los anuncios
    de la página dada. Retorna una lista de dicts con los campos básicos.
    Limita a 2.000.000 de eu_total_reach acumulado para no tardar demasiado.
    """
    fields = "ad_snapshot_url,eu_total_reach,ad_creative_bodies,ad_delivery_start_time,ad_delivery_stop_time,status"
    limit = 500
    base_url = (
        f"https://graph.facebook.com/v24.0/ads_archive"
        f"?ad_reached_countries=['']"
        f"&search_page_ids={page_id}"
        f"&fields={fields}"
        f"&access_token={access_token}"
        f"&limit={limit}"
        f"&locale=en_US"
    )

    all_ads = []
    total_reach = 0
    next_url = base_url

    async with httpx.AsyncClient(timeout=120.0) as client:
        while next_url:
            try:
                response = await client.get(next_url)
                if not response.is_success:
                    print(f"[meta_service] Error {response.status_code} fetching ads for page {page_id}: {response.text[:300]}")
                    break

                data = response.json()
                ads = data.get("data", [])
                all_ads.extend(ads)

                for ad in ads:
                    total_reach += ad.get("eu_total_reach", 0)

                # NOTA: Límite de 2M removido para permitir Full Scrape
                # (Se extraerán todos los anuncios históricos de la página)

                next_url = data.get("paging", {}).get("next")

            except Exception as e:
                print(f"[meta_service] Exception fetching ads for page {page_id}: {e}")
                break

    return all_ads


def group_ads_by_body(ads: list) -> list:
    """
    Agrupa los anuncios por su primer `ad_creative_bodies`.
    Por cada grupo retorna: reach total, si está activo, y lista de links detallados.
    Ordenado de mayor a menor reach.
    """
    from datetime import datetime
    
    groups: dict[str, dict] = {}
    now_date = datetime.utcnow().date()

    for ad in ads:
        bodies = ad.get("ad_creative_bodies") or []
        key = bodies[0].strip() if bodies else "UNKNOWN"

        if key not in groups:
            groups[key] = {
                "body": key,
                "reach": 0,
                "is_active": False,
                "links": []
            }

        groups[key]["reach"] += ad.get("eu_total_reach", 0)
        snapshot = ad.get("ad_snapshot_url")
        
        # Meta API logic: ad is active if stop_time is absent, or if it's strictly in the future.
        stop_time_str = ad.get("ad_delivery_stop_time")
        is_active = False
        if not stop_time_str:
            is_active = True
        else:
            try:
                stop_date = datetime.strptime(stop_time_str, "%Y-%m-%d").date()
                if stop_date > now_date:
                    is_active = True
            except ValueError:
                pass # Default to inactive if we can't parse

        if is_active:
            groups[key]["is_active"] = True

        if snapshot:
            groups[key]["links"].append({
                "url": snapshot,
                "is_active": is_active,
                "reach": ad.get("eu_total_reach", 0),
                "start_time": ad.get("ad_delivery_start_time"),
                "stop_time": stop_time_str
            })

    result = sorted(groups.values(), key=lambda g: g["reach"], reverse=True)
    return result


def build_activity_graph(ads: list) -> list:
    """
    Construye un historial de actividad agrupando la cantidad de anuncios creados
    por semana ('YYYY-Www') basándose en ad_delivery_start_time.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict
    
    counts_per_week = defaultdict(int)
    
    for ad in ads:
        start_str = ad.get("ad_delivery_start_time")
        if not start_str:
            continue
            
        try:
            # Meta format is generally "YYYY-MM-DD"
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except ValueError:
            continue
             
        # ISO format e.g. "2023-W41"
        iso_year, iso_week, _ = start_date.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"
        counts_per_week[week_key] += 1

    # Convert to sorted list format
    sorted_weeks = sorted(counts_per_week.keys())
    graph_data = [{"week": w, "active_count": counts_per_week[w]} for w in sorted_weeks]
    return graph_data


async def analyze_and_save_page_groups(page_id: str):
    """
    Proceso completo bajo demanda:
    1. Obtiene un token de acceso.
    2. Descarga todos los anuncios de la página.
    3. Agrupa por cuerpo creativo.
    4. Guarda el JSON en pages.AdGroupsJson.
    """
    try:
        print(f"[meta_service] Starting ad group analysis for page_id={page_id}")

        access_token = get_available_access_token()
        if not access_token:
            print(f"[meta_service] No access token with status='READY' found. Aborting.")
            clear_analyzing_marker(page_id)
            return

        ads = await fetch_all_page_ads(page_id, access_token)
        print(f"[meta_service] Fetched {len(ads)} ads for page {page_id}")

        groups = group_ads_by_body(ads)
        print(f"[meta_service] Grouped into {len(groups)} groups")
        
        graph = build_activity_graph(ads)

        total_scraped_reach = sum(g["reach"] for g in groups)

        final_data = {
             "groups": groups,
             "activity_graph": graph,
             "total_scraped_reach": total_scraped_reach
        }

        groups_json = json.dumps(final_data, ensure_ascii=False)

        # Guardar en la BD
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE pages SET AdGroupsJson = CAST(? AS NVARCHAR(MAX)) WHERE Page_id = ?",
                (groups_json, page_id)
            )
            conn.commit()
            print(f"[meta_service] Saved AdGroupsJson for page {page_id} ({len(groups)} groups)")
        except Exception as e:
            print(f"[meta_service] Error saving to DB: {e}")
            conn.rollback()
            clear_analyzing_marker(page_id)
        finally:
            conn.close()

    except Exception as e:
        print(f"[meta_service] Unexpected error in analyze_and_save_page_groups: {e}")
        clear_analyzing_marker(page_id)
