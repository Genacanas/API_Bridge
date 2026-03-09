"""
meta_service.py
Lógica para llamar a la API de Anuncios de Meta, paginar y agrupar los anuncios por cuerpo creativo.
"""

import httpx
import json
from database import get_db_connection


def get_available_access_token():
    """Obtiene un token de acceso disponible de la base de datos del backend."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # La tabla accessTokens vive en la BD del backend (dev-milco también la tiene vía la misma conn)
        cursor.execute("""
            SELECT TOP 1 accessToken 
            FROM accessTokens 
            WHERE status = 'READY'
            ORDER BY Id ASC
        """)
        row = cursor.fetchone()
        if row:
            return row[0]
        return None
    finally:
        conn.close()


async def fetch_all_page_ads(page_id: str, access_token: str) -> list:
    """
    Llama a la Meta Ads Library API paginando hasta obtener todos los anuncios
    de la página dada. Retorna una lista de dicts con los campos básicos.
    Limita a 2.000.000 de eu_total_reach acumulado para no tardar demasiado.
    """
    fields = "ad_snapshot_url,eu_total_reach,ad_creative_bodies"
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

                # Parar si superamos 2M de reach acumulado (igual que el esclavo)
                if total_reach >= 2_000_000:
                    print(f"[meta_service] Reached 2M reach limit for page {page_id}. Stopping pagination.")
                    break

                next_url = data.get("paging", {}).get("next")

            except Exception as e:
                print(f"[meta_service] Exception fetching ads for page {page_id}: {e}")
                break

    return all_ads


def group_ads_by_body(ads: list) -> list:
    """
    Agrupa los anuncios por su primer `ad_creative_bodies`.
    Por cada grupo retorna: reach total, lista de links (ad_snapshot_url).
    Ordenado de mayor a menor reach.
    """
    groups: dict[str, dict] = {}

    for ad in ads:
        bodies = ad.get("ad_creative_bodies") or []
        key = bodies[0].strip() if bodies else "UNKNOWN"

        if key not in groups:
            groups[key] = {
                "body": key,
                "reach": 0,
                "links": []
            }

        groups[key]["reach"] += ad.get("eu_total_reach", 0)
        snapshot = ad.get("ad_snapshot_url")
        if snapshot:
            groups[key]["links"].append(snapshot)

    result = sorted(groups.values(), key=lambda g: g["reach"], reverse=True)
    return result


async def analyze_and_save_page_groups(page_id: str):
    """
    Proceso completo bajo demanda:
    1. Obtiene un token de acceso.
    2. Descarga todos los anuncios de la página.
    3. Agrupa por cuerpo creativo.
    4. Guarda el JSON en pages.AdGroupsJson.
    """
    print(f"[meta_service] Starting ad group analysis for page_id={page_id}")

    access_token = get_available_access_token()
    if not access_token:
        print(f"[meta_service] No access token available. Aborting.")
        return

    # Marcar como "analizando" en la BD para que el frontend lo sepa incluso tras recargar
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE pages SET AdGroupsJson = '__ANALYZING__' WHERE Page_id = ?", page_id)
        conn.commit()
    except Exception as e:
        print(f"[meta_service] Could not set ANALYZING marker: {e}")
    finally:
        conn.close()

    ads = await fetch_all_page_ads(page_id, access_token)
    print(f"[meta_service] Fetched {len(ads)} ads for page {page_id}")

    groups = group_ads_by_body(ads)
    print(f"[meta_service] Grouped into {len(groups)} groups")

    groups_json = json.dumps(groups, ensure_ascii=False)

    # Guardar en la BD
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE pages SET AdGroupsJson = ? WHERE Page_id = ?",
            (groups_json, page_id)
        )
        conn.commit()
        print(f"[meta_service] Saved AdGroupsJson for page {page_id} ({len(groups)} groups)")
    except Exception as e:
        print(f"[meta_service] Error saving to DB: {e}")
        # Limpiar el marcador para que no quede trabado en 'ANALYZING'
        try:
            cursor.execute("UPDATE pages SET AdGroupsJson = NULL WHERE Page_id = ?", page_id)
            conn.commit()
        except Exception:
            pass
        conn.rollback()
    finally:
        conn.close()
