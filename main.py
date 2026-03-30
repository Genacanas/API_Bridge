from fastapi import FastAPI, Depends, Query, HTTPException, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Optional, Any
import pyodbc
from database import get_db, get_auth_db
from auth import (
    Token,
    verify_password,
    create_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    get_current_active_user,
    create_users_table_if_not_exists,
    create_initial_admin,
    UserInDB
)
from datetime import timedelta, datetime
import zoneinfo

app = FastAPI(title="NicheBreaker API Bridge")

# Allow frontend to access this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins
    allow_credentials=False, # Must be False if origins is ["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    # Attempt to create the 'users' table in the auth DB if it doesn't exist
    print("Running startup checks...")
    try:
        # We manually create a connection here because Depends() doesn't work in startup event
        from database import get_auth_db_connection
        conn = get_auth_db_connection()
        create_users_table_if_not_exists(conn)
        create_initial_admin(conn)
        conn.close()
        print("Startup checks for users table finished successfully.")
    except Exception as e:
        print(f"Error initializing users table: {e}")

@app.post("/api/login", response_model=Token)
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: pyodbc.Connection = Depends(get_auth_db)
):
    from auth import get_user
    user = get_user(db, form_data.username)
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# Mappings between C# Enum (page_process_status) and Frontend Strings
STATUS_MAP_TO_UI = {
    0: "unprocessed", # INITIAL_PENDING
    11: "saved",      # INITIAL_SUCCESS
    13: "deleted"     # DELETED
}

STATUS_MAP_TO_DB = {
    "unprocessed": 0,
    "saved": 11,
    "deleted": 13
}

CREATIVE_TYPE_MAP = {
    0: "image",
    1: "video",
    2: "carousel"
}

class TopCreative(BaseModel):
    media_url: Optional[str] = None
    media_type: str = "image"
    snapshot_url: Optional[str] = None

class PageData(BaseModel):
    page_id: str
    name: str
    country: str = ""
    total_eu_reach: int
    active_eu_total_reach: Optional[int] = None
    active_ads_count: Optional[int] = None
    manual_status: str
    beneficiary: Optional[str] = None
    tag: Optional[str] = None
    tagId: Optional[int] = None
    top_creative: Optional[TopCreative] = None
    is_queued_for_scrape: bool = False

class StatusUpdateRequest(BaseModel):
    manual_status: str

@app.get("/api/pages", response_model=List[PageData])
def get_pages(
    status: str = "unprocessed",
    searchTerm: Optional[str] = None,
    country: Optional[str] = None,
    category: Optional[str] = None,
    tag: Optional[str] = None,
    action_date: Optional[str] = Query(None, description="Filter by status update date (YYYY-MM-DD)"),
    min_reach: int = Query(default=200000, ge=0, description="Minimum eu_total_reach filter"),
    limit: int = Query(default=100, ge=1, le=500, description="Number of results per page"),
    offset: int = Query(default=0, ge=0, description="Number of rows to skip"),
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
) -> List[PageData]:
    try:
        cursor = db.cursor()
        
        # Translate frontend status to DB integer
        db_status = STATUS_MAP_TO_DB.get(status, 0)
        
        # Use a CTE to deduplicate pages (taking only 1 ad per page) before paginating.
        # This avoiding duplicates from ads JOIN and makes OFFSET/FETCH NEXT reliable.
        
        # Always include status 0 (unprocessed) and 7 (queued for scrape).
        # Additionally add the tab-specific status.
        db_statuses = [0]
        if status == "saved":
            db_statuses = [7, 11]
        elif status == "deleted":
            db_statuses = [13]
            
        status_placeholders = ",".join(["?"] * len(db_statuses))
            
        query = f"""
            WITH RankedAds AS (
                SELECT
                    pg.Id          AS PageInternalId,
                    pg.Page_id,
                    pg.Name,
                    pg.eu_total_reach,
                    pg.active_eu_total_reach,
                    pg.active_ads_count,
                    pg.category    AS pg_category,
                    pg.TagName,
                    pg.TagId,
                    pp.status,
                    pp.beneficiary AS pp_beneficiary,
                    a.creativeUrl,
                    a.creative_type,
                    a.AdSnapshotUrl,
                    a.reachedCountries,
                    ROW_NUMBER() OVER (PARTITION BY pg.Page_id ORDER BY a.Id ASC) AS rn
                FROM pages pg
                LEFT JOIN pagesProducts pp ON pp.pageId = pg.Id
                LEFT JOIN niches n ON pp.nicheId = n.Id
                LEFT JOIN ads a ON a.pageId = pg.Id
                WHERE (pp.status IN ({status_placeholders}) OR (pp.status IS NULL AND ? = 0))
                  AND pg.eu_total_reach >= ?
        """
        params: List[Any] = []
        params.extend(db_statuses)
        params.append(db_statuses[0])
        params.append(min_reach)

        if action_date:
            query += "                  AND CONVERT(DATE, pp.status_updated_at) = ?\n"
            params.append(action_date)

        if searchTerm and searchTerm != "All":
            query += "                  AND pg.Name LIKE ?\n"
            params.append(f"%{searchTerm}%")

        if category and category != "All":
            query += "                  AND pg.category = ?\n"
            params.append(category)

        if country and country != "All" and country != "ALL":
            # Check country by relating to niche name
            query += "                  AND n.Name = ?\n"
            params.append(country)

        if tag and tag != "All":
            if tag == "Untagged":
                query += "                  AND pg.TagName IS NULL\n"
            else:
                query += "                  AND pg.TagName = ?\n"
                params.append(tag)

        query += """
            )
            SELECT *
            FROM RankedAds
            WHERE rn = 1
            ORDER BY eu_total_reach DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """
        params.extend([offset, limit])

        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            top_creative = None
            if row.creativeUrl or row.AdSnapshotUrl:
                c_type_str = CREATIVE_TYPE_MAP.get(row.creative_type, "image") if row.creative_type else "image"
                top_creative = TopCreative(
                    media_url=row.creativeUrl or "",
                    media_type=c_type_str,
                    snapshot_url=row.AdSnapshotUrl or ""
                )
                
            ui_status = STATUS_MAP_TO_UI.get(row.status, "unprocessed")
            
            results.append(PageData(
                page_id=row.Page_id,
                name=row.Name or "Unknown",
                country="",
                total_eu_reach=row.eu_total_reach or 0,
                active_eu_total_reach=row.active_eu_total_reach,
                active_ads_count=row.active_ads_count,
                manual_status="unprocessed" if row.status in (0, 7) else ui_status,
                beneficiary=row.pp_beneficiary or "",
                tag=row.TagName,
                tagId=row.TagId,
                top_creative=top_creative,
                is_queued_for_scrape=(row.status == 7)
            ))
            
        return results
        
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/pages/{page_id}/status")
def update_page_status(
    page_id: str,
    update_data: StatusUpdateRequest,
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        db_status = STATUS_MAP_TO_DB.get(update_data.manual_status)
        if db_status is None:
            raise HTTPException(status_code=400, detail="Invalid status")
            
        cursor = db.cursor()
        
        # Calculate Lithuanian time
        try:
            tz = zoneinfo.ZoneInfo("Europe/Vilnius")
        except zoneinfo.ZoneInfoNotFoundError:
            # Fallback for environments without tzdata locally installed (like Windows temporarily)
            # but in production/linux it will use Europe/Vilnius
            from datetime import timezone
            tz = timezone(timedelta(hours=2)) # Lithuania is UTC+2 (or +3 in summer, but +2 is standard)
            
        lithuanian_now = datetime.now(tz)

        # 1. Update existing pagesProducts
        cursor.execute(
            """
            UPDATE pagesProducts 
            SET status = ?, status_updated_at = ?
            WHERE pageId IN (SELECT Id FROM pages WHERE Page_id = ?)
            """, 
            [db_status, lithuanian_now, page_id]
        )
        
        # 2. Insert missing pagesProducts for clones
        cursor.execute(
            """
            INSERT INTO pagesProducts (pageId, nicheId, total_reach, total_ads, date_updated, status, status_updated_at)
            SELECT p.Id, ISNULL((SELECT TOP 1 Id FROM niches), 1), ISNULL(p.eu_total_reach, 0), 1, GETUTCDATE(), ?, ?
            FROM pages p
            LEFT JOIN pagesProducts pp ON pp.pageId = p.Id
            WHERE p.Page_id = ? AND pp.Id IS NULL
            """,
            [db_status, lithuanian_now, page_id]
        )
        db.commit()
        
        return {"success": True, "message": "Status updated successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pages/{page_id}/trigger-full-scrape")
def trigger_full_scrape(
    page_id: str,
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        
        try:
            tz = zoneinfo.ZoneInfo("Europe/Vilnius")
        except zoneinfo.ZoneInfoNotFoundError:
            from datetime import timezone
            tz = timezone(timedelta(hours=2))
            
        lithuanian_now = datetime.now(tz)

        # 1. Update existing pagesProducts
        cursor.execute(
            """
            UPDATE pagesProducts 
            SET status = 7, scrappingType = 0, status_updated_at = ?
            WHERE pageId IN (SELECT Id FROM pages WHERE Page_id = ?)
            """, 
            [lithuanian_now, page_id]
        )
        
        # 2. Insert missing pagesProducts for clones
        cursor.execute(
            """
            INSERT INTO pagesProducts (pageId, nicheId, total_reach, total_ads, date_updated, status, scrappingType, status_updated_at)
            SELECT p.Id, ISNULL((SELECT TOP 1 Id FROM niches), 1), ISNULL(p.eu_total_reach, 0), 1, GETUTCDATE(), 7, 0, ?
            FROM pages p
            LEFT JOIN pagesProducts pp ON pp.pageId = p.Id
            WHERE p.Page_id = ? AND pp.Id IS NULL
            """,
            [lithuanian_now, page_id]
        )
        db.commit()
        
        return {"success": True, "message": "Triggered full page scrape"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pages/{page_id}/cancel-full-scrape")
def cancel_full_scrape(
    page_id: str,
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        # Revert to status 11 (Saved/Completed) instead of 0 (Pending)
        cursor.execute(
            """
            UPDATE pagesProducts 
            SET status = 11, scrappingType = NULL, status_updated_at = GETUTCDATE()
            WHERE pageId IN (SELECT Id FROM pages WHERE Page_id = ?)
              AND status = 7
            """,
            [page_id]
        )
        db.commit()
        return {"success": True, "message": "Full scrape cancelled, page reverted to pending"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}

COUNTRY_LIST = ["ALL", "BR", "IN", "GB", "US", "CA", "AR", "AU", "AT", "BE", "CL", "CN", "CO", "HR", "DK", "DO", "EG", "FI", "FR", "DE", "GR", "HK", "ID", "IE", "IL", "IT", "JP", "JO", "KW", "LB", "MY", "MX", "NL", "NZ", "NG", "NO", "PK", "PA", "PE", "PH", "PL", "RU", "SA", "RS", "SG", "ZA", "KR", "ES", "SE", "CH", "TW", "TH", "TR", "AE", "VE", "PT", "LU", "BG", "CZ", "SI", "IS", "SK", "LT", "TT", "BD", "LK", "KE", "HU", "MA", "CY", "JM", "EC", "RO", "BO", "GT", "CR", "QA", "SV", "HN", "NI", "PY", "UY", "PR", "BA", "PS", "TN", "BH", "VN", "GH", "MU", "UA", "MT", "BS", "MV", "OM", "MK", "LV", "EE", "IQ", "DZ", "AL", "NP", "MO", "ME", "SN", "GE", "BN", "UG", "GP", "BB", "AZ", "TZ", "LY", "MQ", "CM", "BW", "ET", "KZ", "NA", "MG", "NC", "MD", "FJ", "BY", "JE", "GU", "YE", "ZM", "IM", "HT", "KH", "AW", "PF", "AF", "BM", "GY", "AM", "MW", "AG", "RW", "GG", "GM", "FO", "LC", "KY", "BJ", "AD", "GD", "VI", "BZ", "VC", "MN", "MZ", "ML", "AO", "GF", "UZ", "DJ", "BF", "MC", "TG", "GL", "GA", "GI", "CD", "KG", "PG", "BT", "KN", "SZ", "LS", "LA", "LI", "MP", "SR", "SC", "VG", "TC", "DM", "MR", "AX", "SM", "SL", "NE", "CG", "AI", "YT", "CV", "GN", "TM", "BI", "TJ", "VU", "SB", "ER", "WS", "AS", "FK", "GQ", "TO", "KM", "PW", "FM", "CF", "SO", "MH", "VA", "TD", "KI", "ST", "TV", "NR", "RE", "LR", "ZW", "CI", "MM", "AN", "AQ", "BQ", "BV", "IO", "CX", "CC", "CK", "CW", "TF", "GW", "HM", "XK", "MS", "NU", "NF", "PN", "BL", "SH", "MF", "PM", "SX", "GS", "SD", "SS", "SJ", "TL", "TK", "UM", "WF", "EH"]

class SearchTermRequest(BaseModel):
    country: str
    search_term: str
    min_ad_creation_time: Optional[str] = None

from datetime import datetime

@app.post("/api/search_terms")
def create_search_term(
    term_data: SearchTermRequest,
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        
        country_index = 0 # Default to ALL
        try:
            country_index = COUNTRY_LIST.index(term_data.country.upper())
        except ValueError:
            pass
            
        cursor.execute("SELECT TOP 1 Id FROM niches")
        niche_row = cursor.fetchone()
        niche_id = niche_row.Id if niche_row else 1
        
        query = """
            INSERT INTO searchTerms 
            (nicheId, searchTerm, countryType, searchCreativeType, lastUpdated, isUpdateable, scrapeFully)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        now = datetime.utcnow()
        params = [niche_id, term_data.search_term, country_index, 0, now, True, True]
        
        cursor.execute(query, params)
        db.commit()
        
        return {"success": True, "message": "Search term created successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# --- Countries Management ---

@app.get("/api/countries", response_model=List[str])
def get_countries(
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT Name FROM niches ORDER BY Name ASC")
        rows = cursor.fetchall()
        return [row.Name for row in rows if row.Name]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch countries: {e}")

# --- Tags Management ---

class TagResponse(BaseModel):
    Id: int
    Name: str

class TagCreateRequest(BaseModel):
    name: str

class TagUpdateRequest(BaseModel):
    tagId: Optional[int] = None
    tagName: Optional[str] = None

@app.get("/api/tags", response_model=List[TagResponse])
def get_tags(
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        cursor.execute("SELECT Id, Name FROM tags ORDER BY Name ASC")
        rows = cursor.fetchall()
        return [{"Id": row.Id, "Name": row.Name} for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tags: {e}")

@app.post("/api/tags", response_model=TagResponse)
def create_tag(
    tag: TagCreateRequest, 
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        # Verify it doesnt exist
        cursor.execute("SELECT Id, Name FROM tags WHERE Name = ?", tag.name)
        existing = cursor.fetchone()
        if existing:
            return {"Id": existing.Id, "Name": existing.Name}
            
        # SQL Server PyODBC syntax for OUTPUT inserted.Id doesn't easily return the value with execute, using @@IDENTITY
        cursor.execute("INSERT INTO tags (Name) VALUES (?)", tag.name)
        cursor.execute("SELECT @@IDENTITY AS Id")
        new_id = int(cursor.fetchone().Id)
        db.commit()
        return {"Id": new_id, "Name": tag.name}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create tag: {e}")

@app.delete("/api/tags/{tag_id}")
def delete_tag(
    tag_id: int, 
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        # Sync: remove this tag from any assigned pages before deleting the tag
        cursor.execute("UPDATE pages SET TagId = NULL, TagName = NULL WHERE TagId = ?", tag_id)
        # Delete from tags
        cursor.execute("DELETE FROM tags WHERE Id = ?", tag_id)
        db.commit()
        return {"message": "Tag deleted successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete tag: {e}")

@app.patch("/api/pages/{page_id}/tag")
def update_page_tag(
    page_id: str, 
    request: TagUpdateRequest, 
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    try:
        cursor = db.cursor()
        query = "UPDATE pages SET TagId = ?, TagName = ? WHERE Page_id = ?"
        cursor.execute(query, (request.tagId, request.tagName, page_id))
        db.commit()
        return {"message": "Page tag updated successfully"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update tag: {e}")

# --- Ad Groups Analysis ---

@app.post("/api/pages/{page_id}/analyze-groups", status_code=202)
async def trigger_ad_group_analysis(
    page_id: str,
    background_tasks: BackgroundTasks,
    current_user: UserInDB = Depends(get_current_active_user)
):
    """
    Dispara el análisis de grupos de anuncios para la página dada.
    Escribe __ANALYZING__ en la BD de forma síncrona y luego lanza el proceso en background.
    """
    from meta_service import analyze_and_save_page_groups, set_analyzing_marker
    # Escribir marcador de forma síncrona para que el frontend lo vea de inmediato
    set_analyzing_marker(page_id)
    background_tasks.add_task(analyze_and_save_page_groups, page_id)
    return {"message": "Analysis started", "page_id": page_id}


@app.get("/api/pages/{page_id}/ad-groups")
def get_ad_groups(
    page_id: str,
    db: pyodbc.Connection = Depends(get_db),
    current_user: UserInDB = Depends(get_current_active_user)
):
    """
    Retorna los grupos de anuncios calculados para la página dada.
    Si aún no se analizó, retorna status='not_requested'.
    Si está procesando (AdGroupsJson es NULL pero fue solicitado), retorna status='processing'.
    Si tiene datos, retorna status='done' con la lista de grupos.
    """
    try:
        import json as json_lib
        cursor = db.cursor()
        cursor.execute(
            "SELECT AdGroupsJson FROM pages WHERE Page_id = ?",
            page_id
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Page not found")

        ad_groups_json = row[0]
        if ad_groups_json is None:
            return {"status": "not_requested", "groups": None}

        if ad_groups_json == "__ANALYZING__":
            return {"status": "processing", "groups": None}

        groups = json_lib.loads(ad_groups_json)
        return {"status": "done", "groups": groups}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- AI Integrations ---
class ExplainCompanyRequest(BaseModel):
    page_name: str

@app.post("/api/explain_company")
async def explain_company(
    request: ExplainCompanyRequest,
    current_user: UserInDB = Depends(get_current_active_user)
):
    import os
    try:
        from openai import AsyncOpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set on the server")
        
        client = AsyncOpenAI(api_key=api_key)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful business analyst assistant. Always respond in English with a concise paragraph explaining what the given company does based on its name. Keep it short, direct, and informative. If you don't know the company perfectly, provide an educated guess based on keywords in the name."},
                {"role": "user", "content": f"What does this company do: {request.page_name}"}
            ],
            max_tokens=200
        )
        return {"explanation": response.choices[0].message.content.strip()}
    except ImportError:
        raise HTTPException(status_code=500, detail="openai module is not installed on the server.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
