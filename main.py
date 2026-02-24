from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pyodbc
from database import get_db

app = FastAPI(title="NicheBreaker API Bridge")

# Allow frontend to access this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Replace with Vercel URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    1: "image",
    2: "video"
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
    manual_status: str
    beneficiary: Optional[str] = None
    top_creative: Optional[TopCreative] = None

class StatusUpdateRequest(BaseModel):
    manual_status: str

@app.get("/api/pages", response_model=List[PageData])
def get_pages(
    status: str = "unprocessed",
    country: Optional[str] = None,
    category: Optional[str] = None,
    searchTerm: Optional[str] = None,
    db: pyodbc.Connection = Depends(get_db)
):
    try:
        cursor = db.cursor()
        
        # Translate frontend status to DB integer
        db_status = STATUS_MAP_TO_DB.get(status, 0)
        
        query = """
            SELECT 
                pp.Id as PagesProductsId,
                pg.Page_id,
                pg.Name,
                pg.eu_total_reach,
                pp.status,
                pp.beneficiary as pp_beneficiary,
                a.creativeUrl,
                a.creative_type,
                a.AdSnapshotUrl
            FROM pagesProducts pp
            INNER JOIN pages pg ON pp.pageId = pg.Id
            LEFT JOIN ads a ON a.pageId = pg.Id
            WHERE pp.status = ?
        """
        params = [db_status]
        
        if searchTerm and searchTerm != "All":
            query += " AND pg.Name LIKE ?"
            params.append(f"%{searchTerm}%")
            
        # Simplified query without complex country/category filters since we don't know the exact schema for those.
        # But this gets the core working!
        query += " ORDER BY pg.eu_total_reach DESC"
        
        # VERY basic pagination / limiting for the bridge to not overload
        query = query.replace("SELECT", "SELECT TOP 100")
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        results = []
        # Group by page_id since LEFT JOIN ads might return duplicates
        seen_pages = set()
        
        for row in rows:
            if row.Page_id in seen_pages:
                continue
            seen_pages.add(row.Page_id)
            
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
                country="", # Placeholder for now
                total_eu_reach=row.eu_total_reach or 0,
                manual_status=ui_status,
                beneficiary=row.pp_beneficiary or "",
                top_creative=top_creative
            ))
            
        return results
        
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/pages/{page_id}/status")
def update_page_status(
    page_id: str,
    update_data: StatusUpdateRequest,
    db: pyodbc.Connection = Depends(get_db)
):
    try:
        db_status = STATUS_MAP_TO_DB.get(update_data.manual_status)
        if db_status is None:
            raise HTTPException(status_code=400, detail="Invalid status")
            
        cursor = db.cursor()
        
        # 1. Find the Page.Id from page_id string
        cursor.execute("SELECT Id FROM pages WHERE Page_id = ?", [page_id])
        page_row = cursor.fetchone()
        
        if not page_row:
            raise HTTPException(status_code=404, detail="Page not found")
            
        internal_page_id = page_row.Id
        
        # 2. Update pagesProducts using that PageId
        cursor.execute(
            "UPDATE pagesProducts SET status = ? WHERE pageId = ?", 
            [db_status, internal_page_id]
        )
        db.commit()
        
        return {"success": True, "message": "Status updated successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}
