"""
FastAPI main application
"""

import os
import logging
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime

from ...domain.entities import TariffCode
from ...infrastructure.storage.postgresql_repository import PostgreSQLRepository
from ...infrastructure.storage.cleaned_repository import CleanedDataRepository
from ...infrastructure.storage.redis_cache_repository import RedisCacheRepository
from ...infrastructure.storage.cached_repository import CachedRepository
from ...infrastructure.search.elasticsearch_repository import ElasticsearchRepository
try:
    from ...infrastructure.tasks.celery_app import scrape_tariff_code_task
except ImportError:
    # Celery not available, use sync scraping
    scrape_tariff_code_task = None
from .models import (
    TariffCodeResponse,
    TariffCodeListResponse,
    SectionResponse,
    ScrapeRequest,
    ScrapeResponse,
    StatsResponse
)
from .dependencies import get_db_repo, get_cleaned_repo, get_elasticsearch_repo

logger = logging.getLogger(__name__)

app = FastAPI(
    title="ADIL Scraper API",
    description="REST API for accessing scraped tariff code data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "ok",
        "service": "ADIL Scraper API",
        "version": "1.0.0"
    }


@app.get("/api/tariff-codes", response_model=TariffCodeListResponse, tags=["Tariff Codes"])
async def list_tariff_codes(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """List all tariff codes with pagination"""
    try:
        codes = db_repo.list_all_tariff_codes()
        total = len(codes)
        paginated_codes = codes[skip:skip + limit]
        
        return TariffCodeListResponse(
            codes=paginated_codes,
            total=total,
            skip=skip,
            limit=limit
        )
    except Exception as e:
        logger.error(f"Error listing tariff codes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}", response_model=TariffCodeResponse, tags=["Tariff Codes"])
async def get_tariff_code(
    tariff_code: str,
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """Get complete data for a specific tariff code"""
    try:
        data = db_repo.load_latest(tariff_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"Tariff code {tariff_code} not found")
        
        return TariffCodeResponse.from_scraped_data(data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tariff code {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}/sections", tags=["Sections"])
async def list_sections(
    tariff_code: str,
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """List all sections for a tariff code"""
    try:
        data = db_repo.load_latest(tariff_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"Tariff code {tariff_code} not found")
        
        sections = [
            {
                "name": name,
                "type": section.section_type.value,
                "has_tables": len(section.structured_data.tables) > 0,
                "has_lists": len(section.structured_data.lists) > 0,
                "table_count": len(section.structured_data.tables),
                "list_count": len(section.structured_data.lists)
            }
            for name, section in data.sections.items()
        ]
        
        return {"tariff_code": tariff_code, "sections": sections}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing sections for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}/sections/{section_name}", response_model=SectionResponse, tags=["Sections"])
async def get_section(
    tariff_code: str,
    section_name: str,
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """Get a specific section for a tariff code"""
    try:
        data = db_repo.load_latest(tariff_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"Tariff code {tariff_code} not found")
        
        if section_name not in data.sections:
            raise HTTPException(
                status_code=404,
                detail=f"Section '{section_name}' not found for tariff code {tariff_code}"
            )
        
        section = data.sections[section_name]
        return SectionResponse.from_section_data(section_name, section)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting section {section_name} for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}/tables", tags=["Tables"])
async def list_tables(
    tariff_code: str,
    section_name: Optional[str] = Query(None, description="Filter by section name"),
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """List all tables for a tariff code, optionally filtered by section"""
    try:
        data = db_repo.load_latest(tariff_code)
        if not data:
            raise HTTPException(status_code=404, detail=f"Tariff code {tariff_code} not found")
        
        all_tables = []
        for name, section in data.sections.items():
            if section_name and name != section_name:
                continue
            
            for idx, table in enumerate(section.structured_data.tables):
                all_tables.append({
                    "section": name,
                    "table_index": idx + 1,
                    "headers": table.get('headers', []),
                    "row_count": len(table.get('rows', []))
                })
        
        return {"tariff_code": tariff_code, "tables": all_tables}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing tables for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}/cleaned", tags=["Cleaned Data"])
async def get_cleaned_data(
    tariff_code: str,
    cleaned_repo: CleanedDataRepository = Depends(get_cleaned_repo)
):
    """Get cleaned/transformed data for a tariff code"""
    try:
        data = cleaned_repo.get(tariff_code)
        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"Cleaned data not found for tariff code {tariff_code}"
            )
        
        return data.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cleaned data for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tariff-codes/{tariff_code}/history", tags=["History"])
async def get_history(
    tariff_code: str,
    limit: int = Query(10, ge=1, le=100),
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """Get version history for a tariff code"""
    try:
        history = db_repo.load_history(tariff_code, limit)
        if not history:
            raise HTTPException(status_code=404, detail=f"Tariff code {tariff_code} not found")
        
        return {
            "tariff_code": tariff_code,
            "versions": [
                {
                    "version": data.version if hasattr(data, 'version') else None,
                    "scraped_at": data.scraped_at.isoformat() if hasattr(data, 'scraped_at') else None,
                    "section_count": len(data.sections) if hasattr(data, 'sections') else 0
                }
                for data in history
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting history for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scrape/{tariff_code}", response_model=ScrapeResponse, tags=["Scraping"])
async def trigger_scrape(
    tariff_code: str,
    request: ScrapeRequest = ScrapeRequest()
):
    """Trigger a scrape for a tariff code (async via Celery)"""
    try:
        if scrape_tariff_code_task:
            task = scrape_tariff_code_task.delay(tariff_code)
            return ScrapeResponse(
                tariff_code=tariff_code,
                task_id=task.id,
                status="queued",
                message=f"Scraping task queued for {tariff_code}"
            )
        else:
            # Fallback to sync if Celery not available
            raise HTTPException(
                status_code=503,
                detail="Celery not available. Use scrape_now.py for manual scraping."
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error queuing scrape for {tariff_code}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search", tags=["Search"])
async def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(10, ge=1, le=100),
    es_repo: ElasticsearchRepository = Depends(get_elasticsearch_repo)
):
    """Search across tariff codes using Elasticsearch"""
    try:
        results = es_repo.search(q, limit=limit)
        return {
            "query": q,
            "results": results,
            "total": len(results)
        }
    except Exception as e:
        logger.error(f"Error searching: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats", response_model=StatsResponse, tags=["Statistics"])
async def get_stats(
    db_repo: PostgreSQLRepository = Depends(get_db_repo)
):
    """Get pipeline statistics"""
    try:
        conn = db_repo._get_connection()
        try:
            with conn.cursor() as cur:
                # Total records
                cur.execute("SELECT COUNT(*) FROM scraped_data")
                total_records = cur.fetchone()[0]
                
                # Unique codes
                cur.execute("SELECT COUNT(DISTINCT tariff_code) FROM scraped_data")
                unique_codes = cur.fetchone()[0]
                
                # Latest scrape
                cur.execute("SELECT MAX(scraped_at) FROM scraped_data")
                latest_scrape = cur.fetchone()[0]
                
                # First scrape
                cur.execute("SELECT MIN(scraped_at) FROM scraped_data")
                first_scrape = cur.fetchone()[0]
                
                return StatsResponse(
                    total_records=total_records,
                    unique_codes=unique_codes,
                    latest_scrape=latest_scrape.isoformat() if latest_scrape else None,
                    first_scrape=first_scrape.isoformat() if first_scrape else None
                )
        finally:
            db_repo._return_connection(conn)
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

