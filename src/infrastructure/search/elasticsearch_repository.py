"""
Elasticsearch repository for search functionality
"""

import os
import logging
from typing import List, Dict, Any, Optional
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, RequestError

logger = logging.getLogger(__name__)


class ElasticsearchRepository:
    """Elasticsearch implementation for search"""
    
    INDEX_NAME = "tariff_codes"
    
    def __init__(self, es_url: Optional[str] = None):
        """
        Initialize Elasticsearch repository
        
        Args:
            es_url: Elasticsearch URL (defaults to ELASTICSEARCH_URL env var)
        """
        self.es_url = es_url or os.getenv('ELASTICSEARCH_URL', 'http://localhost:9200')
        self.client = Elasticsearch([self.es_url])
        self._ensure_index()
    
    def _ensure_index(self):
        """Create index if it doesn't exist"""
        try:
            if not self.client.indices.exists(index=self.INDEX_NAME):
                mapping = {
                    "mappings": {
                        "properties": {
                            "tariff_code": {"type": "keyword"},
                            "product_description": {
                                "type": "text",
                                "analyzer": "standard"
                            },
                            "sections": {
                                "type": "nested",
                                "properties": {
                                    "name": {"type": "keyword"},
                                    "content": {"type": "text"},
                                    "tables": {"type": "text"},
                                    "lists": {"type": "text"}
                                }
                            },
                            "scraped_at": {"type": "date"}
                        }
                    }
                }
                self.client.indices.create(index=self.INDEX_NAME, body=mapping)
                logger.info(f"Created Elasticsearch index: {self.INDEX_NAME}")
        except ConnectionError as e:
            logger.warning(f"Could not connect to Elasticsearch: {e}")
        except Exception as e:
            logger.error(f"Error ensuring index: {e}", exc_info=True)
    
    def index_tariff_code(self, tariff_code: str, data: Dict[str, Any]) -> bool:
        """
        Index a tariff code in Elasticsearch
        
        Args:
            tariff_code: Tariff code
            data: Scraped data dictionary
        """
        try:
            # Extract searchable content
            doc = {
                "tariff_code": tariff_code,
                "product_description": data.get('basic_info', {}).get('product_description', ''),
                "scraped_at": data.get('scraped_at'),
                "sections": []
            }
            
            # Extract sections
            sections = data.get('sections', {})
            for section_name, section_data in sections.items():
                section_doc = {
                    "name": section_name,
                    "content": "",
                    "tables": [],
                    "lists": []
                }
                
                structured = section_data.get('structured_data', {})
                
                # Extract table content
                for table in structured.get('tables', []):
                    table_text = " ".join([
                        " ".join(str(cell) for cell in row.values() if row)
                        for row in table.get('rows', [])
                    ])
                    section_doc["tables"].append(table_text)
                
                # Extract list content
                for lst in structured.get('lists', []):
                    section_doc["lists"].extend([str(item) for item in lst])
                
                section_doc["content"] = " ".join(section_doc["tables"] + section_doc["lists"])
                doc["sections"].append(section_doc)
            
            self.client.index(index=self.INDEX_NAME, id=tariff_code, body=doc)
            logger.debug(f"Indexed tariff code {tariff_code} in Elasticsearch")
            return True
        except ConnectionError:
            logger.warning("Elasticsearch not available, skipping indexing")
            return False
        except Exception as e:
            logger.error(f"Error indexing {tariff_code}: {e}", exc_info=True)
            return False
    
    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search across tariff codes
        
        Args:
            query: Search query
            limit: Maximum number of results
        """
        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "product_description^3",
                            "sections.content^2",
                            "sections.tables",
                            "sections.lists"
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                "highlight": {
                    "fields": {
                        "product_description": {},
                        "sections.content": {}
                    }
                },
                "size": limit
            }
            
            response = self.client.search(index=self.INDEX_NAME, body=search_body)
            
            results = []
            for hit in response['hits']['hits']:
                results.append({
                    "tariff_code": hit['_source']['tariff_code'],
                    "product_description": hit['_source'].get('product_description', ''),
                    "score": hit['_score'],
                    "highlight": hit.get('highlight', {})
                })
            
            return results
        except ConnectionError:
            logger.warning("Elasticsearch not available for search")
            return []
        except Exception as e:
            logger.error(f"Error searching: {e}", exc_info=True)
            return []
    
    def delete_tariff_code(self, tariff_code: str) -> bool:
        """Delete a tariff code from index"""
        try:
            self.client.delete(index=self.INDEX_NAME, id=tariff_code, ignore=[404])
            return True
        except Exception as e:
            logger.error(f"Error deleting {tariff_code}: {e}", exc_info=True)
            return False

